# frozen_string_literal: true
require_relative 'spec_helper'
require_relative 'support/system'
require_relative 'support/nats_client'
require_relative '../lib/skvoz/server/nats_connection'

RSpec.describe 'Native per-client authentication and targeted revoke', integration: true do
  include ServerSystem

  def snapshot
    JSON.parse(@server.state.join('state.json').read, allow_duplicate_key: false)
  end

  def identity(login)
    state = snapshot
    "skvoz:#{state.fetch('authority_nonce')}:#{state.fetch('users').fetch(login).fetch('access_revision')}:#{login}"
  end

  def native_connections(account = 'APP')
    http = Net::HTTP.new('127.0.0.1', @server.monitor, nil)
    http.open_timeout = http.read_timeout = 1
    body = http.get("/connz?auth=1&acc=#{account}&limit=1312").body
    raise IOError, 'Native monitor exceeds limit' if body.bytesize > 1_048_576
    JSON.parse(body, allow_duplicate_key: false)
  end

  def client(login, password)
    connection = ServerSystem::NatsClient.new(@server, login, password)
    @clients << connection
    connection
  end

  def record_native(label)
    return unless ENV['SKVOZ_TEST_ARTIFACTS']
    row = { stage: label, accounts: %w[APP AUTH].to_h do |account|
      inventory = native_connections(account)
      [account, { server_id: inventory.fetch('server_id'), total: inventory.fetch('total'),
        connections: inventory.fetch('connections').map { |entry| entry.slice('cid', 'kind', 'authorized_user') } }]
    end }
    @native_proof << row
    FileUtils.mkdir_p(ENV.fetch('SKVOZ_TEST_ARTIFACTS'), mode: 0o700)
    File.write(File.join(ENV.fetch('SKVOZ_TEST_ARTIFACTS'), "auth-native-#{@proof_id}.json"), JSON.pretty_generate(@native_proof))
  end

  def sys_kick(cid)
    Async do |task|
      state = snapshot
      credentials = Struct.new(:value) do
        def service(kind) = value.fetch('services').fetch(kind).merge('username' => "__skvoz_#{kind}")
      end.new(state)
      config = Skvoz::Server::Configuration.new(@server.value)
      sys = Skvoz::Server::NatsConnection.new(config, credentials, 'sys', max_body: 65_588).start(task)
      server = native_connections.fetch('server_id')
      response = sys.query("$SYS.REQ.SERVER.#{server}.KICK", { cid: cid })
      expect(response.dig('server', 'id')).to eq(server)
    ensure
      sys&.stop
    end.wait
  end

  around do |example|
    Dir.mktmpdir('skvoz-auth-') do |directory|
      certificates(directory)
      @clients = []
      @native_proof, @proof_id = [], SecureRandom.hex(8)
      @server = ServerSystem::Server.new(directory, overrides: { 'devices_per_user' => 4, 'max_identities' => 16 })
      @server.command('add', 'alice', 'alice-first-password')
      @server.command('add', 'bob', 'bob-healthy-password')
      example.run
      record_native('final') unless example.exception
    ensure
      preserve_artifacts(directory) if example.exception
      @clients&.each(&:close)
      @server&.close
    end
  end

  it 'rejects malformed passwords and foreign subjects without killing the responder or healthy client' do
    bob = client('bob', 'bob-healthy-password')
    bob_cid = native_connections.fetch('connections').find { |row| row['authorized_user'] == identity('bob') }.fetch('cid')
    ["nul\0password-value", 'incorrect-password', "\u{10ffff}" * 19].each do |password|
      expect { client('alice', password) }.to raise_error(IOError)
    end
    expect { client('alice', 'alice-first-password').subscribe('$SYS.REQ.USER.AUTH') }.to raise_error(IOError)
    expect { client('alice', 'alice-first-password').subscribe('skvoz.enroll.reply.bob.*') }.to raise_error(IOError)
    alice = client('alice', 'alice-first-password')
    alice.flush
    bob.flush
    expect(native_connections.fetch('connections').find { |row| row['authorized_user'] == identity('bob') }.fetch('cid')).to eq(bob_cid)
    expect(@server.command('health')).to include('healthy' => true)
  end

  it 'keeps an unrelated CID across resets, removal, same-login recreation and delayed old KICK' do
    bob = client('bob', 'bob-healthy-password')
    bob_cid = native_connections.fetch('connections').find { |row| row['authorized_user'] == identity('bob') }.fetch('cid')
    client('alice', 'alice-first-password')
    %w[alice-second-password alice-third-password alice-fourth-password].each do |password|
      @server.command('reset-password', 'alice', password)
      client('alice', password)
    end
    deleted_identity = identity('alice')
    deleted_row = native_connections.fetch('connections').find { |row| row['authorized_user'] == deleted_identity }
    deleted_ids = snapshot.fetch('users').fetch('alice').fetch('ids')
    record_native('before-fourth-identity-remove')
    expect(@server.command('remove', 'alice')).to include('pending_connections' => 0)
    replacement = @server.command('add', 'alice', 'alice-new-password')
    recreated = client('alice', 'alice-new-password')
    recreated_identity = identity('alice')
    recreated_cid = native_connections.fetch('connections').find { |row| row['authorized_user'] == recreated_identity }.fetch('cid')
    expect(recreated_identity).not_to eq(deleted_identity)
    expect(deleted_ids).not_to include(replacement.fetch('peer_id'))
    sys_kick(deleted_row.fetch('cid'))
    recreated.flush
    bob.flush
    expect(native_connections.fetch('connections').find { |row| row['authorized_user'] == recreated_identity }.fetch('cid')).to eq(recreated_cid)
    expect(native_connections.fetch('connections').find { |row| row['authorized_user'] == identity('bob') }.fetch('cid')).to eq(bob_cid)
    expect { client('alice', 'alice-fourth-password') }.to raise_error(IOError)
  end

  it 'rejects a bounded simultaneous CONNECT burst at queue16 and recovers without replacing an unrelated CID' do
    bob = client('bob', 'bob-healthy-password')
    cid = native_connections.fetch('connections').find { |row| row['authorized_user'] == identity('bob') }.fetch('cid')
    before = @server.command('health').fetch('authentication').fetch('overloaded')
    barrier = Queue.new
    outcomes = Queue.new
    workers = 64.times.map do
      Thread.new do
        barrier.pop
        connection = ServerSystem::NatsClient.new(@server, 'alice', 'alice-first-password')
        outcomes << :accepted
      rescue IOError, SystemCallError, OpenSSL::SSL::SSLError, Timeout::Error
        outcomes << :rejected
      ensure
        connection&.close
      end
    end
    64.times { barrier << true }
    Timeout.timeout(10) { workers.each(&:value) }
    result = 64.times.map { outcomes.pop }
    expect(result).to include(:rejected)
    expect(@server.command('health').fetch('authentication').fetch('overloaded')).to be > before
    wait_until(timeout: 5) { @server.command('health')['healthy'] }
    client('alice', 'alice-first-password').flush
    bob.flush
    expect(native_connections.fetch('connections').find { |row| row['authorized_user'] == identity('bob') }.fetch('cid')).to eq(cid)
  ensure
    workers&.each { |worker| worker.join(0.1) || worker.kill }
  end

  it 'reconciles more than one bounded SYS page and recovers AUTH without replacing healthy CIDs' do
    alice = client('alice', 'alice-first-password')
    alice_cid = native_connections.fetch('connections').find { |row| row['authorized_user'] == identity('alice') }.fetch('cid')
    bob = 130.times.map { client('bob', 'bob-healthy-password') }
    expect(native_connections.fetch('total')).to be > Skvoz::Server::Revocation::PAGE_SIZE
    record_native('before-auth-recovery')
    auth_cid = native_connections('AUTH').fetch('connections').find { |row| row['authorized_user'] == '__skvoz_auth' }.fetch('cid')
    sys_kick(auth_cid)
    wait_until(timeout: 10) do
      row = native_connections('AUTH').fetch('connections').find { |entry| entry['authorized_user'] == '__skvoz_auth' }
      row && row.fetch('cid') != auth_cid
    end
    wait_until(timeout: 10) { @server.command('health')['healthy'] }
    record_native('after-auth-recovery')
    alice.flush
    client('alice', 'alice-first-password')
    expect(native_connections.fetch('connections').find { |row| row['authorized_user'] == identity('alice') }.fetch('cid')).to eq(alice_cid)
    started = monotonic
    expect(@server.command('remove', 'bob')).to include('pending_connections' => 0)
    expect(monotonic - started).to be <= 5
    expect(bob).to all(satisfy(&:closed?))
    alice.flush
    expect(@server.command('health').fetch('revocation').fetch('largest_page_count')).to be >= 2
  end
end
