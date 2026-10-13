# frozen_string_literal: true
require_relative 'spec_helper'
require_relative 'support/system'
require_relative 'support/nats_client'
require_relative '../lib/skvoz/server/revocation'

RSpec.describe Skvoz::Server::Revocation do
  it 'retires unreachable server inventory after replacements while retaining unresolved sockets' do
    config = instance_double(Skvoz::Server::Configuration)
    allow(config).to receive(:links).and_return(nil)
    state = instance_double(Skvoz::Server::State)
    revocation = described_class.new(config, state)
    connection = instance_double(Skvoz::Server::NatsConnection)
    revocation.instance_variable_set(:@connection, connection)
    allow(revocation).to receive(:current_names).and_return([])
    old_server = Skvoz::Server::NatsAuth::Key.new(:server).public_key
    revocation.instance_variable_set(:@records, { [old_server, 7] => { identity: 'deleted-generation', bound: true } })
    40.times do
      current = Skvoz::Server::NatsAuth::Key.new(:server).public_key
      allow(connection).to receive(:server_id).and_return(current)
      allow(connection).to receive(:query).with('$SYS.REQ.SERVER.PING.IDZ', {}, timeout: 0.3, collect: true).and_return([{ 'id' => current, 'name' => 'local-broker' }])
      revocation.send(:discover)
      expect(revocation.statistics).to include('native_servers' => 1, 'pending_connections' => 1, 'unreachable_connections' => 1)
      expect(revocation.instance_variable_get(:@servers).keys).to eq([current])
    end
    # A SYS partition is not evidence that its old broker socket closed.
    allow(connection).to receive(:server_id).and_return(old_server)
    allow(connection).to receive(:query).with('$SYS.REQ.SERVER.PING.IDZ', {}, timeout: 0.3, collect: true).and_return([{ 'id' => old_server, 'name' => 'local-broker' }])
    revocation.send(:discover)
    expect(revocation.statistics).to include('pending_connections' => 1, 'unreachable_connections' => 0)
  end
end

RSpec.describe 'Startup native connection reconciliation', integration: true do
  include ServerSystem

  it 'retains the durable fence and empty startup admission when the device acknowledgement is interrupted' do
    Dir.mktmpdir('skvoz-unconfirmed-startup-') do |root|
      root = Pathname.new(root)
      certificates(root)
      runtime = root.join('unconfirmed-startup-runtime')
      FileUtils.cp(Pathname.new(__dir__).join('fixtures/control_owner.rb'), runtime)
      runtime.chmod(0o700)
      server = ServerSystem::Server.new(root, overrides: { 'runtime_binary' => runtime.to_s }, autostart: false)
      state = Skvoz::Server::State.new(Skvoz::Server::Configuration.new(server.value))
      candidate, = state.mutate('add', 'retained', password: 'startup-fixture-password')
      state.commit(candidate)
      candidate, id = state.enroll('retained', 'a' * 32)
      state.commit(candidate)
      assigned = candidate.fetch('users').fetch('retained').fetch('assigned').dup
      expect(assigned).to include(id)
      state.close
      state = nil
      expect { server.start }.to raise_error(IOError, /Server startup failed/)
      value = JSON.parse(server.state.join('state.json').read, allow_duplicate_key: false)
      expect(value.fetch('security_pending')).to be(true)
      expect(value.fetch('users').fetch('retained').fetch('assigned')).to eq(assigned)
      profile = JSON.parse(server.state.join('network-profile.json').read, allow_duplicate_key: false)
      expect(profile.fetch('access')).to eq('revision' => 0, 'devices' => [], 'revoke' => [])
      expect(profile.dig('core', 'allowed_peers')).to eq([])
      request = JSON.parse(server.state.join('control-owner-access.json').read, allow_duplicate_key: false)
      expect(request.fetch('devices')).to eq(assigned)
      expect(process_dead(Integer(server.state.join('control-owner.pid').read))).to be(true)
    ensure
      state&.close
      server&.stop(kill: true)
      server&.close
      preserve_artifacts(root) if $! && root
    end
  end

  it 'closes a retained plain-login client before clearing the migration fence and retains scoped native transport' do
    Dir.mktmpdir('skvoz-legacy-fence-') do |root|
      certificates(root)
      ports = free_ports(5)
      config = Skvoz::Server::Configuration.new('address' => 'localhost', 'bind' => '127.0.0.1', 'port' => ports[0], 'monitor_port' => ports[1],
        'state_dir' => File.join(root, 'state'), 'network' => {}, 'tls' => { 'mode' => 'provided', 'certificate' => File.join(root, 'server.pem'), 'key' => File.join(root, 'server.key'), 'ca' => File.join(root, 'ca.pem') })
      state = Skvoz::Server::State.new(config)
      candidate, = state.mutate('add', 'legacy', password: 'legacy-fixture-password')
      candidate['security_pending'] = true
      candidate['tls'] = { 'certificate' => File.join(root, 'server.pem'), 'key' => File.join(root, 'server.key'), 'ca' => File.join(root, 'ca.pem') }
      state.commit(candidate)
      native = JSON.parse(state.nats_config)
      # Only this isolated old-broker fixture has static ordinary login users.
      native.delete('authorization')
      native.fetch('accounts').fetch('APP').fetch('users').concat([
        { 'user' => 'legacy', 'password' => state.value.fetch('users').fetch('legacy').fetch('hash'), 'allowed_connection_types' => ['STANDARD'] },
        { 'user' => '__skvoz_leaf_fixture', 'password' => 'private-leaf-fixture-password', 'allowed_connection_types' => ['LEAFNODE'],
          'permissions' => { 'publish' => ['skvoz.application.>'], 'subscribe' => ['skvoz.application.>'] } }
      ])
      native['leafnodes'] = { 'host' => '127.0.0.1', 'port' => ports[2], 'compression' => 'off', 'tls' => native.fetch('tls').merge('handshake_first' => true) }
      broker_path = File.join(root, 'native.json')
      private_json(broker_path, native)
      log = File.open(File.join(root, 'native.log'), 'ab')
      pid = Process.spawn(ServerSystem::NATS, '--config', broker_path, out: log, err: log)
      wait_until do
        begin
          TCPSocket.new('127.0.0.1', ports[0]).close
          true
        rescue Errno::ECONNREFUSED
          false
        end
      end
      server = Struct.new(:port, :directory).new(ports[0], Pathname.new(root))
      legacy = ServerSystem::NatsClient.new(server, 'legacy', 'legacy-fixture-password')
      probe_credentials = state.service('probe')
      probe = ServerSystem::NatsClient.new(server, probe_credentials.fetch('username'), probe_credentials.fetch('password'))
      leaf_path = File.join(root, 'leaf.json')
      private_json(leaf_path, { 'server_name' => 'isolated-native-transport', 'host' => '127.0.0.1', 'port' => ports[3], 'http' => "127.0.0.1:#{ports[4]}",
        'accounts' => { 'APP' => {} }, 'leafnodes' => { 'compression' => 'off', 'remotes' => [{ 'account' => 'APP',
          'urls' => ["nats-leaf://__skvoz_leaf_fixture:private-leaf-fixture-password@localhost:#{ports[2]}"],
          'tls' => { 'handshake_first' => true, 'ca_file' => File.join(root, 'ca.pem') }, 'compression' => 'off' }] } })
      leaf_pid = Process.spawn(ServerSystem::NATS, '--config', leaf_path, out: log, err: log)
      http = Net::HTTP.new('127.0.0.1', ports[1], nil)
      http.open_timeout = http.read_timeout = 1
      wait_until { JSON.parse(http.get('/leafz').body).fetch('leafnodes') == 1 }
      expect(state.access_ready?).to be(false)
      Async do |task|
        native_rows = JSON.parse(http.get('/connz?auth=1&acc=APP').body)
        legacy_row = native_rows.fetch('connections').find { |row| row['authorized_user'] == 'legacy' }
        plain = Skvoz::Server::NatsConnection.new(config, state, 'sys', max_body: 1_048_576).start(task)
        expect(plain.server_id).to eq(native_rows.fetch('server_id'))
        plain.publish("$SYS.REQ.SERVER.#{native_rows.fetch('server_id')}.KICK", JSON.generate(cid: legacy_row.fetch('cid')))
        plain.flush
        legacy.flush
        expect(JSON.parse(http.get('/connz?auth=1&acc=APP').body).fetch('connections')).to include(include('cid' => legacy_row.fetch('cid')))
        plain.stop
        revocation = Skvoz::Server::Revocation.new(config, state).start(task)
        expect(revocation.pending).to eq(0)
        expect(legacy.closed?).to be(true)
        probe.flush
        expect(JSON.parse(http.get('/leafz').body).fetch('leafnodes')).to eq(1)
        expect(state.access_ready?).to be(false)
      ensure
        plain&.stop
        revocation&.stop
      end.wait
    ensure
      legacy&.close; probe&.close
      terminate(leaf_pid, timeout: 3) if leaf_pid
      terminate(pid, timeout: 3) if pid
      log&.close
      state&.close
    end
  end
end
