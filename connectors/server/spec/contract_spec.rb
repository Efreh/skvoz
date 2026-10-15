# frozen_string_literal: true
require_relative 'spec_helper'

RSpec.describe 'Server network and durable identity contract' do
  def value
    { 'address' => 'example.org', 'tls' => { 'email' => 'test@example.com', 'terms_agreed' => true } }
  end

  it 'requires current settings and rejects removed dataplane settings and old rules' do
    %w[core_binary receive_window stream_queue_bytes tcp_buffer_bytes].each do |key|
      expect { Skvoz::Server::Configuration.new(value.merge(key => 1)) }.to raise_error(Skvoz::Server::Error)
    end
    expect { Skvoz::Server::Configuration.new(value.merge('v' => 2)) }.to raise_error(Skvoz::Server::Error)
    expect { Skvoz::Server::Configuration.new(value.merge('allow' => ['127.0.0.0/8'])) }.to raise_error(Skvoz::Server::Error)
    expect { Skvoz::Server::Configuration.new(value.merge('max_identities' => 129)) }.to raise_error(Skvoz::Server::Error)
  end

  it 'rejects duplicate fields with the pinned JSON3.0.2 decoder used by settings and API1' do
    expect(JSON::VERSION).to eq('3.0.2')
    expect { JSON.parse('{"network":{"families":[],"families":[4]}}', allow_duplicate_key: false) }.to raise_error(JSON::ParserError)
  end

  it 'matches public upstream NKey seed/public pairs and rejects wrong type or checksum' do
    fixture = JSON.parse(File.read(File.join(__dir__, 'fixtures/nkeys.json')), allow_duplicate_key: false)
    fixture.fetch('vectors').each do |vector|
      kind = vector.fetch('kind').to_sym
      key = Skvoz::Server::NatsAuth::Key.new(kind, vector.fetch('seed'))
      expect(key.public_key).to eq(vector.fetch('public_key'))
      expect { Skvoz::Server::NatsAuth.decode(key.public_key, :server) }.to raise_error(ArgumentError)
      corrupted = key.public_key[0...-1] + (key.public_key[-1] == 'A' ? 'B' : 'A')
      expect { Skvoz::Server::NatsAuth.decode(corrupted, kind) }.to raise_error(ArgumentError)
    end
  end

  it 'emits TCP-only and strict NAT44/routed IPv6 policies with finite canonical limits' do
    standard = Skvoz::Server::NetworkConfiguration.new(Skvoz::Server::Configuration.new(value))
    expect(standard.network).to include('families' => [4])
    expect(standard.server).to include('ipv4' => { 'pool' => '10.203.0.0/16', 'egress' => 'nat44', 'interface' => 'eth0' },
      'ipv6' => nil, 'dns_servers' => ['1.1.1.1'])
    tcp = Skvoz::Server::NetworkConfiguration.new(Skvoz::Server::Configuration.new(value.merge('network' => {})))
    expect(tcp.network).to include('families' => [])
    expect(tcp.server).to include('ipv4' => nil, 'ipv6' => nil, 'dns_servers' => [])
    network = { 'ipv4' => { 'pool' => '10.253.0.0/16', 'egress' => 'nat44', 'interface' => 'eth0' },
      'ipv6' => { 'pool' => '2001:db8:10::/64', 'egress' => 'routed', 'interface' => 'eth0' },
      'dns_servers' => ['1.1.1.1', '2606:4700:4700::1111'] }
    policy = Skvoz::Server::NetworkConfiguration.new(Skvoz::Server::Configuration.new(value.merge('network' => network)))
    expect(policy.network).to include('families' => [4, 6])
    expect(policy.network.fetch('limits')).to include('core_streams' => 2048, 'runtime_buffer_bytes' => 536870912, 'receive_window' => 33554432)
    expect { Skvoz::Server::Configuration.new(value.merge('network' => network.merge('ipv6' => network['ipv6'].merge('egress' => 'nat66')))) }.to raise_error(Skvoz::Server::Error)
    expect { Skvoz::Server::Configuration.new(value.merge('network' => network.merge('dns_servers' => ['::ffff:1.1.1.1']))) }.to raise_error(Skvoz::Server::Error)
  end

  it 'retains explicitly configured mapped management endpoints beside local inventory' do
    input = { 'server_addresses' => ['203.0.113.10'], 'management_endpoints' => [{ 'address' => '203.0.113.10', 'protocol' => 6, 'port' => 31422 }] }
    config = Skvoz::Server::Configuration.new(value.merge('network' => input))
    allow(Socket).to receive(:ip_address_list).and_return([Addrinfo.ip('127.0.0.1'), Addrinfo.ip('10.0.0.2')])
    allow(Resolv).to receive(:getaddresses).with('example.org').and_return(['203.0.113.11'])
    policy = Skvoz::Server::NetworkConfiguration.new(config).inventory!(config)
    expect(policy.server.fetch('server_addresses')).to include('203.0.113.10', '203.0.113.11')
    expect(policy.server.fetch('management_endpoints')).to include(input['management_endpoints'].first,
      { 'address' => '203.0.113.11', 'protocol' => 6, 'port' => 4222 })
  end

  it 'transfers only assigned inherited descriptors through the child parent-death guard' do
    owner, child = UNIXSocket.pair
    Async do |task|
      process = Skvoz::Server::ChildProcess.new([RbConfig.ruby, '-rsocket', '-e', 'socket = UNIXSocket.for_fd(3); socket.write("owned"); socket.close'], label: 'fixture', descriptors: { 3 => child }).start(task)
      child.close
      expect(task.with_timeout(3) { owner.read(5) }).to eq('owned')
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 3
      task.sleep(0.01) while process.alive? && Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
      process.stop(deadline)
      expect(process.status.success?).to be(true)
      expect(owner.read(1)).to be_nil
    end.wait
  ensure
    owner.close unless owner.closed?
    child.close unless child.closed?
  end

  it 'creates private state beneath a safe parent and rejects symlink/writable ancestors' do
    Dir.mktmpdir do |temporary|
      parent = File.join(temporary, 'parent')
      Dir.mkdir(parent, 0o755)
      state = Skvoz::Server::PrivateFiles.create_directory(File.join(parent, 'state'))
      expect(File.stat(state).mode & 0o777).to eq(0o700)
      link = File.join(temporary, 'link')
      File.symlink(parent, link)
      expect { Skvoz::Server::PrivateFiles.create_directory(File.join(link, 'unsafe')) }.to raise_error(Skvoz::Server::Error)
      File.chmod(0o777, parent)
      expect { Skvoz::Server::PrivateFiles.create_directory(File.join(parent, 'unsafe')) }.to raise_error(Skvoz::Server::Error)
    end
  end

  it 'allocates durable devices without reusing removed peer IDs and emits the joint runtime profile' do
    Dir.mktmpdir do |temporary|
      config = Skvoz::Server::Configuration.new(value.merge('state_dir' => File.join(temporary, 'state'), 'devices_per_user' => 2, 'max_identities' => 2))
      state = Skvoz::Server::State.new(config)
      first, id = state.mutate('add', 'shared', password: 'long enough password')
      state.commit(first)
      second, another = state.mutate('device-add', 'shared', verified: true)
      state.commit(second)
      expect(another).not_to eq(id)
      expect { state.mutate('device-add', 'shared', verified: true) }.to raise_error(Skvoz::Server::Error, 'Device pool exhausted')
      removed, = state.mutate('remove', 'shared'); state.commit(removed)
      replacement, next_id = state.mutate('add', 'new', password: 'long enough password'); state.commit(replacement)
      expect(next_id).to be > another
      candidate = state.candidate; candidate['tls'] = { 'certificate' => '/unused.pem', 'key' => '/unused.key' }; state.commit(candidate)
      profile = state.profile(Skvoz::Server::NetworkConfiguration.new(config))
      expect(profile).to include('v' => 3, 'role' => 'server')
      expect(profile.fetch('core')).to include('peer_id' => '0', 'membership' => 'allowlist', 'ca_file' => nil, 'tls_server_name' => 'example.org', 'allowed_peers' => [next_id.to_s], 'initiate' => [])
      expect(state.nats_config).to include('skvoz.enroll.v4.')
      expect(state.export('new', next_id, 'long enough password')).to include('v' => 4,
        'network_runtime' => { 'network' => 6, 'api' => 1, 'version' => '0.7.0', 'core' => '5.0.0' })
      state.close
      reopened = Skvoz::Server::State.new(config)
      expect(reopened.value['next_id']).to eq(5)
      expect(reopened.value['users']['new']['assigned']).to eq([next_id])
      reopened.close
    end
  end

  it 'rejects removed roles and keeps native bootstrap identities scoped and independent of callout' do
    %w[components upstream leaf_port advertised_leaf_port].each do |key|
      expect { Skvoz::Server::Configuration.new(value.merge(key => ['control'])) }.to raise_error(Skvoz::Server::Error)
    end
    Dir.mktmpdir do |root|
      config = Skvoz::Server::Configuration.new(value.merge('state_dir' => File.join(root, 'state')))
      state = Skvoz::Server::State.new(config)
      candidate = state.candidate
      candidate['tls'] = { 'certificate' => '/certificate.pem', 'key' => '/key.pem' }
      state.commit(candidate)
      native = JSON.parse(state.nats_config)
      expect(native.fetch('accounts').keys.sort).to eq(%w[APP AUTH SYS])
      users = native.fetch('accounts').transform_values { |account| account.fetch('users') }
      expect(users.fetch('APP').map { |user| user.fetch('user') }).to eq(%w[__skvoz_runtime __skvoz_enrollment __skvoz_probe])
      expect(users.fetch('AUTH').map { |user| user.fetch('user') }).to eq(['__skvoz_auth'])
      expect(users.fetch('SYS').map { |user| user.fetch('user') }).to eq(['__skvoz_sys'])
      credentials = state.value.fetch('services').values
      expect(credentials.map { |credential| credential.fetch('password') }.uniq.size).to eq(5)
      expect(native.dig('authorization', 'auth_callout', 'auth_users').sort).to eq(Skvoz::Server::State::SERVICES.keys.map { |kind| "__skvoz_#{kind}" }.sort)
      expect(native).not_to have_key('leafnodes')
      expect(state.profile(Skvoz::Server::NetworkConfiguration.new(config))).not_to have_key('routing')
      state.close
    end
  end

  it 'admits reserved device capacity only when the complete literal ACL fits the finite native response' do
    Dir.mktmpdir do |root|
      rows = []
      expect { Skvoz::Server::Configuration.new(value.merge('namespace' => 'n' * 256)) }.to raise_error(Skvoz::Server::Error, 'Invalid namespace')
      [[17, 8], [17, 64], [256, 8], [256, 64]].each do |namespace_bytes, devices|
        config = Skvoz::Server::Configuration.new(value.merge('state_dir' => File.join(root, "state-#{namespace_bytes}-#{devices}"),
          'namespace' => (namespace_bytes == 256 ? [63, 63, 63, 64].map { |size| 'n' * size }.join('.') : 'n' * namespace_bytes), 'devices_per_user' => devices))
        state = Skvoz::Server::State.new(config)
        candidate = state.candidate
        candidate['next_id'] = (1 << 63) - 129
        candidate['access_revision'] = (1 << 63) - 2
        state.commit(candidate)
        before = File.binread(File.join(state.directory, 'state.json'))
        if namespace_bytes == 256 && devices == 64
          expect { state.mutate('add', 'a' * 64, password: 'auth-budget-password') }.to raise_error(Skvoz::Server::Error)
          expect(state.value.fetch('users')).to be_empty
          rows << { namespace_bytes: namespace_bytes, reserved_devices: devices, admitted: false }
        else
          candidate, = state.mutate('add', 'a' * 64, password: 'auth-budget-password')
          ids = candidate.fetch('users').fetch('a' * 64).fetch('ids')
          issuer = Skvoz::Server::NatsAuth::Key.new(:account, state.value.fetch('auth').fetch('issuer_seed'))
          curve = Skvoz::Server::NatsAuth::Key.new(:curve, state.value.fetch('auth').fetch('xkey_seed'))
          request = { 'exp' => Time.now.to_i + 2, 'nats' => { 'user_nkey' => Skvoz::Server::NatsAuth::Key.new(:user).public_key,
            'server_id' => { 'id' => Skvoz::Server::NatsAuth::Key.new(:server).public_key, 'xkey' => curve.public_key } } }
          grant = Skvoz::Server::NatsAuth.response(request, issuer, curve, identity: state.identity('a' * 64, candidate),
            permissions: state.permissions('a' * 64, candidate, ids: ids))
          expect(ids.size).to eq(devices)
          expect(grant.bytesize).to be <= 65_588
          expect(grant.bytesize).to be > 16_384 if namespace_bytes == 256
          rows << { namespace_bytes: namespace_bytes, reserved_devices: devices, admitted: true, actual_encrypted_response_bytes: grant.bytesize }
        end
        expect(File.binread(File.join(state.directory, 'state.json'))).to eq(before)
        state.close
      end
      if ENV['SKVOZ_TEST_ARTIFACTS']
        FileUtils.mkdir_p(ENV.fetch('SKVOZ_TEST_ARTIFACTS'), mode: 0o700)
        File.write(File.join(ENV.fetch('SKVOZ_TEST_ARTIFACTS'), 'auth-budget-actual.json'), JSON.pretty_generate(rows))
      end
    end
  end
end
