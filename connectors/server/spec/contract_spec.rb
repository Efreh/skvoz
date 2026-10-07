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
      second, another = state.mutate('device-add', 'shared', password: 'long enough password')
      state.commit(second)
      expect(another).not_to eq(id)
      expect { state.mutate('device-add', 'shared', password: 'long enough password') }.to raise_error(Skvoz::Server::Error, 'Device pool exhausted')
      removed, = state.mutate('remove', 'shared'); state.commit(removed)
      replacement, next_id = state.mutate('add', 'new', password: 'long enough password'); state.commit(replacement)
      expect(next_id).to be > another
      candidate = state.candidate; candidate['tls'] = { 'certificate' => 'unused.pem', 'key' => 'unused.key' }; state.commit(candidate)
      profile = state.profile(Skvoz::Server::NetworkConfiguration.new(config))
      expect(profile).to include('v' => 1, 'role' => 'server')
      expect(profile.fetch('core')).to include('peer_id' => '0', 'membership' => 'broker_authorized', 'ca_file' => nil, 'tls_server_name' => 'example.org')
      expect(state.nats_config).to include('skvoz.enroll.v2.')
      expect(state.export('new', next_id, 'long enough password')).to include('v' => 2,
        'network_runtime' => { 'network' => 4, 'api' => 1, 'version' => '0.4.1', 'core' => '4.0.1' })
      state.close
      reopened = Skvoz::Server::State.new(config)
      expect(reopened.value['next_id']).to eq(5)
      expect(reopened.value['users']['new']['assigned']).to eq([next_id])
      reopened.close
    end
  end
end
