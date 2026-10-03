# frozen_string_literal: true
require_relative 'spec_helper'

RSpec.describe 'Server destination and persistent identity contract' do

  it 'preserves public IPC vectors in its encoder and decoder' do
    path = File.expand_path('../../../daemon/tests/fixtures/ipc-v1.tsv', __dir__)
    count = 0
    File.foreach(path) do |line|
      next if line.start_with?('#') || line.strip.empty?
      _name, hex = line.strip.split("\t")
      encoded = [hex].pack('H*')
      body = encoded.byteslice(4..)
      _magic, _version, kind, request, high, low = body.unpack('a4nnQ>Q>Q>')
      frame = Skvoz::Server::Protocol.encode(kind, request, (high << 64) | low, body.byteslice(32..))
      expect(frame.unpack1('H*')).to eq(hex)
      count += 1
    end
    expect(count).to eq(5)
  end

  it 'loads the standalone IPC adapter without destination policy dependencies' do
    code = 'require "skvoz/server/ipc_session"; abort unless Skvoz::Server::Protocol::OPENED == 0x9002; abort if defined?(Skvoz::Server::Destination)'
    library = File.expand_path('../lib', __dir__)
    expect(system(RbConfig.ruby, '-I', library, '-e', code)).to be(true)
  end

  it 'rejects ambiguous metadata, unicode, invalid port and numeric lookalikes' do
    [ { v: 1, type: 'tcp', host: '127.1', port: 80 }, { v: 1, type: 'tcp', host: 'münich.example', port: 80 },
      { v: 1, type: 'tcp', host: '[::1]', port: 80 }, { v: 1, type: 'tcp', host: 'fe80::1%br-example', port: 80 },
      { v: 1, type: 'tcp', host: 'example.org', port: '80' },
      { v: 1, type: 'tcp', host: 'example.org', port: 80, password: 'discard' } ].each do |metadata|
      expect { Skvoz::Server::Destination.new(Skvoz::Server::Protocol.metadata(metadata)) }.to raise_error(Skvoz::Server::DestinationError, 'invalid_destination')
    end
    expect { Skvoz::Server::Destination.new('{"v":1,"type":"tcp","host":"example.org","host":"127.0.0.1","port":80}') }.to raise_error(Skvoz::Server::DestinationError, 'invalid_destination')
  end

  it 'denies special/local/mapped addresses by default and permits an explicit port bridge' do
    interfaces = Socket.ip_address_list
    bridge_address = instance_double(Addrinfo, ipv4?: false, ipv6?: true, ip_address: 'fe80::1%br-example')
    allow(Socket).to receive(:ip_address_list).and_return(interfaces + [bridge_address])
    default = Skvoz::Server::Policy.new
    %w[127.0.0.1 10.1.2.3 169.254.169.254 192.0.2.1 ::1 ::ffff:127.0.0.1 2001:db8::1 3fff::1 ff02::1].each do |address|
      expect(default.allowed?(address, 8081)).to be(false)
    end
    expect(default.allowed?('8.8.8.8', 443)).to be(true)
    expect(default.allowed?('2606:4700:4700::1111', 443)).to be(true)
    bridge = Skvoz::Server::Policy.new(allow: [{ 'cidr' => '127.0.0.0/8', 'ports' => [8081, 4222] }], own_endpoints: [['0.0.0.0', 4222]])
    expect(bridge.allowed?('127.0.0.1', 8081)).to be(true)
    expect(bridge.allowed?('::ffff:127.0.0.1', 8081)).to be(true)
    expect(bridge.allowed?('127.0.0.1', 8082)).to be(false)
    expect(bridge.allowed?('127.0.0.2', 4222)).to be(false)
    expect { bridge.check!(%w[8.8.8.8 10.0.0.1], 443) }.to raise_error(Skvoz::Server::DestinationError, 'forbidden')
  end

  it 'bounds both queue counts and bytes until actual release' do
    Async do
      global = Skvoz::Server::Budget.new(count: 2, bytes: 16)
      queue = Skvoz::Server::Queue.new(count: 2, bytes: 16, global:)
      expect(queue.push('first', bytes: 8)).to be(true)
      item, bytes = queue.pop
      expect(item).to eq('first')
      expect(queue.push('second', bytes: 8)).to be(true)
      expect(queue.push('third', bytes: 1)).to be(false)
      queue.release(bytes)
      expect(queue.push('third', bytes: 1)).to be(true)
      queue.close
      expect(global.count).to eq(0)
      expect(global.bytes).to eq(0)
    end.wait
  end

  it 'rejects a receive window that its bounded host queue cannot hold' do
    value = { 'address' => 'example.org', 'tls' => { 'email' => 'test@example.com', 'terms_agreed' => true } }
    expect { Skvoz::Server::Configuration.new(value.merge('stream_queue_bytes' => 8192)) }.to raise_error(Skvoz::Server::Error, 'Configured stream queue cannot hold receive window')
    expect { Skvoz::Server::Configuration.new(value.merge('receive_window' => 8192, 'stream_queue_bytes' => 8192)) }.not_to raise_error
    expect { Skvoz::Server::Configuration.new(value.merge('receive_window' => 1_048_577)) }.to raise_error(Skvoz::Server::Error, 'Invalid server configuration limit')
  end

  it 'creates private state beneath a safe0755 parent and rejects symlink/writable ancestors' do
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

  it 'allocates unique durable device profiles and burns historical IDs without burning active capacity' do
    Dir.mktmpdir do |temporary|
      config = Skvoz::Server::Configuration.new('state_dir' => File.join(temporary, 'state'), 'address' => 'example.org',
                                 'devices_per_user' => 2, 'max_identities' => 2,
                                 'tls' => { 'email' => 'test@example.com', 'terms_agreed' => true })
      state = Skvoz::Server::State.new(config)
      first, id = state.mutate('add', 'shared', password: 'long enough password')
      state.commit(first)
      second, another = state.mutate('device-add', 'shared', password: 'long enough password')
      state.commit(second)
      expect(another).not_to eq(id)
      expect { state.mutate('device-add', 'shared', password: 'long enough password') }.to raise_error(Skvoz::Server::Error, 'Device pool exhausted')
      removed, = state.mutate('remove', 'shared')
      state.commit(removed)
      replacement, next_id = state.mutate('add', 'new', password: 'long enough password')
      state.commit(replacement)
      expect(next_id).to be > another
      state.close
      reopened = Skvoz::Server::State.new(config)
      expect(reopened.value['next_id']).to eq(5)
      expect(reopened.value['users']['new']['assigned']).to eq([next_id])
      expect(reopened.value['users']['new']['hash']).not_to eq('long enough password')
      reopened.close
    end
  end
end
