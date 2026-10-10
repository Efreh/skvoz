# frozen_string_literal: true
require_relative 'spec_helper'
require_relative 'support/runtime_client'

RSpec.describe Skvoz::Server::RuntimeControl do
  def serve(socket)
    size = socket.read(4).unpack1('N')
    request = JSON.parse(socket.read(size))
    yield request
  end

  def reply(socket, value)
    bytes = JSON.generate(value)
    socket.write([bytes.bytesize].pack('N') + bytes)
  end

  def hello_result
    { api: 1, network: 5, role: 'server', capabilities: { profiles: ['tcp'], families: [], max_mtu: 1500, max_channels: 1 } }
  end

  it 'classifies closed and truncated stream reads as EOF before descriptor validation' do
    ['', "\x00"].each do |prefix|
      owner, child = UNIXSocket.pair
      child.write(prefix)
      child.close
      control = described_class.new(owner)
      expect { control.send(:exact, 4) }.to raise_error(Skvoz::Server::Error, 'Runtime control EOF')
      expect(control.owner_eof?).to be(true)
    ensure
      owner.close unless owner.closed?
      child.close unless child.closed?
    end
  end

  it 'keeps one entire command in flight while concurrent status and shutdown retain ordered IDs' do
    owner, child = UNIXSocket.pair
    Async do |task|
      server = task.async do
        serve(child) { |request| reply(child, v: 1, id: request['id'], result: hello_result, error: nil, fd_count: 0) }
        serve(child) do |request|
          expect(request).to include('op' => 'STATUS', 'id' => 2)
          task.sleep(0.03)
          expect(child.recv_nonblock(1, exception: false)).to eq(:wait_readable)
          reply(child, v: 1, id: request['id'], result: {}, error: nil, fd_count: 0)
        end
        serve(child) do |request|
          expect(request).to include('op' => 'PREPARE_SHUTDOWN', 'id' => 3)
          reply(child, v: 1, id: request['id'], result: {}, error: nil, fd_count: 0)
        end
      end
      control = described_class.new(owner).start(task)
      first = task.async { control.request('STATUS') }
      second = task.async { control.prepare_shutdown }
      expect(first.wait).to eq({})
      expect(second.wait).to eq({})
      server.wait
      control.stop
    end.wait
  ensure
    owner.close unless owner.closed?
    child.close unless child.closed?
  end

  it 'bounds callers waiting behind the sole in-flight command' do
    owner, child = UNIXSocket.pair
    Async do |task|
      release = Async::Condition.new
      waiting = false
      server = task.async do
        serve(child) { |request| reply(child, v: 1, id: request['id'], result: hello_result, error: nil, fd_count: 0) }
        8.times do |index|
          serve(child) do |request|
            if index.zero?
              waiting = true
              release.wait
            end
            reply(child, v: 1, id: request['id'], result: {}, error: nil, fd_count: 0)
          end
        end
      end
      control = described_class.new(owner).start(task)
      calls = [task.async { control.request('STATUS') }]
      task.sleep(0.001) until waiting
      7.times { calls << task.async { control.request('STATUS') } }
      expect { control.request('STATUS') }.to raise_error(Skvoz::Server::Error, 'Runtime request budget exceeded')
      release.signal
      calls.each { |call| expect(call.wait).to eq({}) }
      server.wait
      control.stop
    end.wait
  ensure
    owner.close unless owner.closed?
    child.close unless child.closed?
  end

  it 'closes on duplicate JSON, unknown errors, overflowing event sequences and malformed capabilities' do
    vectors = [
      '{"v":1,"id":1,"id":1,"result":{},"error":null,"fd_count":0}',
      JSON.generate(v: 1, id: 1, result: nil, error: 'unknown_error', fd_count: 0),
      JSON.generate(v: 1, seq: (1 << 63), event: 'RUNTIME_STATE', data: { state: 'ready', error: nil }, fd_count: 0),
      JSON.generate(v: 1, seq: 1, event: 'RUNTIME_STATE', data: { state: 'ready', error: 'unknown_error' }, fd_count: 0),
      JSON.generate(v: 1, id: 1, result: hello_result.merge(capabilities: {}), error: nil, fd_count: 0)
    ]
    vectors.each do |bytes|
      owner, child = UNIXSocket.pair
      Async do |task|
        server = task.async { serve(child) { child.write([bytes.bytesize].pack('N') + bytes) } }
        control = described_class.new(owner)
        expect { control.start(task) }.to raise_error(Skvoz::Server::Error)
        expect(control.ready?).to be(false)
        expect(owner).to be_closed
        expect(control.owner_eof?).to be(false)
        server.wait
      end.wait
    ensure
      owner.close unless owner.closed?
      child.close unless child.closed?
    end
  end

  it 'processes state events before a HELLO reply and returns aggregate status without a payload path' do
    owner, child = UNIXSocket.pair
    Async do |task|
      server = task.async do
        serve(child) do |request|
          expect(request).to include('op' => 'HELLO', 'args' => { 'api' => 1, 'network' => 5 })
          reply(child, v: 1, seq: 1, event: 'RUNTIME_STATE', data: { state: 'ready', error: nil }, fd_count: 0)
          reply(child, v: 1, id: request['id'], result: { api: 1, network: 5, role: 'server', capabilities: { profiles: ['tcp'], families: [], max_mtu: 1500, max_channels: 1 } }, error: nil, fd_count: 0)
        end
        serve(child) { |request| reply(child, v: 1, id: request['id'], result: { lifecycle: 'ready', mode: 'server', session: nil, routing: { control_ready: true, eligible_exits: 2 }, counters: described_class::COUNTERS.to_h { |name| [name, name == 'tcp_open' ? 3 : 0] } }, error: nil, fd_count: 0) }
      end
      control = described_class.new(owner).start(task)
      expect(control.ready?).to be(true)
      control.refresh
      expect(control.statistics).to include('tcp_open' => 3)
      server.wait
      control.stop
    end.wait
  ensure
    owner.close unless owner.closed?
    child.close unless child.closed?
  end

  it 'fails pending commands and readiness when its sole runtime owner closes' do
    owner, child = UNIXSocket.pair
    Async do |task|
      task.async { serve(child) { child.close } }
      control = described_class.new(owner)
      expect { control.start(task) }.to raise_error(Skvoz::Server::Error, 'Runtime control unavailable')
      expect(control.ready?).to be(false)
      expect(control.failure).to eq('runtime_control_failed')
      expect(control.owner_eof?).to be(true)
    end.wait
  ensure
    owner.close unless owner.closed?
    child.close unless child.closed?
  end

  it 'retains a validated terminal reason after owner EOF' do
    owner, child = UNIXSocket.pair
    Async do |task|
      server = task.async do
        serve(child) { |request| reply(child, v: 1, id: request['id'], result: hello_result, error: nil, fd_count: 0) }
        serve(child) do
          reply(child, v: 1, seq: 1, event: 'RUNTIME_STATE', data: { state: 'closing', error: 'network_unavailable' }, fd_count: 0)
          child.close
        end
      end
      control = described_class.new(owner).start(task)
      expect { control.request('STATUS') }.to raise_error(Skvoz::Server::Error, 'Runtime control unavailable')
      expect(control.failure).to eq('runtime_network_unavailable')
      expect(control.owner_eof?).to be(true)
      expect(control.ready?).to be(false)
      server.wait
      control.stop
    end.wait
  ensure
    owner.close unless owner.closed?
    child.close unless child.closed?
  end

  it 'rejects unsolicited SCM_RIGHTS and closes received descriptors' do
    owner, child = UNIXSocket.pair
    input, output = IO.pipe
    Async do |task|
      task.async do
        serve(child) do |request|
          bytes = JSON.generate(v: 1, id: request['id'], result: {}, error: nil, fd_count: 1)
          child.sendmsg([bytes.bytesize].pack('N') + bytes, 0, nil, Socket::AncillaryData.unix_rights(input))
        end
      end
      control = described_class.new(owner)
      expect { control.start(task) }.to raise_error(Skvoz::Server::Error)
      expect(control.ready?).to be(false)
    end.wait
  ensure
    [owner, child, input, output].each { |io| io.close unless io.closed? }
  end
end

RSpec.describe ServerSystem::RuntimeClient do
  it 'returns a completed shutdown acknowledgment before subsequent EOF and fails later requests' do
    owner, child = UNIXSocket.pair
    client = described_class.new(owner)
    peer = Thread.new do
      request = JSON.parse(child.read(child.read(4).unpack1('N')))
      bytes = JSON.generate(v: 1, id: request.fetch('id'), result: {}, error: nil, fd_count: 0)
      child.write([bytes.bytesize].pack('N') + bytes)
      child.close
    end
    allow(owner).to receive(:write).and_wrap_original do |write, bytes|
      result = write.call(bytes)
      # Hold the caller until the real reader has observed both ACK and EOF.
      reader = client.instance_variable_get(:@reader)
      expect(reader.join(3)).to eq(reader)
      result
    end
    response, descriptors = client.request('PREPARE_SHUTDOWN')
    expect(response).to include('id' => 1, 'result' => {}, 'error' => nil)
    expect(descriptors).to be_empty
    expect { client.request('STATUS') }.to raise_error(IOError, 'Runtime control EOF')
    peer.value
  ensure
    client&.close
    peer&.join(1)
    child&.close unless child&.closed?
  end

  it 'rejects EOF without a complete acknowledgment, including a truncated response' do
    ['', [20].pack('N') + '{"v":1'].each do |prefix|
      owner, child = UNIXSocket.pair
      client = described_class.new(owner)
      input, output = IO.pipe
      inode = File.readlink("/proc/self/fd/#{input.fileno}")
      owned_descriptors = -> { Dir.glob('/proc/self/fd/*').count { |path| File.readlink(path) == inode rescue false } }
      before = owned_descriptors.call
      peer = Thread.new do
        child.read(child.read(4).unpack1('N'))
        child.sendmsg(prefix, 0, nil, Socket::AncillaryData.unix_rights(input)) unless prefix.empty?
        child.close
      end
      expect { client.request('PREPARE_SHUTDOWN') }.to raise_error(IOError, 'Runtime control EOF')
      expect { client.request('STATUS') }.to raise_error(IOError, 'Runtime control EOF')
      peer.value
      expect(owned_descriptors.call).to eq(before)
    ensure
      client&.close
      peer&.join(1)
      child&.close unless child&.closed?
      input&.close unless input&.closed?
      output&.close unless output&.closed?
    end
  end
end
