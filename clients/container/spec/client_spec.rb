# frozen_string_literal: true
require_relative '../lib/skvoz/client'
require 'tmpdir'

RSpec.describe Skvoz::Client do
  def config(extra = {})
    Skvoz::Client::Config.new({ 'server' => 'entry.example:4222', 'username' => 'example', 'password' => 'example-password', 'proxy' => { 'http' => '127.0.0.1:18080' } }.merge(extra).to_yaml)
  end
  it('normalizes an explicit proxy profile') { expect(config.proxy).to eq('http' => '127.0.0.1:18080') }
  it('accepts bracketed IPv6 endpoints') { expect(config('server' => '[2001:db8::1]:4222').host).to eq('2001:db8::1') }
  it('rejects duplicate YAML keys') do
    expect { Skvoz::Client::Config.new("server: one:4222\nserver: two:4222\nusername: user\npassword: example-password\n") }.to raise_error(Skvoz::Client::Terminal, 'duplicate_key')
  end
  it('rejects YAML aliases') { expect { Skvoz::Client::Config.new("server: &x a:1\nusername: *x\npassword: example-password\n") }.to raise_error(Skvoz::Client::Terminal) }
  it('rejects unknown keys') { expect { config('typo' => true) }.to raise_error(Skvoz::Client::Terminal) }
  it('rejects combined VPN and proxy configuration') { expect { config('mode' => 'vpn') }.to raise_error(Skvoz::Client::Terminal, 'incompatible_modes') }
  it('rejects public or wildcard listeners') do
    %w[0.0.0.0:8080 8.8.8.8:8080 [::]:8080].each { |address| expect { config('proxy' => { 'http' => address }) }.to raise_error(Skvoz::Client::Terminal) }
  end
  it('rejects conflicting listener endpoints') do
    expect { config('tcp' => { 'api' => { 'listen' => '127.0.0.1:18080', 'target' => 'example.org:443' } }) }.to raise_error(Skvoz::Client::Terminal, 'listener_conflict')
  end
  it('rejects upstream credentials and queries') do
    %w[https://user:password@example.org/ https://example.org/?q=1].each do |url|
      expect { config('http' => { 'api' => { 'listen' => '127.0.0.1:18081', 'upstream' => url } }) }.to raise_error(Skvoz::Client::Terminal)
    end
  end
  it('persists one device and excludes concurrent state owners') do
    Dir.mktmpdir do |parent|
      state = Skvoz::Client::State.new(File.join(parent, 'state'))
      device = state.device
      expect { Skvoz::Client::State.new(state.directory) }.to raise_error(Skvoz::Client::Terminal, 'state_in_use')
      state.close
      second = Skvoz::Client::State.new(state.directory)
      expect(second.device).to eq(device)
      expect(File.stat(second.path('device.json')).mode & 0o777).to eq(0o600)
      second.close
    end
  end
  it('rejects symlink state directories') do
    Dir.mktmpdir do |parent|
      Dir.mkdir(File.join(parent, 'real'), 0o700)
      File.symlink(File.join(parent, 'real'), File.join(parent, 'link'))
      expect { Skvoz::Client::State.new(File.join(parent, 'link')) }.to raise_error(Skvoz::Client::Terminal)
    end
  end
  it('transfers exactly one descriptor on the first frame byte') do
    left, right = UNIXSocket.pair
    reader, writer = IO.pipe
    record = { 'fd_count' => 1, 'payload' => 'binary' }
    Skvoz::Client::Wire.new(left).write(record, writer)
    value, received = Skvoz::Client::Wire.new(right).read
    expect(value).to eq(record)
    received.write('ok'); received.flush
    expect(reader.read(2)).to eq('ok')
  ensure
    [left, right, reader, writer, received].compact.each(&:close)
  end
  it('rejects a descriptor attached after the first byte') do
    left, right = UNIXSocket.pair
    reader, writer = IO.pipe
    bytes = [14].pack('N') + '{"fd_count":1}'
    left.write(bytes.byteslice(0, 1))
    left.sendmsg(bytes.byteslice(1..), 0, nil, Socket::AncillaryData.unix_rights(writer))
    expect { Skvoz::Client::Wire.new(right).read }.to raise_error(Skvoz::Client::Error, 'misplaced_control_rights')
  ensure
    [left, right, reader, writer].compact.each(&:close)
  end
  it('rejects oversized control records before allocation') do
    left, right = UNIXSocket.pair
    left.write([32769].pack('N'))
    expect { Skvoz::Client::Wire.new(right).read }.to raise_error(Skvoz::Client::Error, 'invalid_control_length')
  ensure
    [left, right].compact.each(&:close)
  end

  def with_gateway(upstream_handler)
    target = TCPServer.new('127.0.0.1', 0)
    listener = TCPServer.new('127.0.0.1', 0)
    port = listener.local_address.ip_port
    listener.close
    received = Queue.new
    worker = Thread.new do
      remote = target.accept
      upstream_handler.call(remote, received)
    ensure
      remote&.close
    end
    profile = config('proxy' => false, 'http' => { 'api' => { 'listen' => "127.0.0.1:#{port}", 'upstream' => "http://upstream.example:#{target.local_address.ip_port}/base/" } })
    control = Object.new
    control.define_singleton_method(:open) { |_host, _port| Socket.tcp('127.0.0.1', target.local_address.ip_port) }
    Async do |task|
      adapters = Skvoz::Client::Adapters.new(profile, control, task)
      adapters.start
      socket = TCPSocket.new('127.0.0.1', port)
      yield socket, received, target
    ensure
      socket&.close
      adapters&.stop
    end.wait
    worker.join(1)
  ensure
    target&.close
    worker&.kill if worker&.alive?
  end
  it('streams binary POST bytes and joins paths while preserving end-to-end headers') do
    handler = proc do |socket, queue|
      head = ''.b
      head << socket.read(1) until head.end_with?("\r\n\r\n")
      length = head[/content-length: (\d+)/i, 1].to_i
      queue << [head, socket.read(length)]
      socket.write("HTTP/1.1 200 OK\r\nContent-Length: 3\r\nConnection: close\r\n\r\nok!")
    end
    with_gateway(handler) do |socket, received|
      payload = "\x00\xfftest".b
      socket.write("POST /v1?q=a%20b HTTP/1.1\r\nHost: ignored.example\r\nAuthorization: Bearer example\r\nConnection: close, x-remove\r\nX-Remove: private\r\nContent-Length: #{payload.bytesize}\r\n\r\n" + payload)
      response = socket.read
      expect(response).to include('200 OK', 'ok!')
      head, body = received.pop
      expect(head).to include('POST /base/v1?q=a%20b HTTP/1.1', 'Authorization: Bearer example')
      expect(head.downcase).to include('host: upstream.example:')
      expect(head.downcase).not_to include('x-remove:', 'ignored.example')
      expect(body).to eq(payload)
    end
  end
  it('flushes chunked event data before the upstream finishes') do
    handler = proc do |socket, _queue|
      head = +''; head << socket.read(1) until head.end_with?("\r\n\r\n")
      socket.write("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nContent-Type: text/event-stream\r\n\r\n9\r\ndata: a\n\n\r\n")
      sleep(0.3)
      socket.write("9\r\ndata: b\n\n\r\n0\r\n\r\n")
    end
    with_gateway(handler) do |socket, _received|
      start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      socket.write("GET /stream HTTP/1.1\r\nHost: gateway\r\nConnection: close\r\n\r\n")
      first = +''; first << socket.readpartial(4096) until first.include?('data: a')
      expect(Process.clock_gettime(Process::CLOCK_MONOTONIC) - start).to be < 0.25
      expect(first + socket.read).to include('data: b')
    end
  end
  it('accepts100-continue before streaming the request body upstream') do
    handler = proc do |socket, queue|
      head = +''; head << socket.read(1) until head.end_with?("\r\n\r\n")
      queue << socket.read(head[/content-length: (\d+)/i, 1].to_i)
      socket.write("HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
    end
    with_gateway(handler) do |socket, received|
      socket.write("POST /upload HTTP/1.1\r\nHost: gateway\r\nExpect: 100-continue\r\nContent-Length: 4\r\nConnection: close\r\n\r\n")
      interim = +''; interim << socket.read(1) until interim.end_with?("\r\n\r\n")
      expect(interim).to include('100 Continue')
      socket.write('body')
      expect(socket.read).to include('200 OK')
      expect(received.pop).to eq('body')
    end
  end

  {
    'Upgrade' => "GET / HTTP/1.1\r\nHost: gateway\r\nConnection: close, upgrade\r\nUpgrade: websocket\r\n\r\n",
    'proxy-form' => "GET http://other.example/private HTTP/1.1\r\nHost: gateway\r\nConnection: close\r\n\r\n"
  }.each do |kind, request|
    it("rejects #{kind} without opening an upstream") do
      handler = proc { |_socket, received| received << :opened }
      with_gateway(handler) do |socket, received, target|
        socket.write(request)
        expect(socket.read).to include('400 Bad Request')
        expect(received).to be_empty
        expect { target.accept_nonblock }.to raise_error(IO::WaitReadable)
      end
    end
  end

  it('returns redirects without following the Location or making another upstream request') do
    handler = proc do |socket, received|
      head = +''; head << socket.read(1) until head.end_with?("\r\n\r\n")
      received << head
      socket.write("HTTP/1.1 302 Found\r\nLocation: http://another.example/private\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
    end
    with_gateway(handler) do |socket, received, target|
      socket.write("GET /redirect HTTP/1.1\r\nHost: gateway\r\nConnection: close\r\n\r\n")
      response = socket.read
      expect(response).to include('302 Found', 'http://another.example/private')
      expect(received.pop).to include('GET /base/redirect HTTP/1.1')
      expect(received).to be_empty
      expect { target.accept_nonblock }.to raise_error(IO::WaitReadable)
    end
  end

end

RSpec.describe 'HTTP parser boundary' do
  it 'rejects an unterminated oversized request line before opening an upstream' do
    Async do |task|
      left, right = UNIXSocket.pair
      parser = Skvoz::Client::HttpServer.new(::IO::Stream(left), 'HTTP/1.1')
      reader = task.async { parser.next_request }
      right.write('G' * 9000)
      expect { reader.wait }.to raise_error(::Protocol::HTTP1::LineLengthError)
    ensure
      parser&.close
      right&.close
    end.wait
  end
end
