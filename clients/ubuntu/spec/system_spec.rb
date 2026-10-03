# frozen_string_literal: true
require 'ipaddr'
require_relative '../../../connectors/server/spec/spec_helper'
require_relative '../../../connectors/server/spec/support/system'
require_relative 'support/application'

RSpec.describe 'Native Ubuntu application through real TLS NATS', integration: true do
  include ServerSystem

  around do |example|
    Dir.mktmpdir('skvoz-native-client-') do |directory|
      @directory = Pathname.new(directory)
      @applications = []
      @requests = ::Queue.new
      @target = ServerSystem::Target.new do |socket|
        header = ''.b
        header << (socket.read(1) || raise(EOFError)) until header.end_with?("\r\n\r\n") || header.bytesize > 16_384
        length = header[/\r\nContent-Length: (\d+)/i, 1].to_i
        body = length.positive? ? socket.read(length) : 'skvoz-real-http-response'.b
        @requests << [header, body]
        socket.write("HTTP/1.1 200 OK\r\nContent-Length: #{body.bytesize}\r\nConnection: close\r\n\r\n".b + body)
      end
      certificates(@directory, san: 'DNS:localhost,IP:127.0.0.1,IP:::1')
      context = OpenSSL::SSL::SSLContext.new
      context.cert = OpenSSL::X509::Certificate.new(@directory.join('server.pem').read)
      context.key = OpenSSL::PKey.read(@directory.join('server.key').read)
      @tls_target = ServerSystem::Target.new do |raw|
        socket = OpenSSL::SSL::SSLSocket.new(raw, context); socket.accept
        header = ''.b
        header << (socket.read(1) || raise(EOFError)) until header.end_with?("\r\n\r\n")
        length = header[/\r\nContent-Length: (\d+)/i, 1].to_i
        body = length.positive? ? socket.read(length) : 'skvoz-real-tls-response!'.b
        socket.write("HTTP/1.1 200 OK\r\nContent-Length: #{body.bytesize}\r\nConnection: close\r\n\r\n".b + body)
        socket.close
      end
      @echo = ServerSystem::Target.new { |socket| ServerSystem.after_fin(socket) }
      @echo6 = ServerSystem::Target.new(host: '::1') { |socket| ServerSystem.after_fin(socket) }
      @server = ServerSystem::Server.new(@directory, allow: [{ 'cidr' => '127.0.0.0/8', 'ports' => [@target.port, @tls_target.port, @echo.port] }, { 'cidr' => '::1/128', 'ports' => [@target.port, @tls_target.port, @echo.port, @echo6.port] }],
                              overrides: { 'devices_per_user' => 4, 'max_identities' => 16 })
      @server.command('add', 'shared', 'process-test-password')
      @server.command('add', 'other', 'process-other-password')
      example.run
    ensure
      @applications&.each(&:close)
      @server&.close
      @ingress6&.close; @target&.close; @tls_target&.close; @echo&.close; @echo6&.close
    end
  end

  def application(name, **options)
    app = UbuntuSystem::Application.new(@directory.join(name), @server, **options)
    @applications << app
    expect(app.ready).to include('ready' => true)
    app
  end

  def curl(app, url, socks: false, arguments: [], input: '')
    proxy = "#{socks ? 'socks5h' : 'http'}://127.0.0.1:#{socks ? app.socks : app.http}"
    stdout, stderr, status = capture('curl', '--silent', '--show-error', '--fail', '--max-time', '20', '--noproxy', '', '--proxy', proxy, *arguments, url, input:, timeout: 25)
    expect(status.success?).to be(true), stderr
    stdout.b
  end

  def tunnel(app, port = @echo.port, socks: false, host: '127.0.0.1')
    socket = TCPSocket.new('127.0.0.1', socks ? app.socks : app.http)
    if socks
      socket.write("\5\1\0".b)
      expect(socket.read(2)).to eq("\5\0".b)
      socket.write([5, 1, 0, 3, host.bytesize].pack('C*') + host + [port].pack('n'))
      expect(socket.read(10)).to eq([5, 0, 0, 1, 0, 0, 0, 0, 0, 0].pack('C*'))
    else
      socket.write("CONNECT #{host}:#{port} HTTP/1.1\r\nHost: #{host}:#{port}\r\n\r\n")
      header = ''.b; header << (socket.read(1) || raise(EOFError)) until header.end_with?("\r\n\r\n")
      expect(header).to include('200 Connection Established')
    end
    socket
  rescue Exception
    socket&.close
    raise
  end

  it 'routes HTTP bodies, real HTTPS CONNECT, SOCKS domains/IP and two persistent device identities' do
    first, second = application('one'), application('two')
    expect(first.ready['peer_id']).not_to eq(second.ready['peer_id'])
    [first, second].each do |app|
      expect(curl(app, "http://localhost:#{@target.port}/p?q=1")).to eq('skvoz-real-http-response')
      expect(curl(app, "https://localhost:#{@tls_target.port}/", arguments: ['--cacert', @directory.join('ca.pem')])).to eq('skvoz-real-tls-response!')
      expect(curl(app, "http://localhost:#{@target.port}/", socks: true)).to eq('skvoz-real-http-response')
      expect(curl(app, "http://127.0.0.1:#{@target.port}/", socks: true)).to eq('skvoz-real-http-response')
    end
    payload = ("\0binary\xff".b * 4096)
    started = monotonic
    expect(curl(first, "http://localhost:#{@target.port}/upload", arguments: ['--data-binary', '@-', '--proxy-header', 'Proxy-Authorization: secret'], input: payload)).to eq(payload)
    if ENV['SKVOZ_CLIENT_REPORT']
      rss = [first.process.pid, first.ready['pid']].map { |pid| File.read("/proc/#{pid}/status")[/^VmRSS:\s+(\d+) kB/, 1].to_i }
      File.write(ENV.fetch('SKVOZ_CLIENT_REPORT'), JSON.pretty_generate(payload_bytes: payload.bytesize, duration_seconds: monotonic - started, client_rss_kib: rss[0], daemon_rss_kib: rss[1], scope: 'single loopback POST; not a capacity guarantee'))
    end
    requests = []; requests << @requests.pop until @requests.empty?
    header, bytes = requests.last
    expect(header).to start_with("POST /upload HTTP/1.1\r\n")
    expect(header.downcase).not_to include('proxy-authorization', 'proxy-connection')
    expect(bytes).to eq(payload)
    @ingress6 = ServerSystem::Target.new(host: '::1') do |socket|
      remote = TCPSocket.new('127.0.0.1', @server.port)
      sender = Thread.new { IO.copy_stream(socket, remote); remote.close_write }
      IO.copy_stream(remote, socket)
      socket.close_write
      sender.join
    ensure
      remote&.close
      sender&.kill
    end
    ipv6 = application('ipv6', host: '[::1]', server_port: @ingress6.port)
    socket = TCPSocket.new('127.0.0.1', ipv6.socks)
    socket.write("\5\1\0".b)
    expect(socket.read(2)).to eq("\5\0".b)
    socket.write([5, 1, 0, 4].pack('C*') + IPAddr.new('::1').hton + [@echo6.port].pack('n'))
    expect(socket.read(10)).to eq([5, 0, 0, 1, 0, 0, 0, 0, 0, 0].pack('C*'))
    socket.write('ipv6'); socket.close_write
    expect(Timeout.timeout(10) { socket.read }).to eq('after-fin:6vpi')
    socket.close
    id = first.ready['peer_id']; first.close
    restored = application('one', password: nil)
    expect(restored.ready['peer_id']).to eq(id)
    expect(curl(restored, "http://localhost:#{@target.port}/")).to eq('skvoz-real-http-response')
    settings = restored.directory.join('settings.json')
    expect(settings.stat.mode & 0o777).to eq(0o600)
    expect(restored.directory.stat.mode & 0o777).to eq(0o700)
    expect(JSON.parse(settings.read).fetch('passwords').values).to include('process-test-password')
  end

  it 'preserves multi-window binary transfers through HTTP, TLS CONNECT and SOCKS with another device active' do
    bulk, healthy = application('bulk', request_log: ENV.fetch('SKVOZ_REQUEST_LOG', '1') == '1'), application('healthy')
    started = monotonic
    payload = (0..255).to_a.pack('C*') * 16_384
    transfers = Thread.new do
      [false, true].each do |socks|
        expect(curl(bulk, "http://localhost:#{@target.port}/bulk", socks:, arguments: ['--data-binary', '@-'], input: payload)).to eq(payload)
      end
      expect(curl(bulk, "https://localhost:#{@tls_target.port}/bulk", arguments: ['--cacert', @directory.join('ca.pem'), '--data-binary', '@-'], input: payload)).to eq(payload)
    end
    4.times { expect(curl(healthy, "http://localhost:#{@target.port}/")).to eq('skvoz-real-http-response') }
    expect(transfers.join(60)).not_to be_nil
    transfers.value
    if ENV['SKVOZ_CLIENT_BULK_REPORT']
      File.write(ENV.fetch('SKVOZ_CLIENT_BULK_REPORT'), JSON.pretty_generate(bytes_per_direction: payload.bytesize * 3, seconds: monotonic - started, request_log: ENV.fetch('SKVOZ_REQUEST_LOG', '1') == '1', client_rss_kib: File.read("/proc/#{bulk.process.pid}/status")[/^VmRSS:\s+(\d+) kB/, 1].to_i, scope: 'loopback real HTTP/SOCKS/CONNECT, three 4MiB uploads and responses plus concurrent device; excludes startup'))
    end
    [bulk, healthy].each { |app| wait_until { app.info['connections'].zero? } }
    limits = JSON.parse(@server.state.join('core-profile.json').read).fetch('limits')
    expect(limits.fetch('receive_window')).to eq(1_048_576)
    expect(limits.fetch('max_frame')).to eq(32_768)
  ensure
    transfers&.kill if transfers&.alive?
  end

  it 'preserves binary half-close with slow consumers and isolates a tiny event budget' do
    healthy = application('one')
    [:http, :socks].each do |type|
      socket = tunnel(healthy, socks: type == :socks, host: 'localhost')
      payload = "\0half-close\xff".b * 4096
      socket.write(payload); socket.close_write
      sleep 0.2
      expect(Timeout.timeout(15) { socket.read }).to eq('after-fin:'.b + payload.reverse)
      socket.close
    end
    tiny = application('tiny', budgets: { event_frames: 1, event_bytes: 1024 })
    socket = tunnel(tiny)
    socket.write('x' * 65536); socket.close_write
    result = Timeout.timeout(10) { socket.read }
    expect(result.bytesize).to be < 65546
    wait_until { tiny.info['connections'].zero? }
    socket.close
    expect(curl(healthy, "http://localhost:#{@target.port}/")).to eq('skvoz-real-http-response')
    wait_until { healthy.info['connections'].zero? }
  end

  it 'recovers new streams after child and broker loss without changing device identity' do
    app = application('one')
    id = app.ready['peer_id']; app.command('kill-core')
    app.event { |value| value['state'] == 'reconnecting' }
    app.event { |value| value['state'] == 'connected' }
    expect(app.info['peer_id']).to eq(id)
    expect(curl(app, "http://localhost:#{@target.port}/")).to eq('skvoz-real-http-response')
    @server.stop
    app.event { |value| value['state'] == 'reconnecting' }
    @server.start
    app.event(timeout: 45) { |value| value['state'] == 'connected' }
    expect(curl(app, "http://localhost:#{@target.port}/")).to eq('skvoz-real-http-response')
    runtime = app.info['runtime']; app.close
    expect(File.exist?(runtime)).to be(false)
  end

  it 'keeps connection intent across resume and initial offline startup, but leaves a disconnected application idle' do
    app = application('wake')
    id = app.ready['peer_id']
    active = tunnel(app)
    active.write('interrupted')
    app.command('resume')
    app.event { |value| value['state'] == 'reconnecting' }
    app.event { |value| value['state'] == 'connected' }
    expect(app.info['peer_id']).to eq(id)
    expect(Timeout.timeout(5) { active.read }).to eq('')
    active.close
    expect(curl(app, "http://localhost:#{@target.port}/")).to eq('skvoz-real-http-response')
    app.command('disconnect')
    app.event { |value| value['state'] == 'disconnected' }
    app.command('resume')
    expect(app.info['state']).to eq('disconnected')
    expect { TCPSocket.new('127.0.0.1', app.http) }.to raise_error(Errno::ECONNREFUSED)
    @server.stop
    offline = UbuntuSystem::Application.new(@directory.join('offline'), @server, wait_ready: false)
    @applications << offline
    offline.event { |value| value['state'] == 'reconnecting' }
    # Info and resume notifications during a retry must not cancel intent.
    offline.command('info'); offline.command('resume')
    @server.start
    offline.event(timeout: 45) { |value| value['state'] == 'connected' }
    expect(curl(offline, "http://localhost:#{@target.port}/")).to eq('skvoz-real-http-response')
    expect(app.info['state']).to eq('disconnected')
  ensure
    active&.close
  end

  it 'counts actual payload directions and records bounded destination metadata without content' do
    app = application('observed')
    [false, true].each do |socks|
      before = app.info
      socket = tunnel(app, socks:)
      payload = 'private-payload-query=password' * 4096
      socket.write(payload); socket.close_write
      expect(Timeout.timeout(15) { socket.read }).to eq('after-fin:' + payload.reverse)
      socket.close
      wait_until { app.info['connections'].zero? }
      info = app.info
      expect(info.fetch('uploaded') - before.fetch('uploaded')).to eq(payload.bytesize)
      expect(info.fetch('downloaded') - before.fetch('downloaded')).to eq(payload.bytesize + 'after-fin:'.bytesize)
      requests = info.fetch('requests')
      expect(requests.length).to be <= 500
      expect(requests.last).to include('protocol' => socks ? 'SOCKS5' : 'CONNECT', 'host' => '127.0.0.1', 'port' => @echo.port, 'result' => 'finished', 'uploaded' => payload.bytesize)
      expect(JSON.generate(requests)).not_to include('private-payload', 'query=password', 'process-test-password')
    end
    expect(curl(app, "http://localhost:#{@target.port}/private-path?secret=hidden")).to eq('skvoz-real-http-response')
    wait_until { app.info['connections'].zero? }
    requests = app.info.fetch('requests')
    expect(requests.last).to include('protocol' => 'HTTP', 'host' => 'localhost')
    expect(JSON.generate(requests)).not_to include('private-path', 'hidden')
    quiet = application('unobserved', request_log: false)
    expect(curl(quiet, "http://localhost:#{@target.port}/")).to eq('skvoz-real-http-response')
    info = quiet.info
    expect(info.fetch('uploaded')).to be > 0
    expect(info.fetch('downloaded')).to be > 0
    expect(info.fetch('requests', [])).to eq([])
  end

  it 'rejects bad credentials/trust, ambiguous framing, forbidden targets and excessive local handshakes' do
    [{ password: 'invalid-test-password' }, { ca: false }].each_with_index do |options, index|
      app = UbuntuSystem::Application.new(@directory.join("negative#{index}"), @server, **options)
      @applications << app
      expect(app.ready['failed']).to eq(index.zero? ? 'authentication_failed' : 'certificate_failed')
    end
    app = application('one', budgets: { max_connections: 2 })
    sockets = 2.times.map { TCPSocket.new('127.0.0.1', app.http) }
    third = TCPSocket.new('127.0.0.1', app.http)
    expect(Timeout.timeout(2) { third.read }).to eq(''); third.close
    sockets.each(&:close)
    wait_until { app.info['connections'].zero? }
    ['Content-Length: 1\r\nTransfer-Encoding: chunked'.gsub('\\r\\n', "\r\n"), 'Content-Length: 1\r\nConnection: content-length'.gsub('\\r\\n', "\r\n")].each do |headers|
      socket = TCPSocket.new('127.0.0.1', app.http)
      socket.write("POST http://localhost:#{@target.port}/ HTTP/1.1\r\n#{headers}\r\n\r\nx")
      response = Timeout.timeout(5) { socket.readpartial(4096) }
      expect(response).to include('502 Bad Gateway'); socket.close
    end
    expect(@requests.empty?).to be(true)
    socket = TCPSocket.new('127.0.0.1', app.http)
    socket.write("CONNECT 127.0.0.1:1 HTTP/1.1\r\n\r\n")
    expect(Timeout.timeout(5) { socket.read }).to include('502 Bad Gateway')
    socket.close
    socket = TCPSocket.new('127.0.0.1', app.socks); socket.write("\5\1\2".b)
    expect(Timeout.timeout(5) { socket.read }).to eq("\5\xff".b); socket.close
    expect(curl(app, "http://localhost:#{@target.port}/")).to eq('skvoz-real-http-response')
  end
  it 'handles occupied HTTP/SOCKS listeners and cleans a crashed startup profile before the next launch' do
    [:http, :socks].each do |kind|
      listener = TCPServer.new('127.0.0.1', 0)
      options = { (kind == :http ? :http_port : :socks_port) => listener.addr[1] }
      app = UbuntuSystem::Application.new(@directory.join("busy-#{kind}"), @server, **options)
      @applications << app
      expect(app.ready).to include('failed' => 'port_busy')
      listener.close
    end
    fake = @directory.join('delayed-core')
    fake.write("#!/bin/sh\nif [ \"$1\" = --version ]; then echo 'skvoz-core-daemon 1.4.0 ipc=1'; else exec sleep 60; fi\n")
    fake.chmod(0o700)
    parent = UbuntuSystem::Application.new(@directory.join('crash'), @server, daemon: fake.to_s, wait_ready: false)
    @applications << parent
    runtime_base = ENV.fetch('XDG_RUNTIME_DIR', Dir.tmpdir)
    runtime = wait_until do
      Dir.glob(File.join(runtime_base, "skvoz-#{Process.uid}", "*", "owner.json")).find do |record|
        value = JSON.parse(File.read(record))
        value['parent'] == parent.process.pid && value['daemon'] && File.exist?(File.join(File.dirname(record), 'profile.json'))
      end
    end
    directory = File.dirname(runtime)
    owner = JSON.parse(File.read(runtime))
    profile = File.join(directory, 'profile.json')
    expect(File.stat(profile).mode & 0o777).to eq(0o600)
    Process.kill('KILL', parent.process.pid)
    wait_until { process_dead(owner.fetch('daemon')) }
    parent.close
    replacement = application('crash')
    expect(File.exist?(directory)).to be(false)
    expect(curl(replacement, "http://localhost:#{@target.port}/")).to eq('skvoz-real-http-response')
  end

end
