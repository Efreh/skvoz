# frozen_string_literal: true
require_relative 'spec_helper'
require_relative 'support/system'

RSpec.describe 'Server TCP transport and administration', integration: true do
  include ServerSystem

  before do
    @directory = Pathname.new(Dir.mktmpdir('skvoz-server-spec-'))
    @directory.chmod(0o700)
    certificates(@directory)
    @targets, @devices, @clients = [], [], []
  end

  after do
    if RSpec.current_example.exception && ENV['SKVOZ_TEST_ARTIFACTS']
      destination = Pathname.new(ENV.fetch('SKVOZ_TEST_ARTIFACTS')).join(@directory.basename)
      FileUtils.mkdir_p(destination.parent, mode: 0o700)
      destination.mkdir(0o700)
      @directory.children.each { |path| FileUtils.cp(path, destination) if path.file? }
    end
    errors = []
    (@clients + @devices.reverse + [@server].compact + @targets.reverse).each do |resource|
      resource.close
    rescue StandardError => error
      errors << error
    end
    FileUtils.remove_entry(@directory)
    raise errors.first unless errors.empty?
  end

  def target(&block)
    @targets << ServerSystem::Target.new(&block)
    @targets.last
  end

  def start_bridge(*ports, **overrides)
    @server = ServerSystem::Server.new(@directory, allow: [{ 'cidr' => '127.0.0.0/8', 'ports' => ports }], overrides: overrides.transform_keys(&:to_s))
    @bundle = @server.command('add', 'shared')
    @devices << ServerSystem::Device.new(@directory.join('first'), @bundle, ServerSystem::CORE, @server.port)
    @devices.last
  end

  def restart(**changes)
    @server.stop
    @server.value.merge!(changes.transform_keys(&:to_s))
    private_json(@server.config, @server.value)
    @server.start
    @devices.each { |device| wait_until { device_ready(device.path) } }
  end

  def remember_client(path, port)
    client, handle = open_client(path, port)
    @clients << client
    [client, handle]
  end

  it 'denies private literal, DNS and mapped addresses by default and rejects malformed destinations safely' do
    echo = target { |socket| after_fin(socket) }
    @server = ServerSystem::Server.new(@directory)
    bundle = @server.command('add', 'shared')
    @devices << ServerSystem::Device.new(@directory.join('first'), bundle, ServerSystem::CORE, @server.port)
    %w[127.0.0.1 localhost ::ffff:127.0.0.1].each do |host|
      expect(transfer(@devices.first.path, echo.port, host:, reject: 'forbidden')).to be_nil
    end
    expect(transfer(@devices.first.path, echo.port, metadata: 'not-json', reject: 'invalid_destination')).to be_nil
    marker = 'do-not-log-' + SecureRandom.hex(16)
    UNIXSocket.open(@server.state.join('admin.sock')) do |socket|
      socket.write(JSON.generate(marker) + "\n")
      expect(JSON.parse(socket.gets)['ok']).to be(false)
    end
    expect(@server.log.read).not_to include(marker)
    restart(allow: [{ 'cidr' => '127.0.0.0/8', 'ports' => [echo.port, @server.port, @server.monitor] }])
    expect(transfer(@devices.first.path, echo.port)).to eq('after-fin:')
    [@server.port, @server.monitor].each do |port|
      expect(transfer(@devices.first.path, port, reject: 'forbidden')).to be_nil
    end
  end

  it 'preserves binary and empty streams, shared-login device isolation and durable identity through restart' do
    echo = target { |socket| after_fin(socket) }
    first = start_bridge(echo.port)
    payload = (0..255).to_a.pack('C*') * 256
    expect(transfer(first.path, echo.port, payload)).to eq('after-fin:'.b + payload.reverse)
    expect(transfer(first.path, echo.port)).to eq('after-fin:')
    second_bundle = @server.command('device-add', 'shared', @bundle.fetch('password'))
    expect(second_bundle.fetch('peer_id')).not_to eq(@bundle.fetch('peer_id'))
    @devices << ServerSystem::Device.new(@directory.join('second'), second_bundle, ServerSystem::CORE, @server.port)
    threads = 8.times.map do |index|
      Thread.new do
        bytes = index.chr.b * 16_384
        expect(transfer(@devices[index % 2].path, echo.port, bytes)).to eq('after-fin:'.b + bytes)
      end
    end
    threads.each { |thread| thread.join(20) || thread.kill }
    threads.each(&:value)
    users = @server.command('list')
    expect(users).to contain_exactly(include('devices' => 2))
    expect(@server.log.read).not_to include(@bundle.fetch('password'))
    restart
    expect(@server.command('list')).to eq(users)
    expect(transfer(first.path, echo.port, 'recreate')).to eq('after-fin:etaercer')
  end

  it 'holds 64 simultaneous streams and rejects the 65th without corrupting mixed replies' do
    admitted, release = ::Queue.new, ::Queue.new
    sustained = target do |socket|
      bytes = socket.read(16_385) || ''.b
      raise IOError, 'ServerSystem::Target request exceeds budget' if bytes.bytesize > 16_384
      admitted << true
      release.pop
      socket.write('mixed:'.b + bytes)
    end
    device = start_bridge(sustained.port)
    second_bundle = @server.command('device-add', 'shared', @bundle.fetch('password'))
    @devices << ServerSystem::Device.new(@directory.join('second'), second_bundle, ServerSystem::CORE, @server.port)
    overflow_device = @devices.last
    threads = 64.times.map do |index|
      Thread.new do
        bytes = index.chr.b * (index % 3 == 0 ? 0 : 8192)
        expect(transfer(device.path, sustained.port, bytes, timeout: 35)).to eq('mixed:'.b + bytes)
      end
    end
    begin
      Timeout.timeout(20) { 64.times { admitted.pop } }
      expect(@server.command('health').fetch('connector').fetch('streams')).to eq(64)
      expect(transfer(overflow_device.path, sustained.port, reject: 'overloaded')).to be_nil
    ensure
      64.times { release << true }
      threads.each { |thread| thread.join(40) || thread.kill }
      threads.each(&:value)
    end
    wait_until { @server.command('health').fetch('connector').fetch('streams').zero? }
  end

  it 'keeps HTTP bytes opaque and permits a long response while another device makes progress' do
    entered = ::Queue.new
    expected = "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n5\r\nworld\r\n0\r\n\r\n".b
    response = lambda do |socket, delay|
      request = socket.read
      raise IOError, 'HTTP request changed' unless request == "GET /stream HTTP/1.1\r\nHost: service.invalid\r\n\r\n"
      entered << true if delay > 0
      sleep delay
      socket.write(expected.byteslice(0, 52))
      sleep 0.25
      socket.write(expected.byteslice(52..))
    end
    slow = target { |socket| response.call(socket, 6) }
    fast = target { |socket| response.call(socket, 0) }
    first = start_bridge(slow.port, fast.port)
    second_bundle = @server.command('device-add', 'shared', @bundle.fetch('password'))
    @devices << ServerSystem::Device.new(@directory.join('second'), second_bundle, ServerSystem::CORE, @server.port)
    request = "GET /stream HTTP/1.1\r\nHost: service.invalid\r\n\r\n"
    idle = Thread.new { transfer(first.path, slow.port, request, timeout: 12) }
    Timeout.timeout(3) { entered.pop }
    started = monotonic
    expect(transfer(@devices.last.path, fast.port, request)).to eq(expected)
    expect(monotonic - started).to be < 4
    expect(idle.value).to eq(expected)
  ensure
    idle&.join(1) || idle&.kill
  end

  it 'returns credit only for real TCP writes and keeps another flow moving during a window stall' do
    entered, release = ::Queue.new, ::Queue.new
    stalled = target do |socket|
      socket.setsockopt(Socket::SOL_SOCKET, Socket::SO_RCVBUF, 1024)
      entered << true
      release.pop
      socket.read
    end
    echo = target { |socket| after_fin(socket) }
    device = start_bridge(stalled.port, echo.port, receive_window: 8192, stream_queue_frames: 32, stream_queue_bytes: 16_384, tcp_buffer_bytes: 16_384)
    client, handle = remember_client(device.path, stalled.port)
    Timeout.timeout(3) { entered.pop }
    sent = 0
    deadline = monotonic + 4
    while monotonic < deadline
      code, count = client.request(5, handle, 'x' * 1024)
      expect([0, 1]).to include(code)
      sent += count
      sleep 0.005
    end
    before = @server.command('health').fetch('connector')
    sleep 1
    after = @server.command('health').fetch('connector')
    expect(sent).to be > 8192
    expect(before.fetch('target_written_bytes')).to eq(after.fetch('target_written_bytes'))
    expect(sent - after.fetch('target_written_bytes')).to eq(8192)
    expect(after.fetch('event_bytes')).to be <= 16_384
    expect(after.fetch('event_frames')).to be <= 32
    expect(transfer(device.path, echo.port, 'healthy')).to eq('after-fin:yhtlaeh')
    expect(client.request(8, handle)[0]).to eq(0)
    release << true
    wait_until { @server.command('health').fetch('connector').fetch('streams').zero? }
    expect(@server.command('health').fetch('connector')).to include('event_bytes' => 0, 'event_frames' => 0)
    expect(@server.command('health').fetch('connector').fetch('outcomes')).to include('cancelled' => 1, 'failed' => 0)
    expect(@server.log.read).not_to include('SKVOZ stream failed: IOError', '"event":"stream_failed"')
  ensure
    release << true if release
  end

  it 'isolates a pressured stream with a deliberate one-frame queue and releases its reservations' do
    release = ::Queue.new
    stalled = target { |_socket| release.pop }
    stalled.listener.setsockopt(Socket::SOL_SOCKET, Socket::SO_RCVBUF, 1024)
    echo = target { |socket| after_fin(socket) }
    device = start_bridge(stalled.port, echo.port, stream_queue_frames: 1)
    client, handle = remember_client(device.path, stalled.port)
    seen = false
    Timeout.timeout(12) do
      until seen
        code = client.request(5, handle, 'q' * 8192)[0]
        expect([0, 1, 4]).to include(code)
        until client.events.empty?
          seen ||= client.event[0] == SkvozIPC::CLOSED
        end
        seen = client.event[0] == SkvozIPC::CLOSED if code == 4 && !seen
        sleep 0.005
      end
    end
    release << true
    wait_until { @server.command('health').fetch('connector').fetch('streams').zero? }
    expect(@server.command('health').fetch('connector')).to include('event_bytes' => 0, 'event_frames' => 0, 'peak_event_frames' => 1)
    expect(transfer(device.path, echo.port)).to eq('after-fin:')
  ensure
    release << true if release
  end

  it 'reports refused and bounded blocked connects and cancels without late acceptance' do
    refused = Socket.new(Socket::AF_INET, Socket::SOCK_STREAM)
    refused.bind(Socket.sockaddr_in(0, '127.0.0.1'))
    blackhole = Socket.new(Socket::AF_INET, Socket::SOCK_STREAM)
    blackhole.bind(Socket.sockaddr_in(0, '127.0.0.1'))
    blackhole.listen(0)
    blocked_port = blackhole.local_address.ip_port
    blocker = TCPSocket.new('127.0.0.1', blocked_port)
    device = start_bridge(refused.local_address.ip_port, blocked_port)
    expect(transfer(device.path, refused.local_address.ip_port, reject: 'refused')).to be_nil
    failure = @server.command('health').fetch('connector').fetch('last_failure')
    expect(failure).to include('outcome' => 'rejected', 'stage' => 'connect', 'error_code' => 'refused', 'errno' => Errno::ECONNREFUSED::Errno)
    if File.read('/proc/sys/net/ipv4/tcp_abort_on_overflow').strip == '0'
      expect(transfer(device.path, blocked_port, reject: 'connect_timeout', timeout: 8)).to be_nil
    end
    client = SkvozIPC::Client.new(device.path.to_s)
    @clients << client
    code, _, handle = client.request(2, 0, [0].pack('Q>') + JSON.generate(v: 1, type: 'tcp', host: '127.0.0.1', port: blocked_port))
    expect(code).to eq(0)
    sleep 0.05
    expect(client.request(8, handle)[0]).to eq(0)
    closed(client, handle)
    wait_until { @server.command('health').fetch('connector').fetch('streams').zero? }
    sleep 3.2
    expect(@server.command('health').fetch('connector').fetch('streams')).to eq(0)
  ensure
    [blocker, blackhole, refused].compact.each(&:close)
  end

  it 'preserves both half-closes when target EOF arrives before a later requester body' do
    received = ::Queue.new
    early = target do |socket|
      socket.write("early-response\0\xff".b)
      socket.shutdown(Socket::SHUT_WR)
      received << socket.read
    end
    device = start_bridge(early.port)
    client, handle = remember_client(device.path, early.port)
    reply = ''.b
    Timeout.timeout(10) do
      loop do
        kind, _, event_handle, bytes = client.event
        expect(event_handle).to eq(handle)
        break if kind == SkvozIPC::REMOTE_FINISHED
        expect(kind).to eq(SkvozIPC::DATA)
        expect(bytes.unpack1('Q>')).to eq(reply.bytesize)
        reply << bytes.byteslice(8..)
        client.consume(handle, reply.bytesize)
      end
    end
    expect(reply).to eq("early-response\0\xff".b)
    body = (0..255).to_a.pack('C*') * 128
    pending = body
    Timeout.timeout(10) do
      until pending.empty?
        code, count = client.request(5, handle, pending.byteslice(0, 1024))
        expect([0, 1]).to include(code)
        pending = pending.byteslice(count..)
        sleep 0.005 if count.zero?
      end
    end
    expect(client.request(7, handle)[0]).to eq(0)
    expect(Timeout.timeout(5) { received.pop }).to eq(body)
    expect(closed(client, handle).unpack1('n')).to eq(1)
    wait_until { @server.command('health').fetch('connector').fetch('streams').zero? }
  end

  it 'cleans up an established target reset and preserves the next same-peer transfer' do
    reset = ::Queue.new
    broken = target do |socket|
      reset.pop
      socket.setsockopt(Socket::SOL_SOCKET, Socket::SO_LINGER, [1, 0].pack('ii'))
    end
    echo = target { |socket| after_fin(socket) }
    device = start_bridge(broken.port, echo.port)
    client, handle = remember_client(device.path, broken.port)
    reset << true
    expect(closed(client, handle).unpack1('n')).not_to eq(1)
    wait_until { @server.command('health').fetch('connector').fetch('streams').zero? }
    expect(@server.command('health').fetch('connector')).to include('event_bytes' => 0, 'event_frames' => 0)
    expect(transfer(device.path, echo.port, 'after-rst')).to eq('after-fin:tsr-retfa')
    failure = @server.command('health').fetch('connector').fetch('last_failure')
    expect(failure).to include('outcome' => 'failed', 'stage' => 'target_read', 'error_class' => 'Errno::ECONNRESET', 'errno' => Errno::ECONNRESET::Errno)
    expect(failure.fetch('handle')).to match(/\A[0-9a-f]{32}\z/)
    expect(failure.fetch('peer')).to be > 0
    expect(@server.log.read).not_to include(@bundle.fetch('password'))
  ensure
    reset << true if reset
  end

  it 'stops cleanly with an unwritable target and an active connect, then restarts durable identity' do
    release = ::Queue.new
    stalled = target do |socket|
      socket.setsockopt(Socket::SOL_SOCKET, Socket::SO_RCVBUF, 1024)
      release.pop
    end
    echo = target { |socket| after_fin(socket) }
    blackhole = Socket.new(Socket::AF_INET, Socket::SOCK_STREAM)
    blackhole.bind(Socket.sockaddr_in(0, '127.0.0.1')); blackhole.listen(0)
    blocked_port = blackhole.local_address.ip_port
    blocker = TCPSocket.new('127.0.0.1', blocked_port)
    device = start_bridge(stalled.port, echo.port, blocked_port)
    client, handle = remember_client(device.path, stalled.port)
    64.times { expect([0, 1]).to include(client.request(5, handle, 'x' * 1024)[0]) }
    connecting = SkvozIPC::Client.new(device.path.to_s); @clients << connecting
    metadata = JSON.generate(v: 1, type: 'tcp', host: '127.0.0.1', port: blocked_port)
    expect(connecting.request(2, 0, [0].pack('Q>') + metadata)[0]).to eq(0)
    wait_until(timeout: 1.5) { @server.command('health').fetch('connector').fetch('streams') == 2 }
    children = File.read("/proc/#{@server.process}/task/#{@server.process}/children").split.map(&:to_i)
    expect(children.length).to be >= 2
    @server.stop
    wait_until(timeout: 3) { children.all? { |pid| process_dead(pid) } }
    release << true
    @server.start
    wait_until { device_ready(device.path) }
    expect(transfer(device.path, echo.port, 'restart')).to eq('after-fin:tratser')
  ensure
    release << true if release
    [blocker, blackhole].compact.each(&:close)
  end

  it 'denies a live IPv6 loopback target by default and permits an explicit IPv6 port bridge' do
    ipv6 = ServerSystem::Target.new(host: '::1') do |socket|
      bytes = socket.read
      socket.write('ipv6:'.b + bytes.reverse)
    end
    @targets << ipv6
    @server = ServerSystem::Server.new(@directory)
    bundle = @server.command('add', 'ipv6')
    @devices << ServerSystem::Device.new(@directory.join('first'), bundle, ServerSystem::CORE, @server.port)
    device = @devices.last
    expect(transfer(device.path, ipv6.port, host: '::1', reject: 'forbidden')).to be_nil
    restart(allow: [{ 'cidr' => '::1/128', 'ports' => [ipv6.port] }])
    expect(transfer(device.path, ipv6.port, "IPv6\0\xff".b, host: '::1')).to eq("ipv6:\xff\x006vPI".b)
  end

  it 'applies native passwords and revokes every shared device while an unrelated established reply survives' do
    echo = target { |socket| after_fin(socket) }
    held, release = ::Queue.new, ::Queue.new
    delayed = target do |socket|
      bytes = socket.read
      held << true
      release.pop
      socket.write('continuous:' + bytes)
    end
    stalled = target { |socket| socket.read }
    first = start_bridge(echo.port, delayed.port, stalled.port)
    second_bundle = @server.command('device-add', 'shared', @bundle.fetch('password'))
    @devices << ServerSystem::Device.new(@directory.join('second'), second_bundle, ServerSystem::CORE, @server.port)
    second = @devices.last
    expect(credentials_work(@server, '', '')).to be(false)
    supplied = 'explicit native system test password'
    expect(@server.command('add', 'specified', supplied).fetch('password')).to eq(supplied)
    expect(credentials_work(@server, 'specified', supplied)).to be(true)
    expect(credentials_work(@server, 'specified', 'intentionally incorrect password')).to be(false)
    expect(JSON.generate(@server.command('list')) + @server.log.read).not_to include(supplied)
    @server.command('remove', 'specified')
    other_bundle = @server.command('add', 'unrelated')
    @devices << ServerSystem::Device.new(@directory.join('unrelated'), other_bundle, ServerSystem::CORE, @server.port)
    unrelated = @devices.last
    profile = JSON.parse(first.profile.read).merge('ipc_path' => @directory.join('forbidden.sock').to_s, 'peer_id' => other_bundle.fetch('peer_id'))
    claim = @directory.join('claim.json'); private_json(claim, profile)
    _, stderr, status = capture(ServerSystem::CORE, '--config', claim, timeout: 12)
    expect([3, 4]).to include(status.exitstatus)
    expect(stderr).not_to include(@bundle.fetch('password'))
    subject = "#{@bundle.fetch('namespace')}.join.#{other_bundle.fetch('peer_id')}.*"
    expect(credentials_work(@server, 'shared', @bundle.fetch('password'), subject:)).to be(false)
    streams = [first, second].map { |device| remember_client(device.path, stalled.port) }
    continuous = Thread.new { transfer(unrelated.path, delayed.port, 'kept', timeout: 30) }
    Timeout.timeout(5) { held.pop }
    replacement = @server.command('reset-password', 'shared')
    expect(credentials_work(@server, 'shared', @bundle.fetch('password'))).to be(false)
    expect(credentials_work(@server, 'shared', replacement.fetch('password'))).to be(true)
    streams.each { |client, handle| closed(client, handle) }
    @devices << ServerSystem::Device.new(@directory.join('replacement'), @bundle.merge('password' => replacement.fetch('password')), ServerSystem::CORE, @server.port)
    expect(transfer(@devices.last.path, echo.port, 'new')).to eq('after-fin:wen')
    client, handle = remember_client(@devices.last.path, stalled.port)
    @server.command('remove', 'shared')
    closed(client, handle)
    expect(credentials_work(@server, 'shared', replacement.fetch('password'))).to be(false)
    release << true
    expect(continuous.value).to eq('continuous:kept')
    expect(transfer(unrelated.path, echo.port, 'unrelated')).to eq('after-fin:detalernu')
  ensure
    release << true if release
    continuous&.join(1) || continuous&.kill
  end

  it 'fails uncertain broker apply closed and recovers only durable committed users after a prepared-state crash' do
    echo = target { |socket| after_fin(socket) }
    device = start_bridge(echo.port)
    users = @server.command('list')
    pid = child_pid(@server.process, 'nats-server')
    Process.kill('STOP', pid)
    begin
      expect { @server.command('add', 'uncertain') }.to raise_error(IOError, /Administration failed/)
      expect(@server.command('health')).to include('healthy' => false, 'failure' => 'configuration_apply_uncertain')
      expect(@server.state.join('candidate.json')).to exist
      expect(JSON.parse(@server.state.join('state.json').read).fetch('users')).not_to have_key('uncertain')
    ensure
      Process.kill('CONT', pid) rescue Errno::ESRCH
    end
    wait_until(timeout: 25) { @server.command('health').fetch('healthy') }
    expect(@server.command('list')).to eq(users)
    expect(@server.state.join('candidate.json')).not_to exist
    mutation = Thread.new { @server.command('add', 'crash-candidate') }
    mutation.report_on_exception = false
    wait_until { @server.state.join('candidate.json').exist? }
    @server.stop(kill: true)
    expect { mutation.value }.to raise_error(IOError)
    @server.start
    wait_until { device_ready(device.path) }
    expect(@server.command('list')).to eq(users)
    expect(transfer(device.path, echo.port, 'recovered')).to eq('after-fin:derevocer')
  end

  it 'contains host SIGKILL and replaces lost NATS and Core with new streams only' do
    echo = target { |socket| after_fin(socket) }
    stalled = target { |socket| socket.read }
    device = start_bridge(echo.port, stalled.port)
    old_children = [child_pid(@server.process, 'nats-server'), JSON.parse(@server.state.join('core-owner.json').read).fetch('pid')]
    @server.stop(kill: true)
    old_children.each { |pid| wait_until { process_dead(pid) } }
    @server.start
    wait_until { device_ready(device.path) }
    expect(transfer(device.path, echo.port, 'kill')).to eq('after-fin:llik')
    %w[core nats].each do |name|
      client, handle = remember_client(device.path, stalled.port)
      pid = name == 'core' ? JSON.parse(@server.state.join('core-owner.json').read).fetch('pid') : child_pid(@server.process, 'nats-server')
      Process.kill('KILL', pid)
      closed(client, handle)
      wait_until(timeout: 25) { @server.command('health').fetch('healthy') }
      wait_until { device_ready(device.path) }
      expect(transfer(device.path, echo.port, name)).to eq('after-fin:' + name.reverse)
    end
  end
end
