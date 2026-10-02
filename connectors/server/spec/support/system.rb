# frozen_string_literal: true
require 'fileutils'
require 'find'
require 'json'
require 'open3'
require 'openssl'
require 'pathname'
require 'rbconfig'
require 'securerandom'
require 'socket'
require 'timeout'
require_relative '../../../../clients/ruby/skvoz_ipc'

module ServerSystem
  extend self
  COMPONENT = Pathname.new(__dir__).join('../..').expand_path
  ROOT = COMPONENT.join('../..').expand_path
  CORE = ENV.fetch('SKVOZ_TEST_CORE', 'skvoz-core-daemon')
  NATS = ENV.fetch('SKVOZ_TEST_NATS', 'nats-server')
  PEBBLE = ENV.fetch('SKVOZ_TEST_PEBBLE', 'pebble')

  def free_port
    TCPServer.open('127.0.0.1', 0) { |socket| socket.addr[1] }
  end

  def monotonic = Process.clock_gettime(Process::CLOCK_MONOTONIC)

  def wait_until(timeout: 15)
    deadline = monotonic + timeout
    loop do
      value = yield
      return value if value
      raise Timeout::Error, 'System test readiness deadline exceeded' if monotonic >= deadline
      sleep 0.02
    end
  end

  def private_json(path, value)
    File.write(path, JSON.generate(value))
    File.chmod(0o600, path)
  end

  def preserve_artifacts(directory)
    return unless ENV['SKVOZ_TEST_ARTIFACTS']
    directory = Pathname.new(directory)
    destination = Pathname.new(ENV.fetch('SKVOZ_TEST_ARTIFACTS')).join(directory.basename)
    FileUtils.mkdir_p(destination, mode: 0o700)
    Find.find(directory.to_s) do |name|
      path = Pathname.new(name)
      target = destination.join(path.relative_path_from(directory))
      info = path.lstat
      if info.directory?
        FileUtils.mkdir_p(target, mode: 0o700)
      elsif info.file?
        maximum = path.extname == '.log' ? 65_536 : 1_048_576
        target.binwrite(File.binread(path, maximum, [info.size - maximum, 0].max))
        target.chmod(0o600)
      end
    rescue Errno::ENOENT
      # Certificate generations can retire during the failure snapshot.
      next
    end
    destination
  end

  def capture(*argv, input: '', timeout: 20, env: {})
    result = nil
    Open3.popen3(env, *argv.map(&:to_s)) do |stdin, stdout, stderr, process|
      begin
        Timeout.timeout(timeout) do
          stdin.write(input)
          stdin.close
          readers = [stdout, stderr].map { |io| Thread.new { io.read } }
          result = [*readers.map(&:value), process.value]
        end
      ensure
        if process.alive?
          Process.kill('KILL', process.pid) rescue Errno::ESRCH
          process.join(5)
        end
      end
    end
    result
  end

  def terminate(pid, signal: 'TERM', timeout: 12)
    Process.kill(signal, pid) rescue Errno::ESRCH
    Timeout.timeout(timeout) { Process.wait2(pid).last }
  rescue Errno::ECHILD
    nil
  rescue Timeout::Error
    Process.kill('KILL', pid) rescue Errno::ESRCH
    Timeout.timeout(5) { Process.wait2(pid).last }
  end

  def process_dead(pid)
    File.read("/proc/#{pid}/stat").split(') ', 2).last.split.first == 'Z'
  rescue Errno::ENOENT
    true
  end

  def child_pid(pid, name)
    children = File.read("/proc/#{pid}/task/#{pid}/children").split
    children.map(&:to_i).find { |child| File.read("/proc/#{child}/comm").strip == name }
  end

  def certificate(key:, issuer: nil, issuer_key: nil, name:, san: nil, eku: 'serverAuth', expires: Time.now + 86_400, ca: false)
    cert = OpenSSL::X509::Certificate.new
    cert.version = 2
    cert.serial = SecureRandom.random_number(1 << 120)
    cert.subject = OpenSSL::X509::Name.parse("/CN=#{name}")
    cert.issuer = issuer ? issuer.subject : cert.subject
    cert.public_key = key.public_key
    cert.not_before = Time.now - 60
    cert.not_after = expires
    factory = OpenSSL::X509::ExtensionFactory.new
    factory.subject_certificate = cert
    factory.issuer_certificate = issuer || cert
    cert.add_extension(factory.create_extension('basicConstraints', ca ? 'CA:TRUE' : 'CA:FALSE', true))
    cert.add_extension(factory.create_extension('keyUsage', ca ? 'keyCertSign,cRLSign' : 'digitalSignature,keyEncipherment', true))
    cert.add_extension(factory.create_extension('extendedKeyUsage', eku)) unless ca
    cert.add_extension(factory.create_extension('subjectAltName', san)) if san
    cert.sign(issuer_key || key, OpenSSL::Digest::SHA256.new)
    cert
  end

  def certificates(directory, identity: 'localhost', san: 'DNS:localhost,IP:127.0.0.1')
    directory = Pathname.new(directory)
    ca_key = OpenSSL::PKey::RSA.new(2048)
    ca = certificate(key: ca_key, name: 'SKVOZ system test CA', ca: true)
    key = OpenSSL::PKey::RSA.new(2048)
    leaf = certificate(key:, issuer: ca, issuer_key: ca_key, name: identity, san:)
    { 'ca.key' => ca_key.to_pem, 'ca.pem' => ca.to_pem, 'server.key' => key.to_pem, 'server.pem' => leaf.to_pem }.each do |name, bytes|
      File.write(directory.join(name), bytes)
      File.chmod(0o600, directory.join(name))
    end
    [ca, ca_key, key]
  end

  class Target
    attr_reader :port, :listener
    def initialize(host: '127.0.0.1', backlog: 64, &operation)
      @listener = TCPServer.new(host, 0)
      @listener.listen(backlog)
      @port = @listener.addr[1]
      @connections, @workers, @errors = [], [], ::Queue.new
      @thread = Thread.new do
        loop do
          socket = @listener.accept
          @connections << socket
          @workers << Thread.new(socket) do |connection|
            begin
              Timeout.timeout(35) { operation.call(connection) }
            rescue IOError, SystemCallError, Timeout::Error
              # Cancellation and deliberate resets close test target sockets.
            rescue StandardError => error
              @errors << error
            ensure
              connection.close rescue IOError
            end
          end
        end
      rescue IOError, Errno::EBADF
        nil
      end
    end

    def close
      @listener.close unless @listener.closed?
      @connections.each { |socket| socket.close rescue IOError }
      @thread.join(1)
      @workers.each { |worker| worker.join(1) || worker.kill }
      raise @errors.pop unless @errors.empty?
    end
  end

  def after_fin(socket)
    request = socket.read(131_073) || ''.b
    raise IOError, 'Target request exceeds budget' if request.bytesize > 131_072
    socket.write('after-fin:'.b + request.reverse)
  end

  def device_ready(path)
    return false unless File.socket?(path)
    client = SkvozIPC::Client.new(path.to_s)
    client.request(11, 0, [0].pack('Q>'))[3] == "\1".b
  rescue IOError, SystemCallError
    false
  ensure
    client&.close
  end

  def open_client(path, port, host: '127.0.0.1')
    client = SkvozIPC::Client.new(path.to_s)
    metadata = JSON.generate(v: 1, type: 'tcp', host:, port:)
    code, _, handle = client.request(2, 0, [0].pack('Q>') + metadata)
    raise IOError, "OPEN admission failed #{code}" unless code.zero?
    Timeout.timeout(10) do
      loop do
        kind, _, event_handle, payload = client.event
        raise IOError, 'Unexpected stream handle' unless event_handle == handle
        return [client, handle] if kind == SkvozIPC::OPENED && JSON.parse(payload)['status'] == 'connected'
        raise IOError, "OPEN failed #{kind}" if [SkvozIPC::CLOSED, SkvozIPC::REJECTED].include?(kind)
      end
    end
  rescue Exception
    client&.close
    raise
  end

  def closed(client, handle)
    Timeout.timeout(12) do
      loop do
        kind, _, event_handle, payload = client.event
        raise IOError, 'Unexpected stream handle' unless event_handle == handle
        return payload if kind == SkvozIPC::CLOSED
        raise IOError, 'Unexpected stream rejection' if kind == SkvozIPC::REJECTED
      end
    end
  end

  def transfer(path, port, payload = ''.b, host: '127.0.0.1', reject: nil, metadata: nil, timeout: 15, receive_delay: 0)
    client = SkvozIPC::Client.new(path.to_s)
    metadata ||= JSON.generate(v: 1, type: 'tcp', host:, port:)
    code, _, handle = client.request(2, 0, [0].pack('Q>') + metadata)
    raise IOError, "OPEN admission failed #{code}" unless code.zero?
    pending, received = payload.b, ''.b
    accepted = finished = false
    Timeout.timeout(timeout) do
      loop do
        if accepted && !pending.empty?
          code, count = client.request(5, handle, pending.byteslice(0, 1024))
          raise IOError, "SEND failed #{code}" unless [0, 1].include?(code)
          pending = pending.byteslice(count..)
        elsif accepted && !finished
          raise IOError, 'FINISH failed' unless client.request(7, handle)[0].zero?
          finished = true
          sleep receive_delay
        end
        next unless !client.events.empty? || IO.select([client.socket], nil, nil, 0.005)
        kind, _, event_handle, bytes = client.event
        raise IOError, 'Unexpected stream handle' unless event_handle == handle
        case kind
        when SkvozIPC::OPENED
          raise IOError, 'False connected response' unless JSON.parse(bytes)['status'] == 'connected'
          accepted = true
        when SkvozIPC::REJECTED
          error = JSON.parse(bytes)['error']
          raise IOError, "Unexpected REJECT #{error}, expected #{reject}" unless error == reject
          return nil
        when SkvozIPC::DATA
          raise IOError, 'DATA offset mismatch' unless bytes.unpack1('Q>') == received.bytesize
          received << bytes.byteslice(8..)
          client.consume(handle, received.bytesize)
        when SkvozIPC::REMOTE_FINISHED, SkvozIPC::CLOSED
          raise IOError, "Early terminal #{kind}: #{bytes.unpack1('H*')}" unless !reject && accepted && pending.empty?
          return received
        end
      end
    end
  ensure
    client&.close
  end

  def credentials_work(server, username, password, subject: nil)
    context = OpenSSL::SSL::SSLContext.new
    context.ca_file = server.directory.join('ca.pem').to_s
    context.verify_mode = OpenSSL::SSL::VERIFY_PEER
    raw = TCPSocket.new('127.0.0.1', server.port)
    tls = OpenSSL::SSL::SSLSocket.new(raw, context)
    tls.sync_close = true
    tls.hostname = 'localhost'
    Timeout.timeout(5) do
      tls.connect
      tls.post_connection_check('localhost')
      request = "CONNECT #{JSON.generate(user: username, pass: password, verbose: false)}\r\n"
      request << "SUB #{subject} 1\r\n" if subject
      tls.write(request + "PING\r\n")
      loop do
        line = tls.gets("\n", 4096)
        if line&.start_with?('-ERR') && subject && !line.include?('Permissions Violation')
          raise IOError, 'Expected native NATS subscription permission rejection'
        end
        return false if !line || line.start_with?('-ERR')
        return true if line == "PONG\r\n"
      end
    end
  rescue OpenSSL::SSL::SSLError, EOFError, Errno::ECONNRESET
    false
  ensure
    tls&.close
    raw&.close unless raw&.closed?
  end

  class Server
    attr_reader :directory, :state, :config, :value, :port, :monitor, :process, :log, :cli
    def initialize(directory, overrides: {}, core: CORE, nats: NATS, allow: [])
      @directory = Pathname.new(directory)
      @cli = [RbConfig.ruby, COMPONENT.join('bin/skvoz-server').to_s]
      @state, @config, @log = %w[server-state server.json server.log].map { |name| @directory.join(name) }
      @port, @monitor = ServerSystem.free_port, ServerSystem.free_port
      @value = { 'state_dir' => @state.to_s, 'address' => 'localhost', 'bind' => '127.0.0.1', 'port' => @port,
                 'monitor_port' => @monitor, 'nats_binary' => nats.to_s, 'core_binary' => core.to_s, 'allow' => allow,
                 'tls' => { 'mode' => 'provided', 'certificate' => @directory.join('server.pem').to_s,
                            'key' => @directory.join('server.key').to_s, 'ca' => @directory.join('ca.pem').to_s } }.merge(overrides)
      ServerSystem.private_json(@config, @value)
      @output = File.open(@log, 'ab')
      start
    rescue Exception
      stop(kill: true)
      @output&.close unless @output&.closed?
      raise
    end

    def start
      @process = Process.spawn(*@cli, 'serve', '--config', @config.to_s, out: @output, err: @output)
      ServerSystem.wait_until(timeout: 25) do
        raise IOError, "Server startup failed: #{@log.read[-2000, 2000] || @log.read}" if ServerSystem.process_dead(@process)
        stdout, _, status = ServerSystem.capture(*@cli, 'health', '--state', @state, timeout: 5)
        status.success? && JSON.parse(stdout)['healthy']
      end
    end

    def command(operation, login = nil, password = nil)
      argv = @cli + (operation == 'health' ? ['health'] : ['user', operation])
      argv << login if login
      argv += ['--state', @state.to_s]
      argv << '--password-stdin' if password
      stdout, stderr, status = ServerSystem.capture(*argv, input: password ? password + "\n" : '')
      unless status.success? || (operation == 'health' && stdout.lstrip.start_with?('{'))
        raise IOError, "Administration failed: #{stderr} #{@log.read[-2000, 2000] || @log.read}"
      end
      JSON.parse(stdout)
    end

    def stop(kill: false)
      return unless @process
      status = ServerSystem.terminate(@process, signal: kill ? 'KILL' : 'TERM')
      @process = nil
      raise IOError, "Server stop failed #{status}" if !kill && status && !status.success?
    end

    def close
      stop
    ensure
      @output&.close unless @output&.closed?
    end
  end

  class Device
    attr_reader :path, :profile, :process, :log
    def initialize(directory, bundle, core = CORE, server_port = nil)
      directory = Pathname.new(directory)
      directory.mkdir(0o700)
      @path, @profile, @log = %w[core.sock profile.json daemon.log].map { |name| directory.join(name) }
      @core = core.to_s
      if server_port && bundle['port'] != server_port
        raise ArgumentError, 'Exported port differs from published server port'
      end
      ca = directory.join('ca.pem')
      ca.write(bundle.fetch('ca_pem')); ca.chmod(0o600)
      ServerSystem.private_json(@profile, { 'ipc_path' => @path.to_s, 'url' => "tls://127.0.0.1:#{bundle.fetch('port')}",
        'tls_server_name' => bundle.fetch('address'), 'trust' => 'managed_ca', 'ca_file' => ca.to_s,
        'username' => bundle.fetch('username'), 'password' => bundle.fetch('password'), 'namespace' => bundle.fetch('namespace'),
        'peer_id' => bundle.fetch('peer_id'), 'allowed_peers' => [0], 'initiate' => [0],
        'limits' => { 'owners' => 64, 'output_frames' => 2048, 'output_bytes' => 1_048_576, 'subscription_frames' => 256 } })
      @output = File.open(@log, 'ab')
      start
    rescue Exception
      close
      raise
    end

    def start
      @process = Process.spawn(@core, '--config', @profile.to_s, out: @output, err: @output)
      ServerSystem.wait_until do
        raise IOError, "Device startup failed: #{@log.read}" if ServerSystem.process_dead(@process)
        ServerSystem.device_ready(@path)
      end
    end

    def restart
      ServerSystem.terminate(@process, timeout: 8) if @process
      @process = nil
      start
    end

    def close
      ServerSystem.terminate(@process, timeout: 8) if @process
      @process = nil
    ensure
      @output&.close unless @output&.closed?
    end
  end
end
