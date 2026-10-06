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
require 'io/nonblock'
require_relative '../../lib/skvoz/server/service'
require_relative 'runtime_client'

module ServerSystem
  extend self
  COMPONENT = Pathname.new(__dir__).join('../..').expand_path
  ROOT = COMPONENT.join('../..').expand_path
  CORE = ENV.fetch('SKVOZ_TEST_CORE', 'skvoz-core-daemon')
  RUNTIME = ENV.fetch('SKVOZ_TEST_RUNTIME', 'skvoz-network-runtime')
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

  def device_ready(client)
    response, = client.request('STATUS')
    response.dig('result', 'lifecycle') == 'ready'
  rescue IOError, SystemCallError
    false
  end

  def transfer(client, port, payload = ''.b, host: '127.0.0.1', reject: nil, timeout: 15, receive_delay: 0)
    Timeout.timeout(timeout) do
      begin
        socket = client.open(host, port)
      rescue IOError
        raise unless reject
        return nil
      end
      raise IOError, 'Expected destination rejection' if reject
      socket.write(payload)
      socket.close_write
      sleep receive_delay
      socket.read
    ensure
      socket&.close
    end
  end

  def credentials_work(server, username, password, subject: nil)
    raw = TCPSocket.new('127.0.0.1', server.port)
    tls = nil
    Timeout.timeout(5) do
      tls = Skvoz::Server::BrokerTLS.connect(raw, identity: 'localhost', ca: server.directory.join('ca.pem').to_s)
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
    def initialize(directory, overrides: {}, nats: NATS, allow: [])
      @directory = Pathname.new(directory)
      @cli = [RbConfig.ruby, COMPONENT.join('bin/skvoz-server').to_s]
      @state, @config, @log = %w[server-state server.json server.log].map { |name| @directory.join(name) }
      @port, @monitor = ServerSystem.free_port, ServerSystem.free_port
      @value = { 'state_dir' => @state.to_s, 'address' => 'localhost', 'bind' => '127.0.0.1', 'port' => @port,
                 'monitor_port' => @monitor, 'nats_binary' => nats.to_s, 'runtime_binary' => RUNTIME, 'allow' => allow, 'network' => {},
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
    def initialize(directory, bundle, server_port = nil)
      directory = Pathname.new(directory)
      directory.mkdir(0o700)
      @profile, @log = %w[profile.json runtime.log].map { |name| directory.join(name) }
      raise ArgumentError, 'Exported port mismatch' if server_port && bundle['port'] != server_port
      ca = directory.join('ca.pem')
      ca.write(bundle.fetch('ca_pem')); ca.chmod(0o600)
      limits = Skvoz::Server::NetworkConfiguration::LIMITS.merge('ip_sessions' => 1, 'core_streams' => 512,
        'lease_identities' => 1, 'core_receive_bytes' => 33554432, 'core_send_bytes' => 2097152,
        'runtime_buffer_bytes' => 100663296, 'runtime_buffer_records' => 16384)
      ServerSystem.private_json(@profile, { v: 1, role: 'client', server: nil,
        core: { url: "tls://127.0.0.1:#{bundle.fetch('port')}", tls_server_name: bundle.fetch('address'),
          trust: 'managed_ca', ca_file: ca.to_s, username: bundle.fetch('username'), password: bundle.fetch('password'),
          namespace: bundle.fetch('namespace'), peer_id: bundle.fetch('peer_id').to_s, membership: 'allowlist', allowed_peers: ['0'], initiate: ['0'] },
        network: { families: [4], max_mtu: 1400, channels: 1, limits: } })
      @output = File.open(@log, 'ab')
      start
    rescue Exception
      close
      raise
    end

    def start
      owner, child = UNIXSocket.pair
      owner.nonblock = child.nonblock = true
      @process = Process.spawn(RUNTIME, '--config', @profile.to_s, '--control-fd', '3', 3 => child, out: @output, err: @output)
      child.close
      @path = RuntimeClient.new(owner)
      hello, = @path.request('HELLO', { api: 1, network: 3 })
      raise IOError, 'Runtime HELLO rejected' if hello['error']
      ServerSystem.wait_until do
        raise IOError, "Device startup failed: #{@log.read}" if ServerSystem.process_dead(@process)
        response, = @path.request('STATUS')
        response.dig('result', 'lifecycle') == 'ready'
      end
      response, = @path.request('START_PROXY', { http_bind: nil, socks_bind: nil })
      raise IOError, 'Runtime mode admission failed' if response['error']
    end

    def restart
      stop
      start
    end

    def stop
      begin
        @path&.request('PREPARE_SHUTDOWN')
      rescue IOError, Timeout::Error
        nil
      end
      @path&.close
      ServerSystem.terminate(@process, timeout: 8) if @process
      @process = nil
    end

    def close
      stop
    ensure
      @output&.close unless @output&.closed?
    end
  end
end
