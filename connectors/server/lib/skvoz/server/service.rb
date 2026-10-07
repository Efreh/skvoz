# frozen_string_literal: true
require 'async'
require 'open3'
require 'uri'
require 'net/http'
require 'io/nonblock'
require_relative 'tls'
require_relative 'state'
require_relative 'runtime_control'
require_relative 'enrollment'

module Skvoz
  module Server
    class Service
      attr_reader :state, :failure

      def initialize(config)
        @config = config
        if ENV['SKVOZ_HELPER_FD']
          raise Error, 'Invalid inherited helper' unless ENV['SKVOZ_HELPER_FD'] == '3' && ENV['SKVOZ_HELPER_PID']&.match?(/\A[0-9]+\z/)
          @helper_pid = Integer(ENV.delete('SKVOZ_HELPER_PID'))
          @helper_owner = UNIXSocket.for_fd(Integer(ENV.delete('SKVOZ_HELPER_FD')))
          credentials = @helper_owner.getsockopt(Socket::SOL_SOCKET, Socket::SO_PEERCRED).to_s.unpack('iii')
          raise Error, 'Untrusted inherited helper channel' unless credentials[0] == Process.pid && credentials[1].zero? && credentials[2].zero?
          @helper_owner.close_on_exec = true
        end
        @state = State.new(config)
        @stopping = false
        @healthy = false
        @children = []
        @socket_identity = {}
        @admin_clients = []
        @mutation = Async::Semaphore.new(1)
      end

      def run(task)
        @task = task
        @admin_path = File.join(@state.directory, 'admin.sock')
        recover_socket(@admin_path, 'admin-owner.json')
        @listener = UNIXServer.new(@admin_path)
        File.chmod(0o600, @admin_path)
        remember_socket(@admin_path, 'admin-owner.json', Process.pid)
        @admin_task = task.async { administration }
        prune_tls(preserve_candidate: true)
        bootstrap_tls
        start_runtime
        while !@stopping && children_alive?
          @connector.refresh
          raise Error, 'Runtime control lost' if @connector.failure
          unless @enrollment&.ready?
            @enrollment&.stop(graceful: true)
            start_enrollment
          end
          @healthy = @tls.valid? && @failure.nil? && @connector.ready? && @enrollment&.ready?
          raise Error, 'TLS certificate expired' unless @tls.valid?
          renew if @config.tls['mode'] == 'acme' && @tls.renewal_due? && Time.now >= (@next_renewal || Time.at(0))
          task.sleep(0.25)
        end
        raise Error, 'Supervised process exited; container restart required' unless @stopping
      rescue StandardError => error
        @failure ||= 'runtime_fatal'
        Diagnostics.emit('runtime_fatal', stage: @runtime_stage, failure: @failure, **Diagnostics.error_fields(error))
        raise
      ensure
        shutdown
      end

      def stop
        @stopping = true
        @healthy = false
      end

      def healthy?
        @healthy && !@failure && !@stopping && @tls&.valid? && children_alive? && @connector&.ready? && @enrollment&.ready?
      end

      private

      def bootstrap_tls
        candidate = @state.candidate
        if @config.tls['mode'] == 'provided'
          input = @config.tls
          generation = File.join(@state.directory, 'tls-' + SecureRandom.hex(8))
          PrivateFiles.create_directory(generation)
          material = {}
          { 'certificate' => 1_048_576, 'key' => 16_384, 'ca' => 65_536 }.each do |kind, cap|
            next unless input[kind]
            path = File.join(generation, kind + '.pem')
            PrivateFiles.write(path, PrivateFiles.read(input[kind], maximum: cap))
            material[kind] = path
          end
          candidate['tls'] = material
          @tls = material_from(material)
          @state.commit(candidate)
          prune_tls
        elsif candidate['tls']
          leaf = OpenSSL::X509::Certificate.new(PrivateFiles.read(candidate['tls'].fetch('certificate'), maximum: 1_048_576))
          leaf.not_after <= Time.now ? renew(initial: true) : @tls = material_from(candidate['tls'])
        else
          renew(initial: true)
        end
      rescue StandardError
        prune_tls(preserve_candidate: true)
        raise
      end

      def material_from(value)
        TLSMaterial.new(certificate: value.fetch('certificate'), key: value.fetch('key'), ca: value['ca'], identity: @config['address'])
      end

      def issue_material
        config_path = File.join(@state.directory, 'issuer.json')
        result_path = File.join(@state.directory, 'issuer-result.json')
        PrivateFiles.write(config_path, JSON.generate(@config.tls))
        File.unlink(result_path) if File.exist?(result_path)
        worker = File.expand_path('../../../bin/skvoz-tls-worker', __dir__)
        child = ChildProcess.new([RbConfig.ruby, worker, config_path, @state.directory, result_path], label: 'issuer').start(@task)
        @children << child
        deadline = monotonic + @config.tls['order_timeout'] + 30
        @task.sleep(0.05) while child.alive? && monotonic < deadline && !@stopping
        raise Error, 'Certificate worker unavailable' if @stopping || child.alive? || !File.exist?(result_path)
        result = JSON.parse(PrivateFiles.read(result_path))
        raise Error, 'Certificate issuance failed' unless result['ok'] == true
        result.fetch('material')
      ensure
        child&.stop(monotonic + 2)
        @children.delete(child)
        File.unlink(result_path) if result_path && File.exist?(result_path)
      end

      def renew(initial: false)
        @next_renewal = Time.now + 300
        material = issue_material
        replacement = material_from(material)
        @mutation.acquire do
          candidate = @state.candidate
          candidate['tls'] = material
          candidate['revision'] += 1
          if initial || !@nats&.alive?
            @state.commit(candidate)
          else
            apply(candidate, expected_serial: replacement.serial)
          end
        end
        @tls = replacement
        prune_tls
        @next_renewal = Time.now + @config.tls['renewal_interval']
      rescue Error, SystemCallError, OpenSSL::OpenSSLError => error
        @failure = 'certificate_renewal_failed' unless @failure == 'configuration_apply_uncertain'
        @healthy = false
        Diagnostics.emit('certificate_renewal_failed', initial:, **Diagnostics.error_fields(error))
        raise if initial
      ensure
        prune_tls(preserve_candidate: true)
      end

      def start_runtime
        @runtime_stage = 'tls'
        @nats = @runtime = nil
        renew(initial: true) unless @tls.valid?
        @runtime_stage = 'binary_validation'
        validate_binary(@config['nats_binary'], 'nats-server: v2.15.0')
        validate_binary(@config['runtime_binary'], 'skvoz-network-runtime 0.4.0 network=4 api=1 core=4.0.0')
        @nats_path = File.join(@state.directory, 'nats.conf')
        PrivateFiles.write(@nats_path, @state.nats_config)
        @runtime_stage = 'nats_validation'
        validate_nats(@nats_path)
        @runtime_stage = 'nats_start'
        @nats = ChildProcess.new([@config['nats_binary'], '--config', @nats_path], label: 'nats').start(@task)
        @children << @nats
        wait_ready { probe }
        @runtime_stage = 'network_start'
        policy = NetworkConfiguration.new(@config)
        policy.inventory!(@config) unless policy.ip? && @helper_owner && ENV['SKVOZ_BOOTSTRAPPED'] == '1'
        profile_path = File.join(@state.directory, 'network-profile.json')
        profile = JSON.generate(@state.profile(policy))
        raise Error, 'Network startup configuration exceeds limit' if profile.bytesize > 32768
        PrivateFiles.write(profile_path, profile)
        control, child_control = UNIXSocket.pair
        control.nonblock = child_control.nonblock = true
        argv = [@config['runtime_binary'], '--config', profile_path, '--control-fd', '3']
        descriptors = { 3 => child_control }
        raise Error, 'Unexpected helper for TCP-only server' if !policy.ip? && @helper_owner
        if policy.ip?
          raise Error, 'Missing inherited helper owner' unless @helper_owner && @helper_pid && ENV['SKVOZ_BOOTSTRAPPED'] == '1'
          helper = @helper_owner
          descriptors[4] = helper
          argv.concat(['--helper-fd', '4'])
        end
        @runtime = ChildProcess.new(argv, label: 'network', descriptors:).start(@task)
        @children << @runtime
        child_control.close
        helper&.close
        @connector = RuntimeControl.new(control).start(@task)
        wait_ready { @connector.refresh; @connector.ready? }
        @runtime_stage = 'enrollment_start'
        start_enrollment
        @failure = nil
        clear_candidate
        prune_tls
        @healthy = @tls.valid?
        @runtime_stage = 'running'
        Diagnostics.emit('runtime_ready', revision: @state.revision)
      rescue StandardError
        control&.close unless control&.closed?
        raise
      ensure
        child_control&.close unless child_control&.closed?
        helper&.close unless helper&.closed?
      end

      def start_enrollment
        @enrollment = Enrollment.new(@config, @state) do |login, token|
          @mutation.acquire do
            raise Error, 'Configuration recovery required' if @failure == 'configuration_apply_uncertain'
            candidate, id = @state.enroll(login, token)
            apply(candidate) if candidate
            { v: 2, namespace: @config['namespace'], peer_id: id, network_runtime: { network: 4, api: 1, version: '0.4.0', core: '4.0.0' } }
          end
        end.start(@task)
      end

      def capture_command(argv)
        child = ChildProcess.new(argv, label: 'validation', capture: 4096).start(@task)
        @children << child
        deadline = monotonic + 3
        @task.sleep(0.01) while child.alive? && monotonic < deadline && !@stopping
        raise Error, 'Binary validation deadline exceeded' if child.alive? || @stopping
        @task.sleep(0) if child.output.empty?
        raise Error, 'Binary validation output exceeded limit' if child.output_overflow
        [child.output, child.status.success?]
      ensure
        child&.stop(monotonic + 1)
        @children.delete(child)
      end

      def validate_binary(binary, expected)
        output, success = capture_command([binary, '--version'])
        raise Error, 'Binary version mismatch' unless success && output.strip == expected
      end

      def validate_nats(path)
        output, success = capture_command([@config['nats_binary'], '-t', '--config', path])
        raise Error, 'NATS configuration rejected' unless success && output.include?('is valid')
      end

      def wait_ready
        deadline = monotonic + 10
        loop do
          return if yield
          raise Error, 'Child readiness deadline exceeded' if monotonic >= deadline || !children_alive? || @stopping
          @task.sleep(0.05)
        rescue Errno::ECONNREFUSED, IOError, OpenSSL::SSL::SSLError
          raise Error, 'Child readiness deadline exceeded' if monotonic >= deadline || !children_alive?
          @task.sleep(0.05)
        end
      end

      def probe(login: State::INTERNAL, password: @state.value.fetch('internal_password'), serial: nil)
        Async::Task.current.with_timeout(2) do
          socket = TCPSocket.new('127.0.0.1', @config['port'])
          tls = BrokerTLS.connect(socket, identity: @config['address'], ca: @state.value.fetch('tls')['ca'])
          raise Error, 'NATS certificate apply unconfirmed' if serial && tls.peer_cert.serial.to_s != serial
          tls.write('CONNECT ' + JSON.generate(user: login, pass: password, verbose: false, protocol: 1) + "\r\nPING\r\n")
          loop do
            line = tls.gets("\r\n", 4096)
            raise Error, 'NATS authentication rejected' if line.nil? || line.start_with?('-ERR')
            return true if line == "PONG\r\n"
          end
        ensure
          tls ? tls.close : socket&.close
        end
      rescue Async::TimeoutError
        raise Error, 'NATS probe deadline exceeded'
      end

      def monitor_revision
        http = Net::HTTP.new('127.0.0.1', @config['monitor_port'], nil)
        http.open_timeout = http.read_timeout = 1
        body = +''
        http.request(Net::HTTP::Get.new('/varz')) do |response|
          raise Error, 'NATS monitoring unavailable' unless response.code == '200'
          response.read_body do |chunk|
            raise Error, 'NATS monitoring exceeds limit' if body.bytesize + chunk.bytesize > 65_536
            body << chunk
          end
        end
        JSON.parse(body).fetch('config_load_time')
      end

      def apply(candidate, credentials: nil, old_credentials: nil, expected_serial: nil)
        raise Error, 'Runtime not ready for configuration apply' unless @nats&.alive?
        candidate_path = File.join(@state.directory, 'candidate.json')
        candidate_config = File.join(@state.directory, 'candidate.conf')
        previous_config = @state.nats_config
        PrivateFiles.write(candidate_path, JSON.generate(candidate))
        PrivateFiles.write(candidate_config, @state.nats_config(candidate))
        validate_nats(candidate_config)
        previous = monitor_revision
        @healthy = false
        PrivateFiles.write(@nats_path, @state.nats_config(candidate))
        @nats.signal('HUP')
        deadline = monotonic + 5
        @task.sleep(0.02) while monitor_revision == previous && monotonic < deadline
        raise Error, 'NATS reload apply unconfirmed' if monitor_revision == previous
        probe(serial: expected_serial)
        probe(login: credentials[0], password: credentials[1]) if credentials
        if old_credentials
          begin
            probe(login: old_credentials[0], password: old_credentials[1])
          rescue Error
            revoked = true
          end
          raise Error, 'NATS old credential remains active' unless revoked
        end
        @state.commit(candidate)
        @failure = nil
        @healthy = @connector&.ready? && @tls.valid?
        true
      rescue StandardError, Async::Stop => error
        Diagnostics.emit('configuration_apply_failed', revision: candidate['revision'], **Diagnostics.error_fields(error))
        @failure = 'configuration_apply_uncertain'
        @healthy = false
        @connector&.stop
        @enrollment&.stop
        PrivateFiles.write(@nats_path, previous_config) if previous_config
        @nats&.signal('HUP')
        raise if error.is_a?(Async::Stop)
        raise Error, 'Configuration apply failed; recovery required'
      ensure
        clear_candidate unless @failure == 'configuration_apply_uncertain'
      end

      def clear_candidate
        %w[candidate.json candidate.conf].each do |name|
          path = File.join(@state.directory, name)
          File.unlink(path) if File.exist?(path)
        end
      end

      def prune_tls(preserve_candidate: false)
        material = [@state.value['tls'], @state.value['previous_tls']]
        candidate_path = File.join(@state.directory, 'candidate.json')
        if preserve_candidate && File.exist?(candidate_path)
          material << JSON.parse(PrivateFiles.read(candidate_path, maximum: 1_048_576))['tls']
        end
        protected = material.compact.flat_map { |material| material.values.compact.map { |path| File.dirname(path) } }
        Dir.children(@state.directory).grep(/\Atls-[0-9a-f]{16}\z/).each do |name|
          path = File.join(@state.directory, name)
          next if protected.include?(path)
          PrivateFiles.directory(path)
          files = Dir.children(path)
          raise Error, 'Unexpected TLS generation entry' unless (files - %w[certificate.pem key.pem ca.pem certificate_ca.pem]).empty?
          files.each do |file|
            entry = File.join(path, file)
            PrivateFiles.read(entry, maximum: 1_048_576)
            File.unlink(entry)
          end
          Dir.rmdir(path)
        end
      end

      def administration
        loop do
          socket = @listener.accept
          @admin_clients.reject! { |client| client.finished? }
          if @admin_clients.length >= 8
            socket.close
          else
            @admin_clients << @task.async { admin_request(socket) }
          end
        end
      rescue IOError, Errno::EBADF
        nil
      end

      def admin_request(socket)
        request = nil
        Async::Task.current.with_timeout(@config['admin_timeout']) do
          uid = socket.getsockopt(Socket::SOL_SOCKET, Socket::SO_PEERCRED).to_s.unpack('iii')[1]
          raise Error, 'Administration UID mismatch' unless uid == Process.euid
          line = socket.gets("\n", 65_537)
          raise Error, 'Administration request exceeds limit' unless line && line.bytesize <= 65_536 && line.end_with?("\n")
          request = JSON.parse(line)
        end
        result = Async::Task.current.with_timeout(15) { @mutation.acquire { command(request) } }
        Async::Task.current.with_timeout(2) { socket.write(JSON.generate('ok' => true, 'result' => result) + "\n") }
      rescue StandardError => error
        Diagnostics.emit('administration_failed', **Diagnostics.error_fields(error))
        socket.write(JSON.generate('ok' => false, 'error' => 'Administration request failed') + "\n") rescue nil
      ensure
        socket.close
      end

      def command(request)
        raise Error, 'Invalid administration schema' unless request.is_a?(Hash) && (request.keys - %w[operation login password]).empty?
        operation = request['operation']
        raise Error, 'Invalid administration operation' unless %w[health list show add device-add reset-password remove].include?(operation)
        raise Error, 'Invalid administration login type' unless request['login'].nil? || request['login'].is_a?(String)
        raise Error, 'Invalid administration password' unless request['password'].nil? || request['password'].is_a?(String)
        unless %w[health list].include?(operation)
          raise Error, 'Invalid administration login' unless request['login'].is_a?(String) && request['login'].match?(State::LOGIN)
        end
        return { 'healthy' => healthy?, 'revision' => @state.revision, 'failure' => @failure, 'connector' => @connector&.statistics } if operation == 'health'
        raise Error, 'Configuration recovery required' if @failure == 'configuration_apply_uncertain'
        return @state.value.fetch('users').map { |login, user| { 'login' => login, 'devices' => user.fetch('assigned').length, 'capacity' => user.fetch('ids').length } } if operation == 'list'
        login = request.fetch('login')
        if operation == 'show'
          user = @state.value.fetch('users').fetch(login)
          return { 'address' => @config['address'], 'port' => @config['advertised_port'], 'username' => login, 'devices' => user.fetch('assigned'), 'trust' => @state.value.fetch('tls')['ca'] ? 'managed_ca' : 'system' }
        end
        password = request['password']
        password ||= SecureRandom.urlsafe_base64(32) if %w[add reset-password].include?(operation)
        candidate, id = @state.mutate(operation, login, password:)
        credentials = [login, password] if %w[add reset-password device-add].include?(operation)
        if id
          result = @state.export(login, id, password)
          result['ca_pem'] = PrivateFiles.read(@state.value.fetch('tls')['ca']) if @state.value.fetch('tls')['ca']
          raise Error, 'Export exceeds administration response limit' if JSON.generate(result).bytesize > 65_472
        end
        apply(candidate, credentials:)
        if id
          result
        elsif operation == 'reset-password'
          { 'address' => @config['address'], 'port' => @config['advertised_port'], 'username' => login, 'password' => password }
        else
          { 'removed' => login }
        end
      end

      def children_alive?
        if @helper_pid && !@helper_status
          result = Process.waitpid2(@helper_pid, Process::WNOHANG)
          @helper_status = result.last if result
        end
        @nats&.alive? && (@runtime.nil? || @runtime.alive?) && @helper_status.nil?
      rescue Errno::ECHILD
        @helper_status = :reaped
        false
      end
      def monotonic = Process.clock_gettime(Process::CLOCK_MONOTONIC)

      def sleep_until(deadline)
        @task.sleep([0.1, deadline - monotonic].min) while !@stopping && monotonic < deadline
      end

      def stop_children(deadline = monotonic + @config['stop_timeout'])
        @children.each { |child| child.signal('TERM') }
        @children.each { |child| child.stop(deadline) }
        @children.clear
      end

      def remember_socket(path, record, pid)
        stat = File.lstat(path)
        @socket_identity[path] = [stat.dev, stat.ino]
        PrivateFiles.write(File.join(@state.directory, record), JSON.generate('pid' => pid, 'start' => ChildProcess.identity(pid)&.fetch(:start), 'dev' => stat.dev, 'ino' => stat.ino))
      end

      def recover_socket(path, record)
        return unless File.exist?(path) || File.symlink?(path)
        owner_path = File.join(@state.directory, record)
        raise Error, 'Existing socket has no proven owner' unless File.exist?(owner_path)
        owner = JSON.parse(PrivateFiles.read(owner_path))
        identity = ChildProcess.identity(owner.fetch('pid'))
        if identity && identity[:start] == owner.fetch('start') && identity[:state] != 'Z'
          raise Error, 'Existing socket owner is alive'
        end
        stat = File.lstat(path)
        raise Error, 'Existing socket identity changed' unless stat.socket? && stat.uid == Process.euid && stat.dev == owner.fetch('dev') && stat.ino == owner.fetch('ino')
        File.unlink(path)
      end

      def reap_exited_children
        loop { break unless Process.waitpid(-1, Process::WNOHANG) }
      rescue Errno::ECHILD
        nil
      end

      def shutdown
        deadline = monotonic + @config['stop_timeout']
        stop
        @listener&.close
        @enrollment&.stop
        begin
          @connector&.prepare_shutdown(timeout: [5, deadline - monotonic].min)
        rescue Error
          @failure = 'network_shutdown_failed'
        ensure
          @connector&.stop
        end
        @admin_clients.each { |task| task.stop if task.alive? }
        @admin_task&.stop if @admin_task&.alive?
        @helper_owner&.close unless @helper_owner&.closed?
        stop_children(deadline)
        if @admin_path && File.socket?(@admin_path)
          stat = File.lstat(@admin_path)
          File.unlink(@admin_path) if @socket_identity[@admin_path] == [stat.dev, stat.ino]
        end
        if @helper_pid
          while !@helper_status && monotonic < deadline
            result = Process.waitpid2(@helper_pid, Process::WNOHANG)
            @helper_status = result.last if result
            @task.sleep(0.02) unless @helper_status
          end
          raise Error, 'Helper cleanup deadline exceeded; container restart required' unless @helper_status.is_a?(Process::Status) && @helper_status.success?
        end
        # PID1 also reaps adopted short-lived descendants after owned children.
        reap_exited_children
        @state.close
      end
    end
  end
end
