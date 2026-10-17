# frozen_string_literal: true
require 'fileutils'
require 'securerandom'
require 'timeout'
require 'io/nonblock'
require 'time'

module Skvoz
  module Client
    class State
      attr_reader :directory, :device
      def initialize(directory)
        @directory = File.expand_path(directory)
        parent = File.dirname(@directory)
        validate_directory(parent)
        Dir.mkdir(@directory, 0o700) unless File.exist?(@directory)
        validate_directory(@directory)
        info = File.lstat(@directory)
        raise Terminal, 'unsafe_state' unless info.uid == Process.euid && info.mode & 0o777 == 0o700
        @lock = File.open(path('lock'), File::RDWR | File::CREAT | File::NOFOLLOW, 0o600)
        unless @lock.flock(File::LOCK_EX | File::LOCK_NB)
          @lock.close
          raise Terminal, 'state_in_use'
        end
        if File.exist?(path('device.json'))
          value = JSON.parse(read('device.json'), allow_duplicate_key: false)
          raise Terminal, 'invalid_device_state' unless value.keys.sort == %w[device v] && value['v'] == 1 && value['device'].is_a?(String) && value['device'].match?(/\A[0-9a-f]{32}\z/)
          @device = value.fetch('device')
        else
          @device = SecureRandom.hex(16)
          write('device.json', { 'v' => 1, 'device' => @device })
        end
      end
      def path(name) = File.join(@directory, name)
      def read(name)
        file = File.open(path(name), File::RDONLY | File::NOFOLLOW)
        info = file.stat
        raise Terminal, 'unsafe_state_file' unless info.file? && info.uid == Process.euid && info.mode & 0o177 == 0 && info.size <= 32768
        file.read(32769)
      ensure
        file&.close
      end
      def write(name, value)
        temporary = path(name + '.tmp-' + SecureRandom.hex(8))
        File.open(temporary, File::WRONLY | File::CREAT | File::EXCL | File::NOFOLLOW, 0o600) do |file|
          file.write(JSON.generate(value)); file.flush; file.fsync
        end
        File.rename(temporary, path(name))
      ensure
        File.unlink(temporary) if temporary && File.exist?(temporary)
      end
      def close = @lock&.close
      private
      def validate_directory(path)
        current = '/'
        path.split('/').reject(&:empty?).each do |part|
          current = File.join(current, part)
          stat = File.lstat(current)
          valid = stat.directory? && !stat.symlink? && [0, Process.euid].include?(stat.uid) && ((stat.mode & 0o022).zero? || stat.mode & 0o1000 != 0)
          raise Terminal, 'unsafe_state_directory' unless valid
        end
      end
    end

    class Service
      RUNTIME = ENV.fetch('SKVOZ_RUNTIME_BINARY', '/usr/local/bin/skvoz-network-runtime')
      attr_reader :state
      def initialize(config, ip, os_socket = nil, os_pid = nil)
        @config, @ip, @os_socket, @os_pid = config, ip, os_socket, os_pid
        @state = State.new(ENV.fetch('SKVOZ_CLIENT_STATE', '/var/lib/skvoz-client/app'))
        @run_directory = ENV.fetch('SKVOZ_CLIENT_RUN', '/run/skvoz-client')
        info = File.lstat(@run_directory)
        raise Terminal, 'unsafe_run_directory' unless info.directory? && !info.symlink? && info.uid == Process.euid && info.mode & 0o777 == 0o700
        @stop_read, @stop_write = IO.pipe
        @stopping, @healthy, @os_blocked = false, false, true
      end
      def stop = @stop_write.write_nonblock('.', exception: false)
      def run(task)
        @os = Control.new(@os_socket, task) if @os_socket
        watcher = task.async do
          @stop_read.read(1)
          @stopping = true
          @shutdown_deadline = self.class.monotonic + 10
          @attempt&.stop
        end
        backoff = 0
        until @stopping
          begin
            @attempt = task.async do |attempt|
              begin
                connect(attempt)
              rescue StandardError => error
                error
              end
            end
            result = @attempt.wait
            raise result if result.is_a?(StandardError)
          rescue Terminal => error
            report('failed', error.message)
            health(false, error.message)
            raise
          rescue StandardError => error
            raise Terminal, 'vpn_os_unavailable' if @os&.failure
            report('reconnecting', safe_code(error))
            health(false, safe_code(error))
          end
          break if @stopping
          delay = [1, 2, 4, 8, 15][[backoff, 4].min]
          backoff += 1
          @attempt = task.async { |sleep_task| sleep_task.sleep(delay) }
          @attempt.wait
        end
      ensure
        watcher&.stop
        @healthy = false
        health(false, 'stopped') rescue nil
        cleanup
        @os&.call('STOP', {}, timeout: budget(1)) rescue nil
        @os&.stop
        reap(@os_pid, budget(0.25), kill: false) if @os_pid
        @stop_read.close
        @stop_write.close
        @state.close
      end
      def self.health(directory)
        path = File.join(File.expand_path(directory), 'health.json')
        file = File.open(path, File::RDONLY | File::NOFOLLOW)
        info = file.stat
        raise Error, 'health_unavailable' unless info.uid == Process.euid && info.mode & 0o177 == 0 && info.size <= 32768
        value = JSON.parse(file.read(32769), allow_duplicate_key: false)
        valid = value['v'] == 1 && value['healthy'] == true && value['pid'].is_a?(Integer) && value['pid'] > 0 && monotonic - value.fetch('updated') <= 2
        valid &&= File.read("/proc/#{value['pid']}/stat").split(') ', 2).last.split.first != 'Z'
        { 'healthy' => !!valid, 'mode' => value['mode'], 'state' => value['state'] }
      rescue StandardError
        { 'healthy' => false, 'state' => 'unavailable' }
      ensure
        file&.close
      end
      def self.monotonic = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      private
      def report(stage, code = nil)
        puts JSON.generate(component: 'client', stage:, code:)
        STDOUT.flush
      end
      def safe_code(error)
        error.is_a?(Error) && error.message.match?(/\A[a-z_]{1,64}\z/) ? error.message : 'connection_failed'
      end
      def health(healthy, status)
        temporary = File.join(@run_directory, 'health-' + SecureRandom.hex(8))
        File.open(temporary, File::WRONLY | File::CREAT | File::EXCL | File::NOFOLLOW, 0o600) do |file|
          file.write(JSON.generate('v' => 1, 'healthy' => healthy, 'pid' => Process.pid, 'updated' => self.class.monotonic, 'mode' => @config.mode, 'state' => status))
        end
        File.rename(temporary, File.join(@run_directory, 'health.json'))
      ensure
        File.unlink(temporary) if temporary && File.exist?(temporary)
      end
      def connect(task)
        health(false, 'connecting')
        task.with_timeout(60) do
          enrollment = Enrollment.new(@config, @ip).call(@state.device)
          start_runtime(task, enrollment)
          if @config.mode == 'proxy'
            result, fd = @control.call('START_PROXY', { 'http_bind' => @config.proxy['http'], 'socks_bind' => @config.proxy['socks'] })
            raise Error, 'invalid_proxy_result' if fd || result != { 'http' => @config.proxy['http'] && 'http://' + @config.proxy['http'], 'socks' => @config.proxy['socks'] && 'socks5://' + @config.proxy['socks'] }
            @adapters = Adapters.new(@config, @control, task)
            @adapters.start
          else
            start_vpn
          end
        end
        @healthy = true
        report('connected')
        loop do
          raise Terminal, 'vpn_os_unavailable' if @os&.failure
          raise Error, 'runtime_exited' if Process.waitpid(@runtime_pid, Process::WNOHANG)
          status, fd = @control.call('STATUS', {}, timeout: 3)
          raise Error, 'runtime_unavailable' if fd || status['lifecycle'] != 'ready' || status['mode'] != (@config.mode == 'vpn' ? 'ip' : 'proxy')
          raise Error, 'ip_session_closed' if @config.mode == 'vpn' && status['session'] != { 'handle' => @ip_handle, 'state' => 'active' }
          health(true, 'connected')
          task.sleep(0.25)
        end
      ensure
        task.defer_stop do
          @healthy = false
          health(false, 'disconnected') rescue nil
          cleanup
        end
      end
      def start_runtime(task, enrollment)
        network = JSON.parse(File.read(File.join(__dir__, 'network-defaults.json')))
        network['families'] = @config.families
        url = @ip.include?(':') ? "tls://[#{@ip}]:#{@config.port}" : "tls://#{@ip}:#{@config.port}"
        value = { 'v' => 3, 'role' => 'client', 'server' => nil, 'access' => nil,
          'core' => { 'url' => url, 'tls_server_name' => @config.host, 'trust' => @config.ca_file ? 'managed_ca' : 'system',
            'ca_file' => @config.ca_file, 'username' => @config.username, 'password' => @config.password,
            'namespace' => enrollment.fetch('namespace'), 'peer_id' => enrollment.fetch('peer_id').to_s,
            'membership' => 'allowlist', 'allowed_peers' => ['0'], 'initiate' => ['0'] }, 'network' => network }
        @state.write('runtime.json', value)
        owner, child = UNIXSocket.pair
        owner.nonblock = child.nonblock = true
        @runtime_pid = Process.spawn('setpriv', '--pdeathsig', 'KILL', RUNTIME, '--config', @state.path('runtime.json'), '--control-fd', '3',
          3 => child, close_others: true, in: File::NULL, out: File::NULL, err: File::NULL, pgroup: true)
        child.close
        @control = Control.new(owner, task)
        hello, fd = @control.call('HELLO', { 'api' => 1, 'network' => 6 }, timeout: 5)
        valid = !fd && hello.keys.sort == %w[api capabilities network role] && hello['api'] == 1 && hello['network'] == 6 && hello['role'] == 'client'
        valid &&= hello['capabilities'] == { 'profiles' => ['tcp', 'ip'], 'families' => @config.families, 'max_mtu' => 1500, 'max_channels' => 1 }
        raise Terminal, 'version_mismatch' unless valid
        File.unlink(@state.path('runtime.json'))
        task.with_timeout(30) do
          loop do
            status, descriptor = @control.call('STATUS', {}, timeout: 3)
            raise Error, 'invalid_runtime_status' if descriptor
            break if status['lifecycle'] == 'ready'
            raise Error, 'runtime_unavailable' if %w[closed closing].include?(status['lifecycle'])
            task.sleep(0.05)
          end
        end
      ensure
        child&.close unless child&.closed?
      end
      def start_vpn
        raise Terminal, 'vpn_requires_root_bootstrap' unless @os
        response, fd = @control.call('START_IP', { 'families' => @config.families, 'family_policy' => 'auto', 'max_mtu' => 1500, 'channels' => 1 })
        raise Error, 'invalid_ip_handle' if fd
        @ip_handle = response.fetch('handle')
        configured = @control.event('CONFIGURED')
        raise Error, 'invalid_ip_handle' unless configured['handle'] == @ip_handle
        config = configured.fetch('config')
        @os_blocked = false
        prepared, tun = @os.call('PREPARE', config)
        raise Error, 'invalid_tun_descriptor' unless tun && prepared == { 'interface' => 'skvoz0' }
        response, descriptor = @control.call('ATTACH_IP', { 'handle' => @ip_handle, 'interface' => 'skvoz0', 'mtu' => config.fetch('mtu') }, fd: tun)
        raise Error, 'invalid_ip_handle' if descriptor || response != { 'handle' => @ip_handle }
        tun.close
        @os.call('ACTIVATE')
        @control.call('LOCAL_READY', { 'handle' => @ip_handle })
        active = @control.event('ACTIVE')
        raise Error, 'invalid_ip_handle' unless active['handle'] == @ip_handle
      ensure
        tun&.close unless tun&.closed?
      end
      def budget(maximum)
        remaining = (@cleanup_deadline || @shutdown_deadline || self.class.monotonic + maximum) - self.class.monotonic
        raise Error, 'shutdown_deadline' unless remaining.positive?
        [remaining, maximum].min
      end
      def cleanup
        @cleanup_deadline = [self.class.monotonic + 8, @shutdown_deadline || Float::INFINITY].min
        unless @os_blocked
          begin
            @os&.call('BLOCK', {}, timeout: budget(2))
          rescue StandardError
            @os&.stop
          ensure
            @os_blocked = true
          end
        end
        @adapters&.stop
        @adapters = nil
        if @control
          @control.call('STOP_IP', { 'handle' => @ip_handle, 'reason' => 'shutdown' }, timeout: budget(2)) rescue nil if @ip_handle
          @control.call('PREPARE_SHUTDOWN', {}, timeout: budget(2)) rescue nil
          @control.stop
          @control = nil
        end
        @ip_handle = nil
        reap(@runtime_pid, budget(2)) if @runtime_pid
      rescue StandardError
        Process.kill('KILL', @runtime_pid) rescue nil if @runtime_pid
      ensure
        @runtime_pid = nil
        @cleanup_deadline = nil
      end
      def reap(pid, seconds, kill: true)
        deadline = self.class.monotonic + seconds
        loop do
          return if Process.waitpid(pid, Process::WNOHANG)
          break if self.class.monotonic >= deadline
          sleep(0.02)
        end
        Process.kill('KILL', pid) if kill
        deadline = self.class.monotonic + 0.25
        until Process.waitpid(pid, Process::WNOHANG)
          break if self.class.monotonic >= deadline
          sleep(0.01)
        end
      rescue Errno::ECHILD, Errno::ESRCH
        nil
      end
    end
  end
end
