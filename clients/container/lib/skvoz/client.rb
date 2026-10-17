# frozen_string_literal: true
require 'async'
require 'json'
require 'openssl'
require_relative 'client/config'
require_relative 'client/control'
require_relative 'client/enrollment'
require_relative 'client/vpn'
require_relative 'client/adapters'
require_relative 'client/service'

module Skvoz
  module Client
    VERSION = '1.0.1'
    class Bootstrap
      def self.run(arguments)
        command = arguments.shift || 'serve'
        raise Terminal, 'invalid_arguments' unless arguments.empty? || command == '--os-worker'
        case command
        when '--version'
          puts "skvoz-client #{VERSION}; Ruby #{RUBY_VERSION}; network 0.7.0 / API 1 / Core 5.0.0"
        when 'check'
          Config.new(ENV.fetch('SKVOZ_CLIENT_CONFIG', ''))
          puts JSON.generate(valid: true)
        when 'health'
          drop! if Process.euid.zero?
          health = Service.health(ENV.fetch('SKVOZ_CLIENT_RUN', '/run/skvoz-client'))
          puts JSON.generate(health)
          exit(health['healthy'] ? 0 : 1)
        when '--os-worker'
          raise Terminal, 'invalid_os_bootstrap' unless Process.euid.zero? && arguments.size == 4
          ip, port, families, descriptor = arguments
          raise Terminal, 'invalid_os_bootstrap' unless IPAddr.new(ip).to_s == ip && port.match?(/\A[0-9]{1,5}\z/) && (1..65535).cover?(port.to_i) && %w[4 6 4,6].include?(families) && descriptor == '3'
          Vpn.new(UNIXSocket.for_fd(3), ip, port.to_i, families.split(',').map(&:to_i)).serve
        when 'serve'
          config = Config.new(ENV.fetch('SKVOZ_CLIENT_CONFIG', ''))
          root(config) if Process.euid.zero?
          raise Terminal, 'unsafe_runtime_privileges' unless Process.euid == 10001 && File.read('/proc/self/status').match?(/^CapEff:\s+0+$/) && File.read('/proc/self/status').match?(/^NoNewPrivs:\s+1$/)
          ip = ENV.fetch('SKVOZ_CLIENT_ENDPOINT') { Timeout.timeout(5) { Addrinfo.getaddrinfo(config.host, config.port, nil, :STREAM).first.ip_address } }
          raise Terminal, 'invalid_endpoint' unless IPAddr.new(ip).to_s == ip
          socket = ENV['SKVOZ_CLIENT_OS_FD'] == '3' ? UNIXSocket.for_fd(3) : nil
          raise Terminal, 'vpn_requires_root_bootstrap' if config.mode == 'vpn' && !socket
          result = Async do |task|
            service = Service.new(config, ip, socket, ENV['SKVOZ_CLIENT_OS_PID']&.to_i)
            %w[TERM INT].each { |signal| Signal.trap(signal) { service.stop } }
            service.run(task)
          rescue StandardError => error
            error
          end.wait
          raise result if result.is_a?(StandardError)
        else
          raise Terminal, 'invalid_command'
        end
      end
      def self.root(config)
        ip = endpoint(config)
        directory = ENV.fetch('SKVOZ_CLIENT_STATE', '/var/lib/skvoz-client/app')
        unless File.exist?(directory)
          Dir.mkdir(directory, 0o700)
          File.chown(10001, 10001, directory)
        end
        info = File.lstat(directory)
        raise Terminal, 'unsafe_state' unless info.directory? && !info.symlink? && info.uid == 10001 && info.mode & 0o777 == 0o700
        ENV['SKVOZ_CLIENT_ENDPOINT'] = ip
        if config.mode == 'vpn'
          owner, child = UNIXSocket.pair
          pid = Process.spawn('setpriv', '--no-new-privs', '--bounding-set=-all,+net_admin', '--inh-caps=-all', '--ambient-caps=-all', RbConfig.ruby, $PROGRAM_NAME,
            '--os-worker', ip, config.port.to_s, config.families.join(','), '3', 3 => child, close_others: true)
          child.close
          reply, fd = Timeout.timeout(8) { Wire.new(owner).read }
          raise Terminal, 'vpn_guard_unavailable' unless !fd && reply == { 'v' => 1, 'id' => 0, 'result' => { 'guard' => 'blocked' }, 'error' => nil, 'fd_count' => 0 }
          ENV['SKVOZ_CLIENT_OS_FD'], ENV['SKVOZ_CLIENT_OS_PID'] = '3', pid.to_s
          drop!(owner)
        else
          drop!
        end
      end
      def self.endpoint(config)
        return Timeout.timeout(5) { Addrinfo.getaddrinfo(config.host, config.port, nil, :STREAM).first.ip_address } unless config.mode == 'vpn'
        directory = '/var/lib/skvoz-client/boot'
        Dir.mkdir(directory, 0o700) unless File.exist?(directory)
        stat = File.lstat(directory)
        raise Terminal, 'unsafe_endpoint_cache' unless stat.directory? && !stat.symlink? && stat.uid.zero? && stat.mode & 0o777 == 0o700
        path = File.join(directory, 'endpoint.json')
        tables, status = Open3.capture2('/usr/sbin/nft', '-j', 'list', 'tables')
        raise Terminal, 'guard_inventory_failed' unless status.success?
        retained = JSON.parse(tables).fetch('nftables').any? { |entry| entry['table']&.values_at('family', 'name') == ['inet', 'skvoz_client'] }
        if retained
          raise Terminal, 'endpoint_cache_missing' unless File.exist?(path)
          File.open(path, File::RDONLY | File::NOFOLLOW) do |file|
            info = file.stat
            raise Terminal, 'unsafe_endpoint_cache' unless info.file? && info.uid.zero? && info.mode & 0o777 == 0o600 && info.size <= 1024
            value = JSON.parse(file.read(1025), allow_duplicate_key: false)
            raise Terminal, 'endpoint_change_requires_recreation' unless value.keys.sort == %w[ip server v] && value['v'] == 1 && value['server'] == config.value.fetch('server') && IPAddr.new(value['ip']).to_s == value['ip']
            return value.fetch('ip')
          end
        end
        ip = Timeout.timeout(5) { Addrinfo.getaddrinfo(config.host, config.port, nil, :STREAM).first.ip_address }
        temporary = File.join(directory, 'endpoint-' + SecureRandom.hex(8))
        File.open(temporary, File::WRONLY | File::CREAT | File::EXCL | File::NOFOLLOW, 0o600) do |file|
          file.write(JSON.generate('v' => 1, 'server' => config.value.fetch('server'), 'ip' => ip)); file.flush; file.fsync
        end
        File.rename(temporary, path)
        ip
      end
      def self.drop!(socket = nil)
        mapping = socket ? { 3 => socket } : {}
        Process.exec('setpriv', '--reuid=10001', '--regid=10001', '--clear-groups', '--no-new-privs', '--bounding-set=-all', '--inh-caps=-all', '--ambient-caps=-all',
          RbConfig.ruby, $PROGRAM_NAME, *ARGV, **mapping, close_others: true)
      end
    end
  end
end
