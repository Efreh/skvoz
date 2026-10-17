# frozen_string_literal: true
require 'open3'
require 'ipaddr'
require 'io/nonblock'
require 'securerandom'
require 'timeout'

module Skvoz
  module Client
    # This process has only NET_ADMIN in the container's own network namespace.
    class Vpn
      TABLE = 51818
      PRIORITY = 10000
      def initialize(socket, endpoint, port, offered)
        @owner = File.open('/run/skvoz-client-os.lock', File::RDWR | File::CREAT | File::NOFOLLOW, 0o600)
        stat = @owner.stat
        raise Terminal, 'unsafe_os_lock' unless stat.file? && stat.uid.zero? && stat.mode & 0o777 == 0o600 && stat.nlink == 1
        raise Terminal, 'vpn_namespace_in_use' unless @owner.flock(File::LOCK_EX | File::LOCK_NB)
        @wire, @endpoint, @port, @offered = Wire.new(socket), IPAddr.new(endpoint), port, offered
        @token, @families, @rules = SecureRandom.hex(16), [], []
        @underlay = JSON.parse(command('ip', '-j', 'route', 'get', endpoint)).fetch(0)
        @device = @underlay.fetch('dev')
        raise Terminal, 'invalid_underlay' unless @device.match?(/\A[a-zA-Z0-9_.-]{1,15}\z/) && @device != 'skvoz0'
        tables = JSON.parse(command('nft', '-j', 'list', 'tables')).fetch('nftables')
        existing = tables.find { |entry| entry['table']&.values_at('family', 'name') == ['inet', 'skvoz_client'] }
        if existing
          table = JSON.parse(command('nft', '-j', 'list', 'table', 'inet', 'skvoz_client')).fetch('nftables').find { |entry| entry.key?('table') }.fetch('table')
          marker = table['comment']
          raise Terminal, 'foreign_guard_table' unless marker.is_a?(String) && marker.match?(/\Askvoz-client\/1:[0-9a-f]{32}\z/)
          @token = marker.split(':').last
        else
          command('nft', '-f', '-', input: "add table inet skvoz_client { comment \"skvoz-client/1:#{@token}\"; }\n")
        end
        link = JSON.parse(command('ip', '-j', 'link', 'show')).find { |item| item['ifname'] == 'skvoz0' }
        raise Terminal, 'foreign_tun_interface' if link && link['ifalias'] != 'skvoz:' + @token
        guard(false)
        if existing
          [4, 6].each do |family|
            rules = JSON.parse(command('ip', "-#{family}", '-j', 'rule', 'show'))
            owned = rules.select { |rule| rule['priority'] == PRIORITY || rule['table'].to_s == TABLE.to_s }
            raise Terminal, 'foreign_route_rule' unless owned.all? { |rule| rule['priority'] == PRIORITY && rule['table'].to_s == TABLE.to_s }
            routes = JSON.parse(command('ip', "-#{family}", '-j', 'route', 'show', 'table', 'all')).select { |route| route['table'].to_s == TABLE.to_s }
            raise Terminal, 'foreign_route_table' unless routes.all? { |route| route['dev'] == 'skvoz0' || route['dev'] == @device && route['dst'] == @endpoint.to_s }
            @rules << family unless owned.empty? && routes.empty?
          end
          clear
          raise Terminal, 'stale_tun_busy' if link
        end
      end
      def serve
        @wire.write(reply(0, { 'guard' => 'blocked' }))
        expected = 0
        loop do
          request, fd = @wire.read
          fd&.close
          valid = !fd && request.keys.sort == %w[args fd_count id op v] && request['v'] == 1 && request['id'] == expected + 1 && request['args'].is_a?(Hash)
          raise Error, 'invalid_os_request' unless valid
          expected = request.fetch('id')
          descriptor = nil
          result = case request.fetch('op')
          when 'PREPARE' then descriptor = prepare(request.fetch('args')); { 'interface' => 'skvoz0' }
          when 'ACTIVATE' then raise(Error, 'os_not_prepared') unless @tun; guard(true); {}
          when 'BLOCK' then guard(false); clear; {}
          when 'STOP' then guard(false); clear; @wire.write(reply(expected, {})); break
          else raise Error, 'invalid_os_operation'
          end
          @wire.write(reply(expected, result, descriptor), descriptor)
        end
      ensure
        guard(false) rescue nil
        clear rescue nil
        @wire.close rescue nil
        @owner&.close
      end
      private
      def reply(id, result, fd = nil) = { 'v' => 1, 'id' => id, 'result' => result, 'error' => nil, 'fd_count' => fd ? 1 : 0 }
      def command(tool, *args, input: '')
        output = nil
        Timeout.timeout(4) do
          Open3.popen3('/usr/sbin/' + tool, *args) do |stdin, stdout, stderr, process|
            stdin.write(input); stdin.close
            readers = [stdout, stderr].map { |io| Thread.new { io.read(524289) || ''.b } }
            output, error = readers.map(&:value)
            raise Error, 'os_command_failed' unless process.value.success? && output.bytesize <= 524288 && error.bytesize <= 524288
          ensure
            Process.kill('KILL', process.pid) rescue Errno::ESRCH if process&.alive?
          end
        end
        output
      rescue Timeout::Error
        raise Error, 'os_command_timeout'
      end
      def guard(active)
        text = +"flush table inet skvoz_client\nadd chain inet skvoz_client output { type filter hook output priority -150; policy drop; }\n"
        text << "add rule inet skvoz_client output ip daddr 127.0.0.11 counter drop\nadd rule inet skvoz_client output oifname lo accept\n"
        text << "add rule inet skvoz_client output meta nfproto ipv6 icmpv6 type { nd-router-solicit, nd-router-advert, nd-neighbor-solicit, nd-neighbor-advert } accept\n"
        text << "add rule inet skvoz_client output #{@endpoint.ipv4? ? 'ip' : 'ip6'} daddr #{@endpoint} tcp dport #{@port} accept\n"
        @families.each { |family| text << "add rule inet skvoz_client output oifname skvoz0 meta nfproto ipv#{family} accept\n" } if active
        command('nft', '-f', '-', input: text)
      end
      def validate(config)
        keys = %w[channels dns_servers egress families mtu packet_queue_bytes packet_queue_records routes session setup_timeout_ms source_grants]
        raise Error, 'invalid_ip_config' unless config.keys.sort == keys && config['families'].is_a?(Array) && !config['families'].empty? && (config['families'] - @offered).empty? && config['families'] == config['families'].uniq.sort
        raise Error, 'invalid_ip_config' unless config['mtu'].is_a?(Integer) && (576..1500).cover?(config['mtu']) && (!config['families'].include?(6) || config['mtu'] >= 1280) && config['channels'] == 1
        %w[source_grants routes].each do |key|
          values = config[key]
          raise Error, 'invalid_ip_config' unless values.is_a?(Array) && values.size.between?(1, key == 'routes' ? 32 : 8)
          values.each do |value|
            raise Error, 'invalid_ip_config' unless value.is_a?(String) && value.match?(/\A[^%\/]+\/[0-9]{1,3}\z/)
            ip = IPAddr.new(value)
            bits = Integer(value.split('/').last, 10)
            family = ip.ipv4? ? 4 : 6
            raise Error, 'invalid_ip_config' unless config['families'].include?(family) && "#{ip}/#{bits}" == value
            if key == 'source_grants'
              raise Error, 'invalid_ip_grant' unless bits == (family == 4 ? 32 : 128) && !ip.loopback? && !ip.link_local? && !ip.include?(@endpoint)
            end
          end
        end
        dns = config['dns_servers']
        raise Error, 'invalid_ip_config' unless dns.is_a?(Array) && dns.size.between?(1, 4) && dns.all? { |address| address.is_a?(String) && !address.include?('%') && (ip = IPAddr.new(address)).to_s == address && config['families'].include?(ip.ipv4? ? 4 : 6) && !ip.loopback? && !ip.link_local? }
      rescue IPAddr::InvalidAddressError, ArgumentError
        raise Error, 'invalid_ip_config'
      end
      def prepare(config)
        raise Error, 'os_already_prepared' if @tun
        validate(config)
        @families = config.fetch('families')
        @tun = File.open('/dev/net/tun', File::RDWR | File::NONBLOCK)
        request = 'skvoz0'.ljust(16, "\0") + [0x9001].pack('S') + "\0" * 22
        @tun.ioctl(0x400454ca, request)
        @tun.ioctl(0x400454d0, 0)
        @tun.nonblock = true
        command('ip', 'link', 'set', 'dev', 'skvoz0', 'alias', 'skvoz:' + @token, 'mtu', config.fetch('mtu').to_s)
        config.fetch('source_grants').each { |grant| command('ip', IPAddr.new(grant).ipv4? ? '-4' : '-6', 'address', 'add', grant, 'dev', 'skvoz0', 'nodad') }
        command('ip', 'link', 'set', 'dev', 'skvoz0', 'up')
        @families.each do |family|
          existing = JSON.parse(command('ip', "-#{family}", '-j', 'route', 'show', 'table', 'all'))
          raise Error, 'route_table_conflict' if existing.any? { |route| route['table'].to_s == TABLE.to_s }
          rules = JSON.parse(command('ip', "-#{family}", '-j', 'rule', 'show'))
          raise Error, 'route_rule_conflict' if rules.any? { |rule| rule['priority'] == PRIORITY || rule['table'].to_s == TABLE.to_s }
          @rules << family
          if @endpoint.ipv4? == (family == 4)
            endpoint = ["#{@endpoint}/#{family == 4 ? 32 : 128}", 'dev', @device]
            endpoint += ['via', @underlay['gateway'], 'onlink'] if @underlay['gateway']
            command('ip', "-#{family}", 'route', 'add', *endpoint, 'table', TABLE.to_s)
          end
          config.fetch('routes').select { |route| IPAddr.new(route).ipv4? == (family == 4) }.each do |route|
            command('ip', "-#{family}", 'route', 'add', route, 'dev', 'skvoz0', 'table', TABLE.to_s)
          end
          command('ip', "-#{family}", 'rule', 'add', 'priority', PRIORITY.to_s, 'lookup', TABLE.to_s)
        end
        File.open('/etc/resolv.conf', 'w') { |file| file.write(config.fetch('dns_servers').map { |address| "nameserver #{address}\n" }.join + "options timeout:2 attempts:1\n") }
        @tun
      end
      def clear
        @rules.each do |family|
          command('ip', "-#{family}", 'rule', 'del', 'priority', PRIORITY.to_s, 'lookup', TABLE.to_s) rescue nil
          command('ip', "-#{family}", 'route', 'flush', 'table', TABLE.to_s)
        end
        @rules.clear
        @tun&.close
        @tun = nil
      end
    end
  end
end
