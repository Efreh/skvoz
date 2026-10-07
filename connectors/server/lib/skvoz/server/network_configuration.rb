# frozen_string_literal: true
require 'json'
require 'ipaddr'
require 'socket'
require 'resolv'
require 'timeout'

module Skvoz
  module Server
    # Produces the same immutable public policy for the runtime and root helper.
    class NetworkConfiguration
      LIMITS = { ip_sessions: 128, core_streams: 2048, streams_per_peer: 512, lease_identities: 4096,
        receive_window: 33554432, max_frame: 16384, core_receive_bytes: 134217728, core_receive_peer_bytes: 33554432,
        core_send_bytes: 67108864, core_send_peer_bytes: 2097152, packet_queue_bytes: 262144,
        packet_queue_records: 256, control_queue_bytes: 32768, control_queue_records: 8,
        runtime_buffer_bytes: 536870912, runtime_buffer_records: 131072, api_queue_bytes: 131072,
        api_queue_records: 128, subscription_frames: 64, join_frames: 32, client_frames: 16,
        core_shards: 8, flow_buckets: 1024, setup_timeout_ms: 15000, teardown_timeout_ms: 3000,
        packet_io_timeout_ms: 1000 }.transform_keys(&:to_s).freeze
      DEFAULTS = { 'ipv4' => nil, 'ipv6' => nil, 'dns_servers' => [], 'service_prefixes' => [],
        'server_addresses' => [], 'management_endpoints' => [], 'max_mtu' => 1500, 'channels' => 1 }.freeze
      attr_reader :network, :server

      def initialize(config)
        input = config['network']
        raise Error, 'Invalid network configuration' unless input.is_a?(Hash) && (input.keys - DEFAULTS.keys).empty?
        value = DEFAULTS.merge(input)
        families = []
        [4, 6].each do |family|
          backend = value["ipv#{family}"]
          next if backend.nil?
          raise Error, 'Invalid IP backend' unless backend.is_a?(Hash) && backend.keys.sort == %w[egress interface pool]
          bits = prefix(backend['pool'], family)
          valid = family == 4 ? (16..24).cover?(bits) && %w[nat44 routed].include?(backend['egress']) : bits == 64 && backend['egress'] == 'routed'
          raise Error, 'Invalid IP backend' unless valid && backend['interface'].is_a?(String) && backend['interface'].match?(/\A[a-zA-Z0-9_.-]{1,15}\z/)
          families << family
        end
        mtu, channels = value.values_at('max_mtu', 'channels')
        raise Error, 'Invalid network limits' unless mtu.is_a?(Integer) && (576..1500).cover?(mtu) && (!families.include?(6) || mtu >= 1280) && channels.is_a?(Integer) && (1..8).cover?(channels)
        dns = value['dns_servers']
        raise Error, 'Invalid DNS inventory' unless dns.is_a?(Array) && dns.size <= 4 && dns.empty? == families.empty? && dns.all? { |address| families.include?(numeric(address).ipv4? ? 4 : 6) }
        rules = %w[allow deny].to_h { |key| [key, config[key].map { |rule| policy_rule(rule) }] }
        addresses = value['server_addresses']
        endpoints = value['management_endpoints']
        raise Error, 'Invalid server inventory' unless addresses.is_a?(Array) && endpoints.is_a?(Array) && addresses.size <= 32 && endpoints.size <= 32
        raise Error, 'Duplicate server inventory' unless addresses.uniq == addresses && endpoints.uniq == endpoints
        addresses.each { |address| numeric(address) }
        endpoints.each do |endpoint|
          raise Error, 'Invalid management endpoint' unless endpoint.is_a?(Hash) && endpoint.keys.sort == %w[address port protocol] && [6, 17].include?(endpoint['protocol']) && endpoint['port'].is_a?(Integer) && (1..65535).cover?(endpoint['port'])
          numeric(endpoint['address'])
        end
        services = value['service_prefixes']
        raise Error, 'Invalid service prefixes' unless services.is_a?(Array) && services.size <= 128
        services.each do |service|
          raise Error, 'Invalid service prefix' unless service.is_a?(Hash) && service.keys.sort == %w[peer prefix] && service['peer'].is_a?(String) && service['peer'].match?(/\A[1-9][0-9]{0,19}\z/) && service['peer'].to_i < 1 << 64
          address = IPAddr.new(service['prefix'])
          raise Error, 'Unsupported service family' unless families.include?(address.ipv4? ? 4 : 6)
          prefix(service['prefix'], address.ipv4? ? 4 : 6)
        end
        @network = { 'families' => families, 'max_mtu' => mtu, 'channels' => channels, 'limits' => LIMITS }
        @server = { 'ipv4' => value['ipv4'], 'ipv6' => value['ipv6'], 'dns_servers' => dns,
          'allow' => rules['allow'], 'deny' => rules['deny'], 'service_prefixes' => services,
          'lease_store' => '/var/lib/skvoz-network/state/leases.json', 'server_addresses' => addresses.uniq,
          'management_endpoints' => endpoints.uniq }
      rescue IPAddr::InvalidAddressError, TypeError, ArgumentError
        raise Error, 'Invalid network configuration'
      end

      def ip? = !@network.fetch('families').empty?

      def inventory!(config)
        addresses = Socket.ip_address_list.filter_map do |entry|
          next unless entry.ipv4? || entry.ipv6?
          address = entry.ip_address.split('%', 2).first
          ip = IPAddr.new(address)
          ip.ipv4_mapped? ? ip.native.to_s : ip.to_s
        end
        advertised = begin
          [numeric(config['address']).to_s]
        rescue Error
          Timeout.timeout(3) { Resolv.getaddresses(config['address']) }
        end
        raise Error, 'Server address resolution exceeded limit' if advertised.empty? || advertised.size > 16
        advertised.each { |address| numeric(address) }
        @server['server_addresses'] = (@server['server_addresses'] + addresses + advertised).uniq
        endpoints = []
        addresses.each do |address|
          endpoints << { 'address' => address, 'protocol' => 6, 'port' => config['port'] }
          if config.tls['mode'] == 'acme' && (config.tls['challenge_host'] == '0.0.0.0' || config.tls['challenge_host'] == '::' || config.tls['challenge_host'] == address)
            endpoints << { 'address' => address, 'protocol' => 6, 'port' => config.tls['challenge_port'] }
          end
        end
        if config.tls['mode'] == 'acme' && !['0.0.0.0', '::'].include?(config.tls['challenge_host'])
          endpoints << { 'address' => numeric(config.tls['challenge_host']).to_s, 'protocol' => 6, 'port' => config.tls['challenge_port'] }
        end
        endpoints << { 'address' => '127.0.0.1', 'protocol' => 6, 'port' => config['monitor_port'] }
        advertised.each do |address|
          [config['port'], config['advertised_port']].uniq.each { |port| endpoints << { 'address' => address, 'protocol' => 6, 'port' => port } }
          if config.tls['mode'] == 'acme'
            [config.tls['challenge_port'], config.tls['challenge_public_port']].uniq.each { |port| endpoints << { 'address' => address, 'protocol' => 6, 'port' => port } }
          end
        end
        @server['management_endpoints'] = (@server['management_endpoints'] + endpoints).uniq
        raise Error, 'Management inventory exceeded limit' if @server['server_addresses'].size > 32 || @server['management_endpoints'].size > 32
        self
      end

      private

      def numeric(value)
        raise Error, 'Canonical numeric address required' unless value.is_a?(String) && !value.include?('%') && !value.include?('/')
        ip = IPAddr.new(value)
        raise Error, 'Canonical numeric address required' unless ip.to_s == value && !ip.ipv4_mapped?
        ip
      rescue IPAddr::InvalidAddressError
        raise Error, 'Canonical numeric address required'
      end

      def prefix(value, family)
        raise Error, 'Canonical prefix required' unless value.is_a?(String) && value.match?(/\A[^%\/]+\/[0-9]+\z/)
        ip = IPAddr.new(value)
        bits = value.split('/').last.to_i
        raise Error, 'Canonical prefix required' unless (ip.ipv4? ? 4 : 6) == family && "#{ip}/#{bits}" == value
        bits
      end

      def policy_rule(rule)
        raise Error, 'Invalid destination rule' unless rule.is_a?(Hash) && rule.keys.sort == %w[cidr ports protocols]
        ip = IPAddr.new(rule['cidr'])
        prefix(rule['cidr'], ip.ipv4? ? 4 : 6)
        protocols, ports = rule.values_at('protocols', 'ports')
        valid = protocols == 'any' && ports.nil?
        if protocols.is_a?(Array) && !protocols.empty? && protocols.size <= 256 && protocols.uniq == protocols && protocols.all? { |p| p.is_a?(Integer) && (0..255).cover?(p) }
          valid = ports.nil? || protocols.size == 1 && [6, 17].include?(protocols.first) && ports.is_a?(Array) && ports.size.between?(1, 64) && ports.uniq == ports && ports.all? { |p| p.is_a?(Integer) && (1..65535).cover?(p) }
        end
        raise Error, 'Invalid destination rule' unless valid
        rule
      end
    end
  end
end
