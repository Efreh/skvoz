# frozen_string_literal: true
require 'yaml'
require 'ipaddr'
require 'socket'
require 'uri'

module Skvoz
  module Client
    class Error < StandardError; end
    class Terminal < Error; end
    class Config
      attr_reader :value, :host, :port, :proxy, :tcp, :http
      def initialize(text)
        raise Terminal, 'invalid_config' unless text.is_a?(String) && text.bytesize.between?(1, 65536)
        document = Psych.parse_stream(text)
        validate_nodes(document)
        @value = YAML.safe_load(text, permitted_classes: [], permitted_symbols: [], aliases: false)
        keys(@value, %w[server username password mode ca_file proxy tcp http families], %w[server username password])
        @value['mode'] ||= 'proxy'
        raise Terminal, 'invalid_mode' unless %w[proxy vpn].include?(mode)
        @host, @port = address(@value.fetch('server'))
        raise Terminal, 'invalid_login' unless username.is_a?(String) && username.match?(/\A[a-zA-Z0-9][a-zA-Z0-9_-]{0,31}\z/)
        raise Terminal, 'invalid_password' unless password.is_a?(String) && password.bytesize.between?(12, 72) && !password.include?("\0")
        file(@value['ca_file']) if @value.key?('ca_file')
        if mode == 'vpn'
          raise Terminal, 'incompatible_modes' unless (@value.keys & %w[proxy tcp http]).empty?
          raise Terminal, 'invalid_families' unless [[4], [6], [4, 6]].include?(families)
          @proxy, @tcp, @http = {}, {}, {}
        else
          raise Terminal, 'invalid_config' if @value.key?('families')
          setup_inputs
        end
      rescue Psych::Exception, URI::InvalidURIError, IPAddr::InvalidAddressError, ArgumentError, TypeError
        raise Terminal, 'invalid_config'
      end
      def mode = @value.fetch('mode')
      def username = @value.fetch('username')
      def password = @value.fetch('password')
      def ca_file = @value['ca_file']
      def families = @value.fetch('families', [4, 6])
      def address(input)
        raise Terminal, 'invalid_address' unless input.is_a?(String) && (match = input.match(/\A(\[[^\]]+\]|[^:\s\/]+):([0-9]{1,5})\z/))
        host = match[1].delete_prefix('[').delete_suffix(']')
        raise Terminal, 'invalid_host' unless host.bytesize.between?(1, 253) && (numeric?(host) || host.match?(/\A[a-zA-Z0-9](?:[a-zA-Z0-9.-]*[a-zA-Z0-9])?\z/))
        port = Integer(match[2], 10)
        raise Terminal, 'invalid_port' unless port.between?(1, 65535)
        [host, port]
      end
      def bind(input)
        input = "#{private_ip}:#{input}" if input.is_a?(Integer)
        host, port = address(input)
        raise Terminal, 'invalid_listener' unless numeric?(host) && private?(host)
        host.include?(':') ? "[#{host}]:#{port}" : "#{host}:#{port}"
      end
      private
      def validate_nodes(node)
        raise Terminal, 'invalid_yaml' if node.is_a?(Psych::Nodes::Alias)
        if node.is_a?(Psych::Nodes::Mapping)
          names = node.children.each_slice(2).map do |key, _|
            raise Terminal, 'invalid_yaml_key' unless key.is_a?(Psych::Nodes::Scalar)
            key.value
          end
          raise Terminal, 'duplicate_key' unless names.uniq == names
        end
        node.children&.each { |child| validate_nodes(child) } if node.respond_to?(:children)
      end
      def keys(value, allowed, required = [])
        raise Terminal, 'invalid_config_keys' unless value.is_a?(Hash) && (value.keys - allowed).empty? && (required - value.keys).empty?
      end
      def setup_inputs
        input = @value.fetch('proxy', { 'http' => 8080, 'socks' => 1080 })
        input = {} if input == false
        keys(input, %w[http socks])
        @proxy = input.transform_values { |endpoint| bind(endpoint) }
        @tcp = routes('tcp', %w[listen target]) do |route|
          host, port = address(route.fetch('target'))
          route.merge('host' => host, 'port' => port)
        end
        @http = routes('http', %w[listen upstream ca_file]) do |route|
          uri = URI.parse(route.fetch('upstream'))
          raise Terminal, 'invalid_upstream' unless %w[http https].include?(uri.scheme) && uri.host && !uri.userinfo && !uri.query && !uri.fragment && uri.port.between?(1, 65535)
          raise Terminal, 'invalid_upstream' if uri.path.include?("\0")
          file(route['ca_file']) if route.key?('ca_file')
          route.merge('uri' => uri)
        end
        raise Terminal, 'too_many_routes' if @tcp.size + @http.size > 32
        endpoints = @proxy.values + @tcp.values.map { |r| r['listen'] } + @http.values.map { |r| r['listen'] }
        raise Terminal, 'missing_inputs' if endpoints.empty?
        parsed = endpoints.map { |endpoint| address(endpoint) }
        raise Terminal, 'listener_conflict' unless parsed.uniq == parsed
      end
      def routes(key, allowed)
        inputs = @value.fetch(key, {})
        raise Terminal, 'invalid_routes' unless inputs.is_a?(Hash) && inputs.size <= 32
        inputs.to_h do |name, route|
          raise Terminal, 'invalid_route_name' unless name.is_a?(String) && name.match?(/\A[a-zA-Z0-9_-]{1,32}\z/)
          keys(route, allowed, allowed - ['ca_file'])
          [name, yield(route.merge('listen' => bind(route.fetch('listen'))))]
        end
      end
      def file(value)
        raise Terminal, 'invalid_ca_file' unless value.is_a?(String) && value.start_with?('/') && !value.include?("\0") && File.file?(value) && File.size(value) <= 1048576
      end
      def numeric?(value)
        IPAddr.new(value).to_s == value
      rescue IPAddr::InvalidAddressError
        false
      end
      def private?(value)
        ip = IPAddr.new(value)
        ip.loopback? || ip.private?
      end
      def private_ip
        @private_ip ||= Socket.ip_address_list.find { |ip| ip.ipv4? && private?(ip.ip_address) && !IPAddr.new(ip.ip_address).loopback? }&.ip_address || raise(Terminal, 'private_address_unavailable')
      end
    end
  end
end
