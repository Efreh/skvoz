# frozen_string_literal: true
require 'socket'
require_relative 'destination'

module Skvoz
  module Server
    class Policy
      NON_PUBLIC = %w[0.0.0.0/8 10.0.0.0/8 100.64.0.0/10 127.0.0.0/8 169.254.0.0/16 172.16.0.0/12 192.0.0.0/24 192.0.2.0/24 192.88.99.0/24 192.168.0.0/16 198.18.0.0/15 198.51.100.0/24 203.0.113.0/24 224.0.0.0/4 240.0.0.0/4 ::/128 ::1/128 100::/64 2001::/23 2001:db8::/32 2002::/16 3fff::/20 fc00::/7 fe80::/10 ff00::/8].map { |cidr| IPAddr.new(cidr) }.freeze
      NEVER = %w[0.0.0.0/32 224.0.0.0/4 255.255.255.255/32 ::/128 ff00::/8].map { |cidr| IPAddr.new(cidr) }.freeze
      LOOPBACK_V4 = IPAddr.new('127.0.0.0/8')
      GLOBAL_V6 = IPAddr.new('2000::/3')

      def initialize(allow: [], deny: [], own_endpoints: [])
        @allow = allow.map do |rule|
          if rule.is_a?(String)
            [IPAddr.new(rule), nil]
          else
            unless rule.is_a?(Hash) && rule.keys.sort == %w[cidr ports] && rule['cidr'].is_a?(String) && rule['ports'].is_a?(Array) && rule['ports'].length.between?(1, 64) && rule['ports'].all? { |port| port.is_a?(Integer) && port.between?(1, 65_535) }
              raise Error, 'Invalid destination allow rule'
            end
            [IPAddr.new(rule['cidr']), rule['ports']]
          end
        end
        @deny = deny.map { |cidr| IPAddr.new(cidr) }
        @own_endpoints = own_endpoints.map { |host, port| [normalize(IPAddr.new(host)), port] }
        @own_addresses = Socket.ip_address_list.filter_map do |entry|
          next unless entry.ipv4? || entry.ipv6?
          # Interface scope names may contain hyphens; compare numeric addresses.
          normalize(IPAddr.new(entry.ip_address.split('%', 2).first))
        end
      end

      def normalize(ip) = ip.ipv4_mapped? ? ip.native : ip

      def allowed?(address, port)
        ip = normalize(IPAddr.new(address))
        return false if NEVER.any? { |network| network.include?(ip) } || @deny.any? { |network| network.include?(ip) }
        return false if @own_endpoints.any? { |host, service_port| port == service_port && (host == ip || (host.to_i.zero? && (@own_addresses.include?(ip) || (ip.ipv4? && LOOPBACK_V4.include?(ip))))) }
        return true if @allow.any? { |network, ports| network.include?(ip) && (ports.nil? || ports.include?(port)) }
        return false if @own_addresses.include?(ip) || NON_PUBLIC.any? { |network| network.include?(ip) }
        ip.ipv4? || GLOBAL_V6.include?(ip)
      end

      def check!(addresses, port)
        raise DestinationError, 'dns_failed' if addresses.empty?
        raise DestinationError, 'forbidden' unless addresses.all? { |address| allowed?(address, port) }
        addresses
      end
    end
  end
end
