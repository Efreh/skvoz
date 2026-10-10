# frozen_string_literal: true
require 'uri'
require 'json'
require 'openssl'
require 'timeout'
require_relative 'process'

module Skvoz
  module Server
    # Private service credentials and public upstream trust, never client data.
    class NodeJoin
      KEYS = %w[v node_id identity username password namespace address port trust ca_pem network_runtime].freeze
      VERSIONS = { 'network' => 5, 'api' => 1, 'version' => '0.5.0', 'core' => '4.1.0' }.freeze
      attr_reader :value

      def initialize(value)
        raise Error, 'Invalid node join schema' unless value.is_a?(Hash) && value.keys.sort == KEYS.sort && value['v'] == 1 && value['network_runtime'] == VERSIONS
        id = value['node_id']
        raise Error, 'Invalid node identity' unless id.is_a?(Integer) && id > 1 << 63 && id < 1 << 64 && (id % 8).zero? && value['identity'].is_a?(String) && value['identity'].match?(/\A[0-9a-f]{32}\z/)
        raise Error, 'Invalid node credentials' unless value['username'] == "__skvoz_node_#{value['identity']}" && value['password'].is_a?(String) && value['password'].match?(/\A[a-zA-Z0-9_-]{43}\z/)
        raise Error, 'Invalid node namespace' unless value['namespace'].is_a?(String) && value['namespace'].bytesize <= 256 && value['namespace'].match?(/\A[a-zA-Z0-9_-]+(?:\.[a-zA-Z0-9_-]+)*\z/)
        raise Error, 'Invalid upstream endpoint' unless value['address'].is_a?(String) && value['port'].is_a?(Integer) && (1..65535).cover?(value['port'])
        uri = URI("tls://#{value['address'].include?(':') ? "[#{value['address']}]" : value['address']}:#{value['port']}")
        raise Error, 'Invalid upstream endpoint' unless uri.host && uri.userinfo.nil? && uri.path.empty? && uri.query.nil? && uri.fragment.nil? && value['address'].bytesize <= 253
        case value['trust']
        when 'system'
          raise Error, 'Unexpected node CA' unless value['ca_pem'].nil?
        when 'managed_ca'
          pem = value['ca_pem']
          raise Error, 'Invalid node CA' unless pem.is_a?(String) && pem.bytesize <= 65536
          certificate = OpenSSL::X509::Certificate.new(pem)
          raise Error, 'Expired node CA' unless certificate.not_before <= Time.now && certificate.not_after > Time.now
        else
          raise Error, 'Unsupported node trust'
        end
        @value = value
      rescue URI::InvalidURIError, OpenSSL::OpenSSLError
        raise Error, 'Invalid node join'
      end

      def self.read(path)
        bytes = if Process.euid.zero?
          # Bootstrap has SETUID/SETGID, but deliberately lacks DAC_OVERRIDE.
          # Read the single private input as its runtime owner; never relax the
          # shared private-file checks or copy the join credentials into state.
          reader, writer = IO.pipe
          pid = fork do
            reader.close
            Process.groups = []
            Process::GID.change_privilege(10001)
            Process::UID.change_privilege(10001)
            writer.write(PrivateFiles.read(path, maximum: 65536))
            writer.close
            exit! 0
          rescue StandardError
            exit! 1
          end
          writer.close
          input = Timeout.timeout(3) { reader.read(65537) }
          _, status = Process.waitpid2(pid)
          pid = nil
          raise Error, 'Private join input unavailable' unless status.success? && input.bytesize <= 65536
          input
        else
          PrivateFiles.read(path, maximum: 65536)
        end
        new(JSON.parse(bytes, allow_duplicate_key: false))
      ensure
        reader&.close unless reader&.closed?
        writer&.close unless writer&.closed?
        if pid
          Process.kill('KILL', pid) rescue Errno::ESRCH
          Process.waitpid(pid) rescue Errno::ECHILD
        end
      end

      def remote(ca_path:)
        value = @value
        host = value['address'].include?(':') ? "[#{value['address']}]" : value['address']
        tls = { 'handshake_first' => true }
        if value['ca_pem']
          PrivateFiles.write(ca_path, value['ca_pem'])
          tls['ca_file'] = ca_path
        end
        { 'url' => "nats-leaf://#{value['username']}:#{value['password']}@#{host}:#{value['port']}",
          'account' => 'APP', 'tls' => tls, 'compression' => 'off', 'ignore_discovered_servers' => true }
      end
    end
  end
end
