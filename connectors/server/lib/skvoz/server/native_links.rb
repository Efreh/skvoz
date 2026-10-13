# frozen_string_literal: true
require 'uri'
require 'json'
require 'openssl'
require 'timeout'
require_relative 'process'

module Skvoz
  module Server
    # Native transport configuration only; no peer IDs or application authority.
    class NativeLinks
      attr_reader :value

      def initialize(value)
        raise Error, 'Invalid native leaf configuration' unless value.is_a?(Hash) && value.keys.sort == %w[entries v] && value['v'] == 1 && value['entries'].is_a?(Array) && value['entries'].size.between?(1, 8)
        @value = value
        passwords = []
        value['entries'].each do |entry|
          raise Error, 'Invalid native leaf entry' unless entry.is_a?(Hash) && entry.keys.sort == %w[address ca_pem port users]
          host, port = entry.values_at('address', 'port')
          raise Error, 'Invalid native leaf endpoint' unless host.is_a?(String) && host.bytesize.between?(1, 253) && port.is_a?(Integer) && port.between?(1, 65_535)
          uri = URI("tls://#{host.include?(':') ? "[#{host}]" : host}:#{port}")
          raise Error, 'Invalid native leaf endpoint' unless uri.host && uri.userinfo.nil? && uri.path.empty? && uri.query.nil? && uri.fragment.nil?
          users = entry['users']
          raise Error, 'Missing native account credentials' unless users.is_a?(Hash) && users.keys.sort == %w[APP AUTH SYS]
          usernames = users.values.map { |credential| credential.is_a?(Hash) && credential['username'] }
          raise Error, 'Native leaf usernames shared between accounts' unless usernames.uniq == usernames
          users.each_value do |credential|
            raise Error, 'Invalid native leaf credential' unless credential.is_a?(Hash) && credential.keys.sort == %w[password username] && credential['username'].is_a?(String) && credential['username'].match?(/\A__skvoz_[a-zA-Z0-9_-]{1,64}\z/) && credential['password'].is_a?(String) && credential['password'].match?(/\A[a-zA-Z0-9_-]{43}\z/)
            passwords << credential['password']
          end
          if entry['ca_pem']
            pem = entry['ca_pem']
            raise Error, 'Invalid native leaf trust' unless pem.is_a?(String) && pem.bytesize <= 65_536
            certificate = OpenSSL::X509::Certificate.new(pem)
            raise Error, 'Expired native leaf trust' unless certificate.not_before <= Time.now && certificate.not_after > Time.now
          end
        end
        raise Error, 'Native leaf credentials shared' unless passwords.uniq == passwords
        endpoints = value['entries'].map { |entry| entry.values_at('address', 'port') }
        raise Error, 'Duplicate native leaf endpoint' unless endpoints.uniq == endpoints
      rescue URI::InvalidURIError, OpenSSL::OpenSSLError
        raise Error, 'Invalid native leaf configuration'
      end

      def self.read(path)
        raise Error, 'Native leaf path must be absolute' unless path.is_a?(String) && path.start_with?('/') && path.bytesize <= 4096 && !path.include?("\0")
        if Process.euid.zero?
          reader, writer = IO.pipe
          pid = fork do
            reader.close
            Process.groups = []
            Process::GID.change_privilege(10001)
            Process::UID.change_privilege(10001)
            writer.write(PrivateFiles.read(path, maximum: 524_288))
            writer.close
            exit! 0
          rescue StandardError
            exit! 1
          end
          writer.close
          bytes = Timeout.timeout(3) { reader.read(524_289) }
          _, status = Process.waitpid2(pid)
          pid = nil
          raise Error, 'Private native leaf input unavailable' unless status.success? && bytes.bytesize <= 524_288
        else
          bytes = PrivateFiles.read(path, maximum: 524_288)
        end
        new(JSON.parse(bytes, max_nesting: 8, allow_duplicate_key: false))
      ensure
        reader&.close unless reader&.closed?
        writer&.close unless writer&.closed?
        if pid
          Process.kill('KILL', pid) rescue Errno::ESRCH
          Process.waitpid(pid) rescue Errno::ECHILD
        end
      end

      def remotes(directory)
        value.fetch('entries').each_with_index.flat_map do |entry, index|
          tls = { 'handshake_first' => true }
          if entry['ca_pem']
            path = File.join(directory, "leaf-ca-#{index}.pem")
            PrivateFiles.write(path, entry['ca_pem'])
            tls['ca_file'] = path
          end
          host = entry['address'].include?(':') ? "[#{entry['address']}]" : entry['address']
          entry.fetch('users').map do |account, credential|
            { 'urls' => ["nats-leaf://#{credential['username']}:#{credential['password']}@#{host}:#{entry['port']}"],
              'account' => account, 'tls' => tls, 'compression' => 'off', 'ignore_discovered_servers' => true }
          end
        end
      end
    end
  end
end
