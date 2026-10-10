# frozen_string_literal: true
require 'openssl'
require 'acme-client'
require 'faraday/net_http_persistent'
require 'timeout'
require 'securerandom'
require_relative 'process'

module Skvoz
  module Server
    module BrokerTLS
      def self.connect(raw, identity:, ca:)
        line = raw.gets("\r\n", 4096)
        raise Error, 'Invalid NATS greeting' unless line && line.start_with?('INFO ') && line.end_with?("\r\n")
        info = JSON.parse(line.delete_prefix('INFO '))
        raise Error, 'NATS TLS required' unless info.is_a?(Hash) && info['tls_required'] == true
        context = OpenSSL::SSL::SSLContext.new
        context.ca_file = ca if ca
        context.set_params(verify_mode: OpenSSL::SSL::VERIFY_PEER)
        tls = OpenSSL::SSL::SSLSocket.new(raw, context)
        tls.sync_close = true
        tls.hostname = identity
        tls.connect
        tls.post_connection_check(identity)
        tls
      rescue StandardError
        tls ? tls.close : raw.close
        raise
      end
    end

    class AcmeTransport < Acme::Client
      private

      def connection_for(url:, mode:)
        connection = super
        @prepared ||= {}
        unless @prepared[connection.object_id]
          raise Error, 'ACME endpoint budget exceeded' if @prepared.length >= 16
          connection.request(:retry, max: 2, interval: 0.2, backoff_factor: 2)
          connection.builder.adapter(:net_http_persistent, pool_size: 10) { |http| http.idle_timeout = 60 }
          @prepared[connection.object_id] = true
        end
        connection
      end
    end

    class ChallengeServer
      def initialize(host:, port:)
        @listener = TCPServer.new(host, port)
        @tokens = {}
        @mutex = Mutex.new
        @workers = []
        @thread = Thread.new { serve }
      end

      def add(token, content)
        raise Error, 'Invalid ACME challenge token' unless token.match?(/\A[a-zA-Z0-9_-]{1,256}\z/) && content.bytesize <= 1024
        @mutex.synchronize { @tokens[token] = content }
      end

      def remove(token) = @mutex.synchronize { @tokens.delete(token) }

      def close
        @listener.close
        @thread.join(1)
        @workers.each { |worker| worker.kill if worker.alive? }
        @workers.each(&:join)
      end

      private

      def serve
        loop do
          socket = @listener.accept
          @workers.reject! { |worker| !worker.alive? }
          if @workers.length >= 8
            socket.close
          else
            @workers << Thread.new(socket) { |client| respond(client) }
          end
        end
      rescue IOError, Errno::EBADF
        nil
      end

      def respond(socket)
        Timeout.timeout(5) do
          request = ''.b
          until request.include?("\r\n\r\n")
            raise Error, 'HTTP challenge header exceeds limit' if request.bytesize >= 4096
            request << socket.readpartial([1024, 4096 - request.bytesize].min)
          end
          method, path, version = request.lines.first.split
          token = path&.match(%r{\A/\.well-known/acme-challenge/([a-zA-Z0-9_-]{1,256})\z})&.captures&.first
          content = @mutex.synchronize { @tokens[token] } if method == 'GET' && version == 'HTTP/1.1'
          content ||= ''
          status = content.empty? ? '404 Not Found' : '200 OK'
          socket.write("HTTP/1.1 #{status}\r\nContent-Type: text/plain\r\nContent-Length: #{content.bytesize}\r\nConnection: close\r\n\r\n#{content}")
        end
      rescue StandardError
        nil
      ensure
        socket.close
      end
    end

    class TLSMaterial
      attr_reader :certificate, :key, :ca

      def initialize(certificate:, key:, ca: nil, identity:)
        @certificate, @key, @ca, @identity = certificate, key, ca, identity
        validate
      end

      def validate
        pem = PrivateFiles.read(@certificate, maximum: 1_048_576)
        certificates = pem.scan(/-----BEGIN CERTIFICATE-----.*?-----END CERTIFICATE-----/m).map { |part| OpenSSL::X509::Certificate.new(part) }
        raise Error, 'TLS certificate chain is empty' if certificates.empty?
        @leaf = certificates.shift
        key = OpenSSL::PKey.read(PrivateFiles.read(@key, maximum: 16_384))
        raise Error, 'TLS certificate key mismatch' unless @leaf.check_private_key(key)
        san = @leaf.extensions.find { |extension| extension.oid == 'subjectAltName' }
        raise Error, 'TLS certificate SAN required' unless san
        names = OpenSSL::ASN1.decode(OpenSSL::ASN1.decode(san.to_der).value.last.value).value
        literal = IPAddr.new(@identity) rescue nil
        matches = if literal
          names.any? { |name| name.tag == 7 && name.value == literal.hton }
        else
          names.any? { |name| name.tag == 2 } && OpenSSL::SSL.verify_certificate_identity(@leaf, @identity)
        end
        raise Error, 'TLS certificate identity mismatch' unless matches
        store = OpenSSL::X509::Store.new
        store.purpose = OpenSSL::X509::PURPOSE_SSL_SERVER
        @ca ? store.add_file(@ca) : store.set_default_paths
        raise Error, 'TLS certificate trust invalid' unless store.verify(@leaf, certificates)
        self
      rescue OpenSSL::OpenSSLError
        raise Error, 'TLS material invalid'
      end

      def expires_at = @leaf.not_after
      def renewal_due? = expires_at <= Time.now + 48 * 3600
      def valid? = @leaf.not_before <= Time.now && expires_at > Time.now
      def serial = @leaf.serial.to_s
    end

    # Private loopback TLS for egress-only nodes; no public ACME dependency.
    class LocalCertificateIssuer
      def self.issue(directory, identity)
        ca_path, key_path = File.join(directory, 'local-ca.pem'), File.join(directory, 'local-ca-key.pem')
        if File.exist?(ca_path) && File.exist?(key_path)
          ca = OpenSSL::X509::Certificate.new(PrivateFiles.read(ca_path))
          key = OpenSSL::PKey.read(PrivateFiles.read(key_path, maximum: 16384))
          raise Error, 'Invalid local issuer' unless ca.check_private_key(key) && ca.not_after > Time.now + 86400
        else
          key = OpenSSL::PKey::EC.generate('prime256v1')
          ca = certificate(identity: 'SKVOZ local node CA', key:, days: 3650)
          extensions = OpenSSL::X509::ExtensionFactory.new(ca, ca)
          ca.add_extension(extensions.create_extension('basicConstraints', 'CA:TRUE', true))
          ca.add_extension(extensions.create_extension('keyUsage', 'keyCertSign,cRLSign', true))
          ca.sign(key, OpenSSL::Digest.new('SHA256'))
          PrivateFiles.write(ca_path, ca.to_pem)
          PrivateFiles.write(key_path, key.private_to_pem)
        end
        leaf_key = OpenSSL::PKey::EC.generate('prime256v1')
        leaf = certificate(identity:, key: leaf_key, days: 30)
        leaf.issuer = ca.subject
        extensions = OpenSSL::X509::ExtensionFactory.new(ca, leaf)
        leaf.add_extension(extensions.create_extension('basicConstraints', 'CA:FALSE', true))
        leaf.add_extension(extensions.create_extension('keyUsage', 'digitalSignature', true))
        leaf.add_extension(extensions.create_extension('extendedKeyUsage', 'serverAuth'))
        san = begin
          IPAddr.new(identity)
          "IP:#{identity}"
        rescue IPAddr::InvalidAddressError
          "DNS:#{identity}"
        end
        leaf.add_extension(extensions.create_extension('subjectAltName', san))
        leaf.sign(key, OpenSSL::Digest.new('SHA256'))
        generation = File.join(directory, 'tls-' + SecureRandom.hex(8))
        PrivateFiles.create_directory(generation)
        material = { 'certificate' => File.join(generation, 'certificate.pem'), 'key' => File.join(generation, 'key.pem'), 'ca' => ca_path }
        PrivateFiles.write(material['certificate'], leaf.to_pem)
        PrivateFiles.write(material['key'], leaf_key.private_to_pem)
        material
      end

      def self.certificate(identity:, key:, days:)
        value = OpenSSL::X509::Certificate.new
        value.version, value.serial = 2, SecureRandom.random_number(1 << 128) + 1
        value.subject = OpenSSL::X509::Name.new([['CN', identity]])
        value.issuer, value.public_key = value.subject, key
        value.not_before, value.not_after = Time.now - 60, Time.now + days * 86400
        value
      end
    end

    class CertificateIssuer
      def initialize(config, state_dir)
        @config, @state_dir = config, state_dir
      end

      def issue
        raise Error, 'ACME terms agreement required' unless @config.fetch('terms_agreed', false)
        key_path = File.join(@state_dir, 'acme-account.key')
        PrivateFiles.write(key_path, OpenSSL::PKey::EC.generate('prime256v1').private_to_pem) unless File.exist?(key_path)
        account_key = OpenSSL::PKey.read(PrivateFiles.read(key_path, maximum: 16_384))
        options = { request: { timeout: 15, open_timeout: 10 } }
        options[:ssl] = { ca_file: @config['issuer_ca'] } if @config['issuer_ca']
        client = AcmeTransport.new(private_key: account_key, directory: @config.fetch('directory'), connection_options: options, bad_nonce_retry: 2)
        account_path = File.join(@state_dir, 'acme-account.json')
        if File.exist?(account_path)
          record = JSON.parse(PrivateFiles.read(account_path))
          raise Error, 'ACME account belongs to a different issuer' unless record['directory'] == @config.fetch('directory')
          client = AcmeTransport.new(private_key: account_key, kid: record.fetch('kid'), directory: @config.fetch('directory'), connection_options: options, bad_nonce_retry: 2)
        else
          account = client.new_account(contact: ["mailto:#{@config.fetch('email')}"], terms_of_service_agreed: true)
          PrivateFiles.write(account_path, JSON.generate(kid: account.kid, directory: @config.fetch('directory')))
        end
        identity = @config.fetch('identity')
        identifier = begin
          IPAddr.new(identity)
          { type: 'ip', value: identity }
        rescue IPAddr::InvalidAddressError
          { type: 'dns', value: identity }
        end
        raise Error, 'Required ACME profile unavailable' unless client.profiles&.key?('shortlived')
        server = ChallengeServer.new(host: @config.fetch('challenge_host', '0.0.0.0'), port: @config.fetch('challenge_port', 8080))
        deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + @config.fetch('order_timeout', 120)
        order = client.new_order(identifiers: [identifier], profile: 'shortlived')
        order.authorizations.each do |authorization|
          next if authorization.status == 'valid'
          challenge = authorization.http
          raise Error, 'HTTP01 challenge unavailable' unless challenge
          server.add(challenge.token, challenge.file_content)
          challenge.request_validation
          poll(deadline) { authorization.reload; authorization.status }
          server.remove(challenge.token)
        end
        leaf_key = OpenSSL::PKey::EC.generate('prime256v1')
        csr = certificate_request(identity, leaf_key)
        order.finalize(csr: csr)
        poll(deadline) { order.reload; order.status }
        generation = File.join(@state_dir, 'tls-' + SecureRandom.hex(8))
        PrivateFiles.create_directory(generation)
        cert_path, leaf_path = File.join(generation, 'certificate.pem'), File.join(generation, 'key.pem')
        PrivateFiles.write(cert_path, order.certificate)
        PrivateFiles.write(leaf_path, leaf_key.private_to_pem)
        TLSMaterial.new(certificate: cert_path, key: leaf_path, ca: @config['certificate_ca'], identity:)
        { 'certificate' => cert_path, 'key' => leaf_path, 'ca' => @config['certificate_ca'] }
      rescue Acme::Client::Error, Faraday::Error, OpenSSL::OpenSSLError
        raise Error, 'Certificate issuance failed'
      ensure
        server&.close
      end

      private

      def certificate_request(identity, key)
        csr = Acme::Client::CertificateRequest.new(names: [identity], private_key: key)
        literal = IPAddr.new(identity) rescue nil
        if literal
          csr.csr.subject = OpenSSL::X509::Name.new
          csr.csr.sign(key, OpenSSL::Digest::SHA256.new)
        end
        csr
      end

      def poll(deadline)
        loop do
          status = yield
          return if status == 'valid'
          raise Error, 'ACME order rejected' if %w[invalid expired revoked deactivated].include?(status)
          raise Error, 'ACME order deadline exceeded' if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline
          sleep 0.25
        end
      end
    end
  end
end
