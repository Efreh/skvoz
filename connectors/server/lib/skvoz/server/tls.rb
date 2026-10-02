# frozen_string_literal: true
require 'openssl'
require 'acme-client'
require 'faraday/net_http_persistent'
require 'timeout'
require 'securerandom'
require_relative 'process'

module Skvoz
  module Server
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
          raise Error, 'ACME issuer changed; explicit account migration required' unless record['directory'] == @config.fetch('directory')
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
        csr = Acme::Client::CertificateRequest.new(names: [identity], private_key: leaf_key)
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
