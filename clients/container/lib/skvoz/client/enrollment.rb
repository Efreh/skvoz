# frozen_string_literal: true
require 'openssl'
require 'securerandom'
require 'json'
require 'socket'

module Skvoz
  module Client
    class Enrollment
      VERSION = { 'version' => '0.7.0', 'api' => 1, 'network' => 6, 'core' => '5.0.0' }.freeze
      class Authorization < Error; end
      def initialize(config, ip)
        @config, @ip = config, ip
      end
      def call(device)
        Async::Task.current.with_timeout(20) do
          [0, 1, 2].each_with_index do |delay, index|
            Async::Task.current.sleep(delay) if delay.positive?
            begin
              return attempt(device)
            rescue Authorization
              raise Terminal, 'authentication_failed' if index == 2
            end
          end
        end
      rescue OpenSSL::SSL::SSLError
        raise Terminal, 'certificate_failed'
      rescue Async::TimeoutError, IOError, SystemCallError, SocketError
        raise Error, 'server_unavailable'
      end
      private
      def line(socket)
        value = socket.gets("\r\n", 4097)
        raise Error, 'invalid_enrollment_frame' unless value && value.bytesize <= 4096 && value.end_with?("\r\n")
        raise Authorization if value.start_with?('-ERR') && value.downcase.include?('authorization')
        raise Terminal, 'enrollment_rejected' if value.start_with?('-ERR')
        value
      end
      def pong(socket)
        loop do
          value = line(socket)
          socket.write("PONG\r\n") if value == "PING\r\n"
          return if value == "PONG\r\n"
        end
      end
      def attempt(device)
        raw = Socket.tcp(@ip, @config.port, connect_timeout: 5)
        greeting = line(raw)
        info = JSON.parse(greeting.delete_prefix('INFO '), allow_duplicate_key: false, max_nesting: 16)
        raise Terminal, 'tls_required' unless greeting.start_with?('INFO ') && info['tls_required'] == true
        context = OpenSSL::SSL::SSLContext.new
        context.ca_file = @config.ca_file if @config.ca_file
        context.set_params(verify_mode: OpenSSL::SSL::VERIFY_PEER, verify_hostname: true)
        tls = OpenSSL::SSL::SSLSocket.new(raw, context)
        tls.sync_close = true
        tls.hostname = @config.host
        tls.connect
        tls.post_connection_check(@config.host)
        tls.write('CONNECT ' + JSON.generate(user: @config.username, pass: @config.password, verbose: false, protocol: 1, headers: true) + "\r\nPING\r\n")
        pong(tls)
        reply = "skvoz.enroll.reply.#{@config.username}.#{SecureRandom.hex(16)}"
        tls.write("SUB #{reply} 1\r\nPING\r\n")
        pong(tls)
        body = JSON.generate(v: 4, device:)
        tls.write("PUB skvoz.enroll.v4.#{@config.username} #{reply} #{body.bytesize}\r\n#{body}\r\n")
        loop do
          value = line(tls)
          if value == "PING\r\n"
            tls.write("PONG\r\n")
            next
          end
          next if value == "PONG\r\n" || value.start_with?('INFO ')
          fields = value.strip.split(' ')
          raise Error, 'invalid_enrollment_reply' unless fields.size == 4 && fields[0] == 'MSG' && fields[1] == reply && fields[2] == '1' && fields[3].match?(/\A[0-9]+\z/)
          size = Integer(fields[3], 10)
          raise Error, 'enrollment_reply_oversized' unless size.between?(1, 512)
          bytes = tls.read(size)
          raise Error, 'invalid_enrollment_reply' unless bytes&.bytesize == size && tls.read(2) == "\r\n"
          result = JSON.parse(bytes, allow_duplicate_key: false, max_nesting: 8)
          raise Terminal, 'enrollment_rejected' if result['error']
          valid = result.keys.sort == %w[namespace network_runtime peer_id v] && result['v'] == 4 && result['network_runtime'] == VERSION
          valid &&= result['peer_id'].is_a?(Integer) && result['peer_id'].between?(1, (1 << 63) - 1)
          namespace = result['namespace']
          valid &&= namespace.is_a?(String) && namespace.bytesize <= 256 && namespace.split('.', -1).all? { |part| part.match?(/\A[a-zA-Z0-9_-]{1,64}\z/) }
          raise Terminal, 'version_mismatch' unless valid
          return result
        end
      rescue JSON::ParserError
        raise Error, 'invalid_enrollment_reply'
      ensure
        tls&.close
        raw&.close unless raw&.closed?
      end
    end
  end
end
