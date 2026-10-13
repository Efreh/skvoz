# frozen_string_literal: true
require 'async/condition'
require 'async/semaphore'
require 'json'
require_relative 'tls'
require_relative 'nats_auth'

module Skvoz
  module Server
    # Each connection authenticates one scoped native account identity.
    class NatsConnection
      attr_reader :failure, :server_id

      def initialize(config, state, kind, max_body: 16_384, max_publish: 16_384)
        @config, @state, @kind, @max_body, @max_publish = config, state, kind, max_body, max_publish
        @writes = Async::Semaphore.new(1)
        @handlers = {}
        @sid = 0
        @pong = 0
        @changed = Async::Condition.new
        @ready = false
      end

      def start(task)
        raw = nil
        @server_id = nil
        task.with_timeout(3) do
          raw = TCPSocket.new('127.0.0.1', @config['port'])
          @socket = BrokerTLS.connect(raw, identity: @config['address'], ca: @state.value.fetch('tls')['ca']) do |info|
            @server_id = info.fetch('server_id')
            begin
              NatsAuth.decode(@server_id, :server)
            rescue ArgumentError
              raise Error, 'Native service server identity invalid'
            end
          end
          credential = @state.service(@kind)
          write('CONNECT ' + JSON.generate(user: credential.fetch('username'), pass: credential.fetch('password'),
                                          headers: true, protocol: 1, verbose: false, echo: false) + "\r\nPING\r\n")
          loop do
            line = @socket.gets("\r\n", 4096)
            raise Error, 'Native service authentication failed' unless line && !line.start_with?('-ERR')
            if line.start_with?('INFO ')
              info = JSON.parse(line.delete_prefix('INFO '), max_nesting: 8, allow_duplicate_key: false)
              raise Error, 'Native service server identity changed' unless info.fetch('server_id') == @server_id
            end
            break if line == "PONG\r\n"
          end
          raise Error, 'Native service server identity absent' unless @server_id
        end
        @ready = true
        @reader = task.async { reader }
        self
      rescue StandardError
        raw&.close unless @socket
        stop
        raise
      end

      def ready? = @ready && @failure.nil?

      def subscribe(subject, &handler)
        validate_subject(subject, subscription: true)
        raise Error, 'Native subscription budget exceeded' if @handlers.size >= 16
        sid = (@sid += 1).to_s
        @handlers[sid] = handler
        write("SUB #{subject} #{sid}\r\n")
        flush
        sid
      end

      def unsubscribe(sid)
        write("UNSUB #{sid}\r\n") if ready?
        @handlers.delete(sid)
      end

      def publish(subject, body, reply: nil)
        validate_subject(subject)
        validate_subject(reply) if reply
        raise Error, 'Native publication exceeds limit' unless body.bytesize <= @max_publish
        write("PUB #{subject}#{reply ? " #{reply}" : ''} #{body.bytesize}\r\n" + body.b + "\r\n")
      end

      def flush
        previous = @pong
        Async::Task.current.with_timeout(2) do
          write("PING\r\n")
          @changed.wait while @pong == previous && ready?
          raise Error, 'Native service connection unavailable' unless ready?
        end
      end

      def query(subject, body, timeout: 1, collect: false)
        replies = []
        inbox = "_INBOX.skvoz.sys.#{SecureRandom.hex(16)}"
        changed = Async::Condition.new
        sid = subscribe(inbox) do |message|
          raise Error, 'Native reply count exceeded' if replies.size >= 32
          replies << JSON.parse(message.fetch(:data), max_nesting: 32, allow_duplicate_key: false)
          changed.signal
        end
        publish(subject, JSON.generate(body), reply: inbox)
        begin
          Async::Task.current.with_timeout(timeout) do
            changed.wait while ready? && (collect || replies.empty?)
            raise Error, 'Native service response unavailable' if replies.empty? || !ready?
          end
        rescue Async::TimeoutError
          raise unless collect && ready? && !replies.empty?
        end
        collect ? replies : replies.first
      ensure
        unsubscribe(sid) if sid
      end

      def stop
        @ready = false
        @socket&.close rescue IOError
        @changed.signal
        @reader.stop if @reader && @reader != Async::Task.current && @reader.alive?
      end

      private

      def validate_subject(subject, subscription: false)
        valid = subject.is_a?(String) && subject.bytesize.between?(1, 1024) && subject.ascii_only?
        tokens = valid ? subject.split('.', -1) : []
        valid &&= tokens.each_with_index.all? do |token, index|
          token.match?(/\A[a-zA-Z0-9_$-]+\z/) || subscription && (token == '*' || token == '>' && index == tokens.size - 1)
        end
        raise Error, 'Native subject invalid' unless valid
      end

      def write(bytes)
        @writes.acquire { Async::Task.current.with_timeout(2) { @socket.write(bytes) } }
      end

      def exact(size)
        bytes = ''.b
        bytes << @socket.readpartial(size - bytes.bytesize) while bytes.bytesize < size
        bytes
      end

      def reader
        while @ready
          line = @socket.gets("\r\n", 4096)
          raise Error, 'Native service frame invalid' unless line && line.end_with?("\r\n")
          case line
          when "PING\r\n" then write("PONG\r\n")
          when "PONG\r\n" then @pong += 1; @changed.signal
          when /\AINFO /, "+OK\r\n" then next
          when /\A-ERR/ then raise Error, 'Native service request rejected'
          else
            fields = line.strip.split(' ')
            header = fields[0] == 'HMSG'
            raise Error, 'Native message envelope invalid' unless %w[MSG HMSG].include?(fields[0]) && [header ? 5 : 4, header ? 6 : 5].include?(fields.size) && fields[-1].match?(/\A\d+\z/)
            size = Integer(fields[-1])
            head = header ? Integer(fields[-2]) : 0
            raise Error, 'Native message budget exceeded' unless size.between?(0, @max_body) && head.between?(0, [size, 4096].min)
            data = Async::Task.current.with_timeout(2) do
              body = exact(size)
              raise Error, 'Native message terminator invalid' unless exact(2) == "\r\n"
              body
            end
            headers = {}
            if header
              lines = data.byteslice(0, head).split("\r\n")
              raise Error, 'Native headers invalid' unless lines.shift == 'NATS/1.0'
              lines.each do |entry|
                key, value = entry.split(':', 2)
                raise Error, 'Native header invalid' unless value && !headers.key?(key)
                headers[key] = value.strip
              end
            end
            reply = fields.size == (header ? 6 : 5) ? fields[3] : nil
            @handlers[fields[2]]&.call(subject: fields[1], reply: reply, headers: headers, data: data.byteslice(head, size - head))
          end
        end
      rescue StandardError => error
        @failure = 'native_service_unavailable' if @ready
        Diagnostics.emit('native_service_unavailable', service: @kind, **Diagnostics.error_fields(error)) if @ready
        stop
      end
    end
  end
end
