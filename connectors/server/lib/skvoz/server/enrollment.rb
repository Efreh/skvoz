# frozen_string_literal: true
require 'async'
require 'async/semaphore'
require 'async/condition'
require 'openssl'
require 'json'
require_relative 'diagnostics'

module Skvoz
  module Server
    # NATS publish ACLs bind the literal request subject to its login. Reply
    # subjects are separately scoped to the same login; payload identity is absent.
    class Enrollment
      class Object < Hash
        def []=(key, value)
          raise Error, 'Duplicate enrollment field' if key?(key)
          super
        end
      end

      def initialize(config, state, &operation)
        @config, @state, @operation = config, state, operation
        @queue = []
        @work = Async::Condition.new
        @writes = Async::Semaphore.new(1)
        @tasks = []
        @ready = false
      end

      def start(task)
        task.with_timeout(5) do
          raw = TCPSocket.new('127.0.0.1', @config['port'])
          @socket = BrokerTLS.connect(raw, identity: @config['address'], ca: @state.value.fetch('tls')['ca'])
          send_bytes('CONNECT ' + JSON.generate(user: State::INTERNAL, pass: @state.value.fetch('internal_password'), verbose: false, protocol: 1) + "\r\nSUB skvoz.enroll.v3.* 1\r\nPING\r\n")
          loop do
            line = @socket.gets("\r\n", 4096)
            raise Error, 'Enrollment authentication failed' unless line && !line.start_with?('-ERR')
            break if line == "PONG\r\n"
          end
        end
        @ready = true
        @tasks << task.async { reader }
        @tasks << (@worker = task.async { worker })
        self
      rescue StandardError
        raw&.close unless @socket
        stop
        raise
      end

      def ready? = @ready

      def stop(graceful: false)
        @ready = false
        @socket&.close rescue IOError
        @queue.clear
        @work.signal
        @tasks.each do |task|
          next if task == Async::Task.current || graceful && task == @worker
          task.stop if task.alive?
        end
        # A responder-only reconnect must not cancel an in-flight ACL apply.
        # The worker has its own operation deadline and suppresses retired replies.
        @worker.wait if graceful && @worker && @worker != Async::Task.current
      end

      private

      def send_bytes(bytes)
        @writes.acquire { Async::Task.current.with_timeout(2) { @socket.write(bytes) } }
      end

      def respond(reply, result)
        bytes = JSON.generate(result)
        raise Error, 'Enrollment response exceeds limit' if bytes.bytesize > 512
        send_bytes("PUB #{reply} #{bytes.bytesize}\r\n" + bytes + "\r\n")
      end

      def exact(size)
        bytes = ''.b
        bytes << @socket.readpartial(size - bytes.bytesize) while bytes.bytesize < size
        bytes
      end

      def reader
        while @ready
          line = @socket.gets("\r\n", 4096)
          raise Error, 'Enrollment protocol closed' unless line && line.end_with?("\r\n")
          if line == "PING\r\n"
            send_bytes("PONG\r\n")
            next
          end
          next if line == "PONG\r\n" || line.start_with?('INFO ')
          parts = line.strip.split(' ')
          raise Error, 'Invalid enrollment broker frame' unless parts[0] == 'MSG' && [4, 5].include?(parts.length) && parts[2] == '1' && parts[-1].match?(/\A\d+\z/)
          size = Integer(parts[-1])
          raise Error, 'Invalid enrollment broker payload size' unless size.between?(0, 65_588)
          if size > 512
            remaining = size
            while remaining.positive?
              chunk = [remaining, 4096].min
              exact(chunk)
              remaining -= chunk
            end
            payload = nil
          else
            payload = exact(size)
          end
          raise Error, 'Invalid enrollment terminator' unless exact(2) == "\r\n"
          next unless parts.length == 5
          subject, reply = parts[1], parts[3]
          login = subject.delete_prefix('skvoz.enroll.v3.')
          next unless login.match?(State::LOGIN) && login != State::INTERNAL && subject == "skvoz.enroll.v3.#{login}"
          next unless reply.match?(/\Askvoz\.enroll\.reply\.#{Regexp.escape(login)}\.[0-9a-f]{32}\z/)
          if !payload
            respond(reply, { v: 3, error: 'invalid_request' })
          elsif @queue.length >= 16
            respond(reply, { v: 3, error: 'overloaded' })
          else
            @queue << [login, reply, payload]
            @work.signal
          end
        end
      rescue StandardError => error
        Diagnostics.emit('enrollment_unavailable', **Diagnostics.error_fields(error)) if @ready
        @ready = false
      end

      def worker
        while @ready
          @work.wait while @queue.empty? && @ready
          break unless @ready
          login, reply, payload = @queue.shift
          begin
          result = Async::Task.current.with_timeout(15) do
            request = JSON.parse(payload, object_class: Object)
            raise Error, 'Invalid enrollment request' unless request.is_a?(Hash) && request.keys.sort == %w[device v] && request['v'] == 3
            @operation.call(login, request.fetch('device'))
          end
          respond(reply, result) if @ready
          rescue JSON::ParserError, KeyError, Error, Async::TimeoutError => error
            code = error.message == 'Device pool exhausted' ? 'device_limit' : 'enrollment_failed'
            Diagnostics.emit('enrollment_failed', reason: code, **Diagnostics.error_fields(error))
            respond(reply, { v: 3, error: code }) if @ready
          end
        end
      rescue StandardError => error
        Diagnostics.emit('enrollment_unavailable', **Diagnostics.error_fields(error)) if @ready
        @ready = false
      end
    end
  end
end
