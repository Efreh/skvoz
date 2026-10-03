# frozen_string_literal: true
require 'async/semaphore'
require 'resolv'
require_relative 'ipc_session'
require_relative 'policy'

module Skvoz
  module Server
    class TCPConnector
      attr_reader :session, :streams, :event_budget, :failure

      def initialize(path:, policy:, limits: {})
        @path, @policy = path, policy
        @limits = { streams: 64, connecting: 8, tcp_buffer_bytes: 262_144, connect_timeout: 3, command_timeout: 5,
                    stream_frames: 128, stream_bytes: 2_097_152, event_frames: 8192, event_bytes: 134_217_728 }.merge(limits)
        @streams = {}
        @cleanups = {}
        @event_budget = Budget.new(count: @limits[:event_frames], bytes: @limits[:event_bytes])
        @connects = Async::Semaphore.new(@limits[:connecting])
        @running = false
      end

      def start(task)
        @task = task
        @running = true
        @session = IPCSession.new(@path, timeout: @limits[:command_timeout]) { |frame, error| dispatch(frame, error) }
        @session.start(task)
        raise Error, 'Configured streams exceed daemon admission' if @limits[:streams] > @session.capabilities[:streams]
        raise Error, 'Configured stream queue cannot hold receive window' if @limits[:stream_bytes] < @session.capabilities[:window]
        self
      rescue StandardError
        stop
        raise
      end

      def statistics
        { 'streams' => @streams.length, 'target_written_bytes' => @streams.values.sum(&:written_bytes), 'cleanup_actors' => @cleanups.length, 'event_frames' => @event_budget.count,
          'event_bytes' => @event_budget.bytes, 'peak_event_frames' => @event_budget.peak_count, 'peak_event_bytes' => @event_budget.peak_bytes }
      end

      def ready? = @running && @failure.nil? && @session && @session.error.nil?

      def stop
        @running = false
        @session&.stop
        @streams.values.each(&:stop)
        @streams.clear
        @cleanups.values.each { |task| task.stop if task&.alive? }
        @cleanups.clear
      end

      def retired(handle) = @streams.delete(handle)

      def close_handle(handle) = control(handle, 8)

      def control(handle, kind, payload = ''.b)
        return if @cleanups.key?(handle)
        raise Error, 'Host cleanup actor budget exceeded' if @cleanups.length >= @limits[:streams]
        @cleanups[handle] = nil
        task = @task.async do
          @session.check(@session.request(kind, handle, payload), allowed: [0, 4])
        rescue Error
          nil
        ensure
          @cleanups.delete(handle)
        end
        @cleanups[handle] = task if @cleanups.key?(handle)
      end

      def resolve(destination)
        addresses = []
        if destination.literal
          addresses << destination.literal.to_s
        else
          Resolv.each_address(destination.host) do |address|
            raise DestinationError, 'dns_failed' if addresses.length >= 128
            addresses << address
          end
        end
        @policy.check!(addresses.uniq, destination.port).first(8)
      rescue Resolv::ResolvError, SocketError
        raise DestinationError, 'dns_failed'
      end

      def connect(destination)
        socket = nil
        Async::Task.current.with_timeout(@limits[:connect_timeout]) do
          @connects.acquire do
            addresses = resolve(destination)
            addresses.each do |address|
              info = Addrinfo.tcp(address, destination.port)
              socket = Socket.new(info.afamily, Socket::SOCK_STREAM, 0)
              socket.setsockopt(Socket::SOL_SOCKET, Socket::SO_SNDBUF, @limits[:tcp_buffer_bytes])
              socket.setsockopt(Socket::SOL_SOCKET, Socket::SO_RCVBUF, @limits[:tcp_buffer_bytes])
              begin
                result = socket.connect_nonblock(info, exception: false)
                if result == :wait_writable
                  socket.wait_writable
                  errno = socket.getsockopt(Socket::SOL_SOCKET, Socket::SO_ERROR).int
                  raise SystemCallError.new('TCP connect failed', errno) unless errno.zero?
                end
                return socket
              rescue SystemCallError
                socket.close
                socket = nil
              end
            end
            raise DestinationError, 'refused'
          end
        end
      rescue Async::TimeoutError
        socket&.close
        raise DestinationError, 'connect_timeout'
      ensure
        socket.close if socket && $! && !socket.closed?
      end

      private

      def dispatch(frame, error)
        if error
          @failure ||= 'IPC unavailable'
          @running = false
          @streams.values.each(&:abort)
          return
        end
        if frame.kind == Protocol::INCOMING
          raise ProtocolError, 'Invalid incoming metadata' unless frame.payload.bytesize.between?(8, 520)
          raise ProtocolError, 'Duplicate incoming handle' if @streams.key?(frame.handle)
          if !@running || @streams.length >= @limits[:streams]
            control(frame.handle, 4, Protocol.metadata(v: 1, type: 'tcp', error: 'overloaded'))
          else
            peer = frame.payload.unpack1('Q>')
            stream = TCPStream.new(self, frame.handle, peer, frame.payload.byteslice(8..), @limits)
            @streams[frame.handle] = stream
            stream.start(@task)
          end
        else
          @streams[frame.handle]&.event(frame)
        end
      end

    end

    class TCPStream
      def initialize(connector, handle, peer, metadata, limits)
        @connector, @handle, @peer, @metadata, @limits = connector, handle, peer, metadata, limits
        @queue = Queue.new(count: limits[:stream_frames], bytes: limits[:stream_bytes], global: connector.event_budget)
        @writable = Async::Condition.new
        @writable_generation = 0
        @tasks = []
        @offset = 0
        @closed = false
        @accepted = false
        @opened = false
        @accept_metadata = Protocol.metadata(v: 1, type: 'tcp', status: 'connected')
      end

      def start(task)
        @parent = task
        @tasks << task.async { establish }
      end

      def event(frame)
        return if @closed
        if frame.kind == Protocol::OPENED
          unless @accepted && !@opened && frame.payload == @accept_metadata
            raise ProtocolError, 'Unexpected local acceptance event'
          end
          @opened = true
        elsif frame.kind == Protocol::WRITABLE
          raise ProtocolError, 'Invalid writable event' unless frame.payload.empty?
          @writable_generation += 1
          @writable.signal
        elsif frame.kind == Protocol::CLOSED
          raise ProtocolError, 'Invalid closed event' unless frame.payload.bytesize == 2
          reason = frame.payload.unpack1('n')
          unless reason == 1
            abort
            return
          end
          enqueue(frame)
        elsif frame.kind == Protocol::DATA || frame.kind == Protocol::REMOTE_FINISHED
          raise ProtocolError, 'Stream data before local acceptance event' unless @opened
          raise ProtocolError, 'Invalid DATA length' if frame.kind == Protocol::DATA && !frame.payload.bytesize.between?(9, Protocol::MAX_PAYLOAD)
          raise ProtocolError, 'Invalid FIN payload' if frame.kind == Protocol::REMOTE_FINISHED && !frame.payload.empty?
          enqueue(frame)
        else
          raise ProtocolError, 'Unexpected incoming stream event'
        end
      end

      def abort
        return if @closed
        @closed = true
        @socket&.close unless @socket&.closed?
        @queue.close
        @writable.signal
        @connector.retired(@handle)
        @tasks.each { |task| task.stop if task.alive? && task != Async::Task.current }
      end

      def written_bytes = @offset
      def stop = abort

      private

      def enqueue(frame)
        return if @queue.push(frame, bytes: frame.payload.bytesize + 36)
        warn "SKVOZ stream queue admission exceeded: frames=#{@queue.budget.count} bytes=#{@queue.budget.bytes}"
        abort
        @connector.close_handle(@handle)
      end

      def establish
        destination = Destination.new(@metadata)
        @socket = @connector.connect(destination)
        return @socket.close if @closed
        @accepted = true
        @connector.session.check(@connector.session.request(3, @handle, @accept_metadata))
        @tasks << @parent.async { write_target }
        read_target
      rescue DestinationError => error
        reject(error.code) unless @closed
        abort
      rescue StandardError => error
        warn(error.is_a?(Error) ? 'SKVOZ stream failed: ' + error.message : 'SKVOZ stream failed: ' + error.class.name)
        close_remote unless @closed
        abort
      ensure
        abort if @closed
      end

      def write_target
        while (entry = @queue.pop)
          frame, bytes = entry
          begin
            case frame.kind
            when Protocol::DATA
              offset = frame.payload.unpack1('Q>')
              raise ProtocolError, 'Noncontiguous stream DATA' unless offset == @offset
              payload = frame.payload.byteslice(8..)
              write_bytes(payload)
            when Protocol::REMOTE_FINISHED
              @socket.shutdown(Socket::SHUT_WR)
            when Protocol::CLOSED
              abort
              return
            end
          ensure
            @queue.release(bytes)
          end
        end
      rescue StandardError => error
        warn(error.is_a?(Error) ? 'SKVOZ stream failed: ' + error.message : 'SKVOZ stream failed: ' + error.class.name)
        close_remote unless @closed
        abort
      end

      def write_bytes(bytes)
        written = 0
        while written < bytes.bytesize
          count = @socket.write_nonblock(bytes.byteslice(written..), exception: false)
          if count == :wait_writable
            @socket.wait_writable
          else
            written += count
            @offset += count
            reply = @connector.session.request(6, @handle, [@offset].pack('Q>'))
            @connector.session.check(reply, allowed: [0, 4])
          end
        end
      end

      def read_target
        until @closed
          bytes = @socket.read_nonblock(32_768, exception: false)
          if bytes == :wait_readable
            @socket.wait_readable
          elsif bytes.nil?
            @connector.session.check(@connector.session.request(7, @handle), allowed: [0, 4])
            return
          else
            send_bytes(bytes)
          end
        end
      end

      def send_bytes(bytes)
        sent = 0
        while sent < bytes.bytesize && !@closed
          generation = @writable_generation
          reply = @connector.session.request(5, @handle, bytes.byteslice(sent..))
          @connector.session.check(reply, allowed: [0, 1, 4])
          return abort if reply.code == 4
          raise ProtocolError, 'Invalid SEND accepted prefix' if reply.value > bytes.bytesize - sent || (reply.code == 1 && reply.value != 0)
          sent += reply.value
          @writable.wait if sent < bytes.bytesize && reply.value.zero? && generation == @writable_generation && !@closed
        end
      end

      def reject(code)
        @connector.session.check(@connector.session.request(4, @handle, Protocol.metadata(v: 1, type: 'tcp', error: code)), allowed: [0, 4])
      rescue Error
        nil
      end

      def close_remote
        @connector.session.check(@connector.session.request(8, @handle), allowed: [0, 4])
      rescue Error
        nil
      end
    end
  end
end
