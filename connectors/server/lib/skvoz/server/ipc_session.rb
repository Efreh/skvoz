# frozen_string_literal: true
require 'async'
require 'io/wait'
require_relative 'ipc_protocol'
require_relative 'budget'

module Skvoz
  module Server
    class IPCSession
      Request = Struct.new(:kind, :handle, :payload, :ready, :result, :error, :queued, :abandoned, :released)
      attr_reader :capabilities, :error

      def initialize(path, timeout: 5, commands: 128, command_bytes: 262_144, &events)
        @socket = UNIXSocket.new(path)
        @timeout = timeout
        @events = events
        @sequence = 0
        @pending = {}
        @outstanding = 0
        @data_outstanding = 0
        @requests = {}
        @max_outstanding = commands
        @slots = Async::Condition.new
        @work = Async::Condition.new
        @control = Queue.new(count: 16, bytes: 8192)
        @data = Queue.new(count: commands - 16, bytes: command_bytes - 8192)
        @tasks = []
        @closed = false
      end

      def start(task, acceptor: true)
        @tasks << task.async { reader }
        @tasks << task.async { writer }
        reply = request(1, 0, [1, 1, acceptor ? 1 : 0].pack('nnC'))
        check(reply)
        raise ProtocolError, 'Invalid IPC capabilities' unless reply.extra.bytesize == 46
        version, session, epoch, features, payload, metadata, window, streams, frames, bytes = reply.extra.unpack('nQ>Q>NNNNNNN')
        unless version == 1 && features & 15 == 15 && payload == 65_536 && metadata == 512 && window.positive? && streams.positive?
          raise ProtocolError, 'Incompatible IPC capabilities'
        end
        @capabilities = { session:, epoch:, window:, streams:, frames:, bytes: }.freeze
        self
      rescue StandardError
        close
        raise
      end

      def request(kind, handle = 0, payload = ''.b)
        raise Error, 'IPC session closed' if @closed
        raise ProtocolError, 'Host command payload exceeds limit' if payload.bytesize > 1024 && kind != 1
        item = Request.new(kind, handle, payload.b, Async::Condition.new)
        admitted = false
        Async::Task.current.with_timeout(@timeout) do
          @slots.wait while !@closed && (@outstanding >= @max_outstanding || (kind == 5 && @data_outstanding >= @max_outstanding - 16))
          raise Error, 'IPC session closed' if @closed
          @outstanding += 1
          @data_outstanding += 1 if kind == 5
          @requests[item.object_id] = item
          admitted = true
          queue = kind == 5 ? @data : @control
          until queue.push(item, bytes: payload.bytesize + 36)
            raise Error, 'IPC session closed' if @closed
            queue.wait_for_space
          end
          item.queued = true
          @work.signal
          item.ready.wait until item.result || item.error
          raise item.error if item.error
          item.result
        end
      rescue Async::TimeoutError
        fail_session(Error.new('IPC command deadline exceeded'))
        raise Error, 'IPC command deadline exceeded'
      ensure
        if admitted && !item.released
          item.abandoned = true
          release_request(item) unless item.queued
        end
      end

      def check(reply, allowed: [0])
        raise Error, "IPC command rejected: code #{reply.code}" unless allowed.include?(reply.code)
        reply
      end

      def status
        reply = check(request(9))
        raise ProtocolError, 'Invalid IPC status' unless reply.extra.bytesize == 97
        lifecycle, *values = reply.extra.unpack('CQ>12')
        raise ProtocolError, 'Invalid IPC lifecycle' unless (0..5).cover?(lifecycle)
        { lifecycle:, owners: values[0], streams: values[1], queued_frames: values[2], queued_bytes: values[3], runtime_streams: values[4] }
      end

      def close
        return if @closed
        @closed = true
        @socket.close unless @socket.closed?
        @control.close
        @data.close
        @requests.values.each do |item|
          item.error = @error || Error.new('IPC session closed')
          release_request(item)
          item.ready.signal
        end
        @pending.clear
        @work.signal
        @slots.signal
      end

      def stop
        close
        @tasks.each { |task| task.stop if task.alive? && task != Async::Task.current }
      end

      private

      def release_request(item)
        return if item.released
        item.released = true
        @requests.delete(item.object_id)
        @outstanding -= 1
        @data_outstanding -= 1 if item.kind == 5
        @slots.signal
      end

      def fail_session(error)
        @error ||= error
        close
        @events.call(nil, @error)
      end

      def exact(size)
        result = ''.b
        while result.bytesize < size
          part = @socket.read_nonblock(size - result.bytesize, exception: false)
          case part
          when :wait_readable then @socket.wait_readable
          when nil then raise Error, 'IPC connection closed'
          else result << part
          end
        end
        result
      end

      def reader
        until @closed
          size = exact(4).unpack1('N')
          raise ProtocolError, 'Invalid IPC frame length' unless (32..Protocol::MAX_BODY).cover?(size)
          frame = Protocol.decode(exact(size))
          if frame.kind == Protocol::RESPONSE
            item = @pending.delete(frame.request)
            raise ProtocolError, 'Unexpected IPC response' unless item
            item.result = Protocol.result(frame)
            release_request(item)
            item.ready.signal
          else
            raise ProtocolError, 'Invalid IPC event handle' if frame.handle.zero?
            @events.call(frame, nil)
          end
        end
      rescue StandardError => error
        fail_session(error) unless @closed
      end

      def writer
        until @closed
          @work.wait while @control.empty? && @data.empty? && !@closed
          break if @closed
          queue = @control.empty? ? @data : @control
          item, bytes = queue.pop
          begin
            if item.abandoned
              release_request(item)
              next
            end
            @sequence += 1
            raise ProtocolError, 'IPC request identity exhausted' if @sequence >= 1 << 64
            @pending[@sequence] = item
            frame = Protocol.encode(item.kind, @sequence, item.handle, item.payload)
            Async::Task.current.with_timeout(@timeout) { write(frame) }
          ensure
            queue.release(bytes)
          end
        end
      rescue StandardError => error
        fail_session(Error.new('IPC writer failed')) unless @closed
      end

      def write(bytes)
        offset = 0
        while offset < bytes.bytesize
          result = @socket.write_nonblock(bytes.byteslice(offset..), exception: false)
          result == :wait_writable ? @socket.wait_writable : offset += result
        end
      end
    end
  end
end
