# frozen_string_literal: true
require 'async/condition'
require 'async/semaphore'
require 'json'
require 'socket'
require 'io/wait'

module Skvoz
  module Client
    class Wire
      MAX = 32768
      def initialize(socket) = @socket = socket
      def read
        first, rights = receive(1, nil)
        deadline = monotonic + 5
        size = (first + exact(3, deadline)).unpack1('N')
        raise Error, 'invalid_control_length' unless size.between?(1, MAX)
        value = JSON.parse(exact(size, deadline), allow_duplicate_key: false, max_nesting: 32)
        raise Error, 'invalid_control_rights' unless value.is_a?(Hash) && value['fd_count'] == rights.size && rights.size <= 1
        [value, rights.first]
      rescue Exception
        rights&.each(&:close)
        raise
      end
      def write(value, fd = nil)
        body = JSON.generate(value)
        raise Error, 'control_record_oversized' unless body.bytesize.between?(1, MAX)
        bytes = [body.bytesize].pack('N') + body
        position = 0
        deadline = monotonic + 3
        while position < bytes.bytesize
          sent = if fd && position.zero?
            @socket.sendmsg_nonblock(bytes, 0, nil, Socket::AncillaryData.unix_rights(fd), exception: false)
          else
            @socket.write_nonblock(bytes.byteslice(position..), exception: false)
          end
          if sent == :wait_writable
            wait(false, deadline)
          else
            raise Error, 'control_closed' unless sent.positive?
            position += sent
          end
        end
      end
      def close = (@socket.close unless @socket.closed?)
      private
      def monotonic = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      def wait(read, deadline)
        remaining = deadline && deadline - monotonic
        raise Error, 'control_timeout' if remaining && remaining <= 0
        ready = read ? @socket.wait_readable(remaining) : @socket.wait_writable(remaining)
        raise Error, 'control_timeout' unless ready
      end
      def receive(size, deadline)
        loop do
          message = @socket.recvmsg_nonblock(size, 0, 256, scm_rights: true, exception: false)
          if message == :wait_readable
            wait(true, deadline)
            next
          end
          bytes, _, flags, *controls = message
          rights = controls.flat_map { |control| control.unix_rights || [] }
          unless flags & Socket::MSG_CTRUNC == 0 && controls.all? { |c| c.cmsg_is?(Socket::SOL_SOCKET, Socket::SCM_RIGHTS) } && rights.size <= 1 && !bytes.empty?
            rights.each(&:close)
            raise Error, 'invalid_control_frame'
          end
          return [bytes, rights]
        end
      end
      def exact(size, deadline)
        bytes = ''.b
        while bytes.bytesize < size
          part, rights = receive(size - bytes.bytesize, deadline)
          unless rights.empty?
            rights.each(&:close)
            raise Error, 'misplaced_control_rights'
          end
          bytes << part
        end
        bytes
      end
    end

    class Control
      attr_reader :failure, :counters
      def initialize(socket, task)
        @wire, @id, @seq, @callers = Wire.new(socket), 0, 0, 0
        @commands, @changed = Async::Semaphore.new(1), Async::Condition.new
        @events, @counters = [], {}
        @reader = task.async { reader }
      end
      def call(op, args = {}, fd: nil, timeout: 15)
        raise Error, 'control_overloaded' if @callers >= 16
        @callers += 1
        admitted = true
        Async::Task.current.with_timeout(timeout) do
          @commands.acquire do
            raise Error, @failure if @failure
            raise Error, 'control_id_exhausted' if @id >= 2147483647
            @pending = (@id += 1)
            @wire.write({ 'v' => 1, 'id' => @id, 'op' => op, 'args' => args, 'fd_count' => fd ? 1 : 0 }, fd)
            @changed.wait until @reply || @failure
            raise Error, @failure if @failure
            value, descriptor = @reply
            @reply = nil
            if value['error']
              descriptor&.close
              raise Error, value.fetch('error')
            end
            [value.fetch('result'), descriptor]
          ensure
            @pending = nil
          end
        end
      rescue Async::TimeoutError
        stop
        raise Error, 'control_timeout'
      ensure
        @callers -= 1 if admitted
      end
      def event(name)
        Async::Task.current.with_timeout(20) do
          loop do
            raise Error, @failure if @failure
            index = @events.index { |value| value['event'] == name }
            return @events.delete_at(index).fetch('data') if index
            @changed.wait
          end
        end
      end
      def open(host, port)
        result, fd = call('OPEN_TCP', { 'host' => host, 'port' => port })
        raise Error, 'invalid_tcp_result' unless result.is_a?(Hash) && result.keys == ['handle'] && fd
        UNIXSocket.for_fd(fd.fileno).tap { fd.autoclose = false }
      rescue Exception
        fd&.close
        raise
      end
      def stop
        @failure ||= 'control_closed'
        @wire.close rescue IOError
        @reader.stop if @reader && @reader != Async::Task.current && @reader.alive?
        @reply&.last&.close
        @reply = nil
        @changed.signal
      end
      private
      def reader
        fd = nil
        loop do
          value, fd = @wire.read
          raise Error, 'invalid_control_version' unless value['v'].is_a?(Integer) && value['v'] == 1
          if value.key?('id')
            valid = value.keys.sort == %w[error fd_count id result v] && value['id'].is_a?(Integer) && value['id'] == @pending && !@reply && value['result'].nil? != value['error'].nil?
            raise Error, 'invalid_control_reply' unless valid
            @reply = [value, fd]
            fd = nil
          else
            valid = value.keys.sort == %w[data event fd_count seq v] && !fd && value['seq'].is_a?(Integer) && value['seq'] > @seq && value['seq'] < 1 << 63 && value['data'].is_a?(Hash)
            raise Error, 'invalid_control_event' unless valid
            @seq = value['seq']
            case value['event']
            when 'STATS' then @counters = value['data'].fetch('counters')
            when 'REQUEST' then nil
            when 'RUNTIME_STATE', 'CONFIGURED', 'ACTIVE', 'CLOSED'
              @events.reject! { |event| event['event'] == 'RUNTIME_STATE' } if value['event'] == 'RUNTIME_STATE'
              raise Error, 'control_event_overflow' if @events.size >= 128
              @events << value
            else raise Error, 'unknown_control_event'
            end
          end
          @changed.signal
        end
      rescue StandardError
        fd&.close
        @failure ||= 'control_failed'
        stop
      end
    end
  end
end
