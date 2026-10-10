# frozen_string_literal: true
require 'async/condition'
require 'async/semaphore'
require 'json'
require 'socket'
require 'io/wait'
require_relative 'errors'

module Skvoz
  module Server
    # Only control records cross Ruby. Rust owns sockets, packets and the helper.
    class RuntimeControl
      MAX_BODY = 32768
      COUNTERS = %w[tcp_open ip_sessions packet_in packet_out packet_dropped queue_bytes queue_records buffer_bytes buffer_records errors uploaded downloaded].freeze
      ERRORS = %w[unsupported_version invalid_request invalid_state unknown_handle forbidden overloaded local_setup_failed network_unavailable timeout closed].freeze
      LIFECYCLE = %w[starting ready closing closed].freeze
      attr_reader :failure

      def initialize(socket)
        @socket = socket
        @next_id = 0
        @pending = {}
        @commands = Async::Semaphore.new(1)
        @callers = 0
        @ready = false
        @closed = false
        @owner_eof = false
        @counters = {}
        @routing = { 'control_ready' => false, 'eligible_exits' => 0 }
        @seq = 0
      end

      def start(task)
        @reader = task.async { reader }
        hello = request('HELLO', { 'api' => 1, 'network' => 5 })
        raise Error, 'Incompatible runtime HELLO' unless hello.is_a?(Hash) && hello.keys.sort == %w[api capabilities network role] && hello['api'].is_a?(Integer) && hello['api'] == 1 && hello['network'].is_a?(Integer) && hello['network'] == 5 && hello['role'] == 'server' && capabilities?(hello['capabilities'])
        self
      rescue StandardError
        close
        raise
      end

      def request(operation, args = {}, timeout: 5)
        raise Error, 'Runtime control closed' if @closed
        raise Error, 'Runtime request budget exceeded' if @callers >= 8
        @callers += 1
        admitted = true
        Async::Task.current.with_timeout(timeout) do
          @commands.acquire do
            raise Error, 'Runtime control closed' if @closed
            raise Error, 'Runtime request ID exhausted' if @next_id >= 2147483647
            id = (@next_id += 1)
            pending = { condition: Async::Condition.new }
            @pending[id] = pending
            bytes = JSON.generate(v: 1, id:, op: operation, args:, fd_count: 0)
            raise Error, 'Runtime control record exceeds limit' if bytes.bytesize > MAX_BODY
            @socket.write([bytes.bytesize].pack('N') + bytes)
            pending[:condition].wait until pending[:response] || @closed
            response = pending[:response]
            raise Error, 'Runtime control unavailable' unless response
            raise Error, 'Runtime request rejected' unless response['error'].nil?
            response.fetch('result')
          ensure
            @pending.delete(id) if id
          end
        end
      rescue Async::TimeoutError
        close
        raise Error, 'Runtime request deadline exceeded'
      ensure
        @callers -= 1 if admitted
      end

      def ready? = @ready && !@closed
      def owner_eof? = @owner_eof
      def statistics = @counters.merge('routing' => @routing)

      def refresh
        value = request('STATUS')
        raise Error, 'Invalid runtime STATUS' unless value.is_a?(Hash) && value.keys.sort == %w[counters lifecycle mode routing session] && LIFECYCLE.include?(value['lifecycle']) && value['mode'] == 'server' && value['session'].nil?
        routing = value['routing']
        raise Error, 'Invalid runtime routing status' unless routing.is_a?(Hash) && routing.keys.sort == %w[control_ready eligible_exits] && [true, false].include?(routing['control_ready']) && routing['eligible_exits'].is_a?(Integer) && routing['eligible_exits'].between?(0, 8)
        @routing = routing
        @ready = value['lifecycle'] == 'ready'
        @counters = counters(value['counters'])
        value
      end

      def prepare_shutdown(timeout: 5)
        request('PREPARE_SHUTDOWN', {}, timeout:) unless @closed
      end

      def stop = close

      private

      def exact(size)
        bytes = ''.b
        while bytes.bytesize < size
          message = @socket.recvmsg_nonblock(size - bytes.bytesize, 0, 1024, scm_rights: true, exception: false)
          if message == :wait_readable
            @socket.wait_readable
            next
          end
          owner_eof! if message.nil?
          data, _address, flags, *rights = message
          received = rights.flat_map { |right| right.unix_rights || [] }
          received.each(&:close)
          raise Error, 'Unexpected runtime descriptors' unless received.empty? && rights.empty? && flags & Socket::MSG_CTRUNC == 0
          owner_eof! if data.empty?
          bytes << data
        end
        bytes
      end

      def owner_eof!
        @owner_eof = true
        raise Error, 'Runtime control EOF'
      end

      def reader
        loop do
          first = exact(1)
          value = Async::Task.current.with_timeout(5) do
            size = (first + exact(3)).unpack1('N')
            raise Error, 'Runtime record length invalid' unless size.between?(1, MAX_BODY)
            JSON.parse(exact(size), allow_duplicate_key: false)
          end
          raise Error, 'Invalid runtime envelope' unless value.is_a?(Hash) && value['v'].is_a?(Integer) && value['v'] == 1 && value['fd_count'].is_a?(Integer) && value['fd_count'] == 0
          if value.key?('id')
            raise Error, 'Invalid runtime response' unless value.keys.sort == %w[error fd_count id result v] && value['id'].is_a?(Integer) && @pending.key?(value['id']) && value['result'].nil? != value['error'].nil? && valid_error?(value['error'])
            pending = @pending.fetch(value['id'])
            raise Error, 'Duplicate runtime response' if pending[:response]
            pending[:response] = value
            pending[:condition].signal
          else
            raise Error, 'Invalid runtime event' unless value.keys.sort == %w[data event fd_count seq v] && value['seq'].is_a?(Integer) && value['seq'] > @seq && value['seq'] <= (1 << 63) - 1 && value['data'].is_a?(Hash)
            @seq = value['seq']
            case value['event']
            when 'RUNTIME_STATE'
              raise Error, 'Invalid runtime state event' unless value['data'].keys.sort == %w[error state] && LIFECYCLE.include?(value['data']['state']) && valid_error?(value['data']['error'])
              @ready = value['data']['state'] == 'ready'
              if value['data']['error'] && !@failure
                @failure = 'runtime_' + value['data']['error']
                Diagnostics.emit('runtime_state_failed', state: value['data']['state'], failure: @failure)
              end
            when 'STATS'
              raise Error, 'Invalid runtime statistics event' unless value['data'].keys == ['counters']
              @counters = counters(value['data'].fetch('counters'))
            else
              raise Error, 'Unexpected server runtime event'
            end
          end
        end
      rescue StandardError => error
        @failure ||= 'runtime_control_failed'
        Diagnostics.emit('runtime_control_failed', **Diagnostics.error_fields(error)) unless @closed
        close
      end

      def valid_error?(value) = value.nil? || ERRORS.include?(value)

      def capabilities?(value)
        return false unless value.is_a?(Hash) && value.keys.sort == %w[families max_channels max_mtu profiles]
        families = value['families']
        return false unless [[], [4], [6], [4, 6]].include?(families)
        expected = families.empty? ? ['tcp'] : ['tcp', 'ip']
        mtu = value['max_mtu']
        channels = value['max_channels']
        value['profiles'] == expected && mtu.is_a?(Integer) && (576..1500).cover?(mtu) &&
          (!families.include?(6) || mtu >= 1280) && channels.is_a?(Integer) && (1..8).cover?(channels)
      end

      def counters(value)
        raise Error, 'Invalid runtime counters' unless value.is_a?(Hash) && value.keys.sort == COUNTERS.sort && value.values.all? { |count| count.is_a?(Integer) && (0...(1 << 64)).cover?(count) }
        value
      end

      def close
        @closed = true
        @ready = false
        @socket.close unless @socket.closed?
        @reader.stop if @reader && @reader != Async::Task.current && @reader.alive?
        @pending.each_value { |pending| pending[:condition].signal }
      end
    end
  end
end
