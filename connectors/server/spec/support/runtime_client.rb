# frozen_string_literal: true
require 'json'
require 'socket'
require 'timeout'

module ServerSystem
  class RuntimeClient
    attr_reader :socket
    def initialize(socket)
      @socket, @id, @events = socket, 0, []
      @mutex, @calls, @condition = Mutex.new, Mutex.new, ConditionVariable.new
      @pending, @response, @failure = nil, nil, nil
      @reader = Thread.new { receive }
    end

    def request(op, args = {})
      @calls.synchronize do
        Timeout.timeout(15) do
          id = @mutex.synchronize do
            raise @failure if @failure
            @pending = (@id += 1)
          end
          bytes = JSON.generate(v: 1, id:, op:, args:, fd_count: 0)
          @socket.write([bytes.bytesize].pack('N') + bytes)
          @mutex.synchronize do
            @condition.wait(@mutex) until @response || @failure
            raise @failure unless @response
            response, @response, @pending = @response, nil, nil
            response
          end
        end
      end
    rescue Timeout::Error
      close
      raise
    end

    def open(host, port)
      response, descriptors = request('OPEN_TCP', { host:, port: })
      if response['error']
        descriptors.each(&:close)
        raise IOError, response.fetch('error')
      end
      raise IOError, 'OPEN_TCP descriptor count invalid' unless descriptors.size == 1 && response['fd_count'] == 1
      UNIXSocket.for_fd(descriptors.first.fileno).tap { descriptors.first.autoclose = false }
    end

    def close
      @socket.close unless @socket.closed?
      @reader.join(1) unless @reader == Thread.current
      @mutex.synchronize do
        @response&.last&.each(&:close)
        @response = nil
      end
    end

    private

    def receive_event(value)
      return if value['event'] == 'STATS'
      # This adapter has consumed the event; retain only recent diagnostics.
      # Specialized fixtures can observe every event through this hook.
      @events.shift if @events.size == 128
      @events << value
    end

    def receive
      descriptors = []
      loop do
        value, descriptors = read
        @mutex.synchronize do
          if value.key?('event')
            raise IOError, 'Unexpected event descriptors' unless descriptors.empty?
            receive_event(value)
          else
            raise IOError, 'Unexpected response ID' unless value['id'] == @pending && @response.nil?
            @response = [value, descriptors]
            descriptors = []
          end
          @condition.broadcast
        end
      end
    rescue StandardError => error
      descriptors&.each(&:close)
      @mutex.synchronize do
        @failure = error
        @condition.broadcast
      end
    end

    def exact(size, descriptors)
      bytes = ''.b
      while bytes.bytesize < size
        message = @socket.recvmsg(size - bytes.bytesize, 0, 256, scm_rights: true)
        raise IOError, 'Runtime control EOF' if message.nil?
        data, _, flags, *controls = message
        controls.each { |control| descriptors.concat(control.unix_rights || []) }
        raise IOError, 'Runtime control truncated' unless flags & Socket::MSG_CTRUNC == 0
        raise IOError, 'Runtime control EOF' if data.empty?
        bytes << data
      end
      bytes
    end

    def read
      descriptors = []
      size = exact(4, descriptors).unpack1('N')
      raise IOError, 'Runtime control length invalid' unless size.between?(1, 32768)
      value = JSON.parse(exact(size, descriptors), allow_duplicate_key: false)
      raise IOError, 'Runtime control envelope invalid' unless value['v'] == 1 && value['fd_count'] == descriptors.size
      [value, descriptors]
    rescue Exception
      descriptors.each(&:close)
      raise
    end
  end
end
