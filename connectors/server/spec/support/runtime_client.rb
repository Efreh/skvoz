# frozen_string_literal: true
require 'json'
require 'socket'
require 'timeout'

module ServerSystem
  class RuntimeClient
    attr_reader :socket
    def initialize(socket)
      @socket, @id, @events = socket, 0, []
      @mutex = Mutex.new
    end

    def request(op, args = {})
      @mutex.synchronize do
        Timeout.timeout(15) do
          id = (@id += 1)
          bytes = JSON.generate(v: 1, id:, op:, args:, fd_count: 0)
          @socket.write([bytes.bytesize].pack('N') + bytes)
          loop do
            value, descriptors = read
            if value.key?('event')
              raise IOError, 'Unexpected event descriptors' unless descriptors.empty?
              raise IOError, 'Runtime event queue exceeded' if @events.size >= 128
              @events << value
              next
            end
            raise IOError, 'Unexpected response ID' unless value['id'] == id
            return [value, descriptors]
          end
        end
      end
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
    end

    private

    def exact(size, descriptors)
      bytes = ''.b
      while bytes.bytesize < size
        data, _, flags, *controls = @socket.recvmsg(size - bytes.bytesize, 0, 256, scm_rights: true)
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
