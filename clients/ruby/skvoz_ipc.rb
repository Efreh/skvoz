# frozen_string_literal: true
# Standard-library IPC v1 client; no native SKVOZ bindings required.
require 'socket'

module SkvozIPC
  RESPONSE = 0x8000
  INCOMING, OPENED, REJECTED, DATA, WRITABLE, REMOTE_FINISHED, CLOSED = (0x9001..0x9007).to_a

  def self.encode(kind, request, handle = 0, payload = ''.b)
    raise ArgumentError, 'IPC payload exceeds limit' if payload.bytesize > 65_536
    body = ['SKI1', 1, kind, request, handle >> 64, handle & ((1 << 64) - 1)].pack('a4nnQ>Q>Q>') + payload.b
    [body.bytesize].pack('N') + body
  end

  class Client
    attr_reader :socket, :events, :capabilities
    def initialize(path, acceptor: false)
      @socket = UNIXSocket.new(path)
      begin
        @sequence = 0
        @events = []
        @event_bytes = 0
        result = request(1, 0, [1, 1, acceptor ? 1 : 0].pack('nnC'))
        raise IOError, "IPC HELLO rejected: code #{result[0]}" unless result[0].zero?
        @capabilities = result[3]
      rescue Exception # Close the partially initialized session, then re-raise.
        @socket.close
        raise
      end
    end

    def close
      @socket.close
    end

    def exact(n)
      result = ''.b
      while result.bytesize < n
        raise IOError, 'IPC read deadline' unless IO.select([@socket], nil, nil, 10)
        part = @socket.readpartial(n - result.bytesize)
        result << part
      end
      result
    end

    def read
      n = exact(4).unpack1('N')
      raise IOError, 'invalid IPC frame size' unless (32..65_568).cover?(n)
      body = exact(n)
      magic, version, kind, request, high, low = body[0, 32].unpack('a4nnQ>Q>Q>')
      raise IOError, 'unsupported IPC frame' unless magic == 'SKI1' && version == 1
      raise IOError, 'unknown IPC kind' unless kind == RESPONSE || (INCOMING..CLOSED).cover?(kind)
      [kind, request, (high << 64) | low, body[32..]]
    end

    def request(kind, handle = 0, payload = ''.b)
      @sequence += 1
      bytes = SkvozIPC.encode(kind, @sequence, handle, payload)
      until bytes.empty?
        raise IOError, 'IPC write deadline' unless IO.select(nil, [@socket], nil, 10)
        n = @socket.write_nonblock(bytes, exception: false)
        bytes = bytes[n..] unless n == :wait_writable
      end
      loop do
        frame = read
        if frame[0] == RESPONSE
          raise IOError, 'unexpected response' unless frame[1] == @sequence && frame[3].bytesize >= 10
          code, value = frame[3][0, 10].unpack('nQ>')
          return [code, value, frame[2], frame[3][10..]]
        end
        raise IOError, 'unexpected event request' unless frame[1].zero?
        raise IOError, 'client event budget exceeded; drain events' if @events.length >= 4096 || @event_bytes + frame[3].bytesize > 8 * 1024 * 1024
        @events << frame
        @event_bytes += frame[3].bytesize
      end
    end

    def event
      if @events.empty?
        frame = read
        raise IOError, 'unexpected response' if frame[0] == RESPONSE || frame[1] != 0
        return frame
      end
      frame = @events.shift
      @event_bytes -= frame[3].bytesize
      frame
    end

    def consume(handle, end_offset)
      code = request(6, handle, [end_offset].pack('Q>'))[0]
      # Terminal CLOSED may retire the handle before the last acknowledgement.
      raise IOError, "consume rejected: code #{code}" unless [0, 4].include?(code)
      code
    end
  end
end
