# frozen_string_literal: true
require 'json'
require_relative 'errors'

module Skvoz
  module Server
    module Protocol
      RESPONSE = 0x8000
      INCOMING, OPENED, REJECTED, DATA, WRITABLE, REMOTE_FINISHED, CLOSED = (0x9001..0x9007).to_a
      MAX_BODY = 65_568
      MAX_PAYLOAD = 65_536
      MAX_METADATA = 512
      Frame = Data.define(:kind, :request, :handle, :payload)
      Result = Data.define(:code, :value, :handle, :extra)

      def self.encode(kind, request, handle = 0, payload = ''.b)
        raise ProtocolError, 'IPC payload exceeds limit' if payload.bytesize > MAX_PAYLOAD
        body = ['SKI1', 1, kind, request, handle >> 64, handle & ((1 << 64) - 1)].pack('a4nnQ>Q>Q>') + payload.b
        [body.bytesize].pack('N') + body
      end

      def self.decode(body)
        raise ProtocolError, 'Invalid IPC body length' unless (32..MAX_BODY).cover?(body.bytesize)
        magic, version, kind, request, high, low = body.unpack('a4nnQ>Q>Q>')
        raise ProtocolError, 'Unsupported IPC header' unless magic == 'SKI1' && version == 1
        raise ProtocolError, 'Unsupported IPC event' unless kind == RESPONSE || (INCOMING..CLOSED).cover?(kind)
        raise ProtocolError, 'Invalid IPC request identity' unless kind == RESPONSE ? request.positive? : request.zero?
        Frame.new(kind, request, (high << 64) | low, body.byteslice(32..))
      end

      def self.result(frame)
        raise ProtocolError, 'Invalid IPC response' unless frame.kind == RESPONSE && frame.payload.bytesize >= 10
        code, value = frame.payload.unpack('nQ>')
        raise ProtocolError, 'Invalid IPC result code' unless (0..10).cover?(code)
        Result.new(code, value, frame.handle, frame.payload.byteslice(10..))
      end

      def self.metadata(value)
        bytes = JSON.generate(value)
        raise ProtocolError, 'Metadata exceeds limit' if bytes.bytesize > MAX_METADATA
        bytes.b
      end
    end

  end
end
