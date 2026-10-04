# frozen_string_literal: true

module Skvoz
  module Server
    class Error < StandardError; end
    class ProtocolError < Error; end
    class IPCCommandError < Error
      attr_reader :code
      def initialize(code)
        @code = code
        super("IPC command rejected: code #{code}")
      end
    end
    class DestinationError < Error
      attr_reader :code, :errno
      def initialize(code, errno: nil)
        @code = code
        @errno = errno
        super(code)
      end
    end

  end
end
