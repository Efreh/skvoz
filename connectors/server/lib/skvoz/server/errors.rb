# frozen_string_literal: true

module Skvoz
  module Server
    class Error < StandardError; end
    class ProtocolError < Error; end
    class DestinationError < Error
      attr_reader :code
      def initialize(code)
        @code = code
        super(code)
      end
    end

  end
end
