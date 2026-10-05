# frozen_string_literal: true

module Skvoz
  module Server
    class Error < StandardError; end
    class ProtocolError < Error; end
  end
end
