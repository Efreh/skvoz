# frozen_string_literal: true
require 'ipaddr'
require_relative 'ipc_protocol'

module Skvoz
  module Server
    class Destination
      attr_reader :host, :port, :literal
      def initialize(bytes)
        raise DestinationError, 'invalid_destination' if bytes.bytesize > Protocol::MAX_METADATA
        text = bytes.dup.force_encoding(Encoding::UTF_8)
        raise DestinationError, 'invalid_destination' unless text.valid_encoding?
        value = JSON.parse(text, allow_duplicate_key: false)
        unless value.is_a?(Hash) && value.keys.sort == %w[host port type v] && value['v'] == 1 && value['type'] == 'tcp'
          raise DestinationError, 'invalid_destination'
        end
        @host, @port = value.values_at('host', 'port')
        validate
      rescue JSON::ParserError
        raise DestinationError, 'invalid_destination'
      end

      private

      def validate
        unless @host.is_a?(String) && @host.ascii_only? && @host.bytesize.between?(1, 253) && @port.is_a?(Integer) && @port.between?(1, 65_535)
          raise DestinationError, 'invalid_destination'
        end
        raise DestinationError, 'invalid_destination' if @host.include?('%') || @host.include?('/') || @host.include?('[')
        begin
          @literal = IPAddr.new(@host)
          @host = @literal.to_s
        rescue IPAddr::InvalidAddressError
          raise DestinationError, 'invalid_destination' if @host.include?(':') || @host.match?(/\A[\d.]+\z/)
          labels = @host.delete_suffix('.').split('.', -1)
          unless labels.all? { |label| label.match?(/\A[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\z/) }
            raise DestinationError, 'invalid_destination'
          end
          @host = @host.delete_suffix('.').downcase
        end
      end
    end

  end
end
