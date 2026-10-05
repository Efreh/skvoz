# frozen_string_literal: true
require 'json'
require 'time'

module Skvoz
  module Server
    module Diagnostics
      def self.error_fields(error)
        fields = { error_class: error.class.name, error_code: error_code(error) }
        fields[:errno] = error.errno if error.is_a?(SystemCallError)
        fields[:sites] = Array(error.backtrace_locations).filter_map do |location|
          next unless location.path.include?('/lib/skvoz/server/')
          "#{File.basename(location.path)}:#{location.lineno}"
        end.first(3)
        fields
      end

      def self.error_code(error)
        case error
        when Async::TimeoutError then 'deadline_exceeded'
        when ProtocolError then 'protocol_error'
        when SystemCallError then error.class.name.delete_prefix('Errno::').downcase
        when IOError then ['closed stream', 'stream closed in another thread'].include?(error.message) ? 'io_closed' : 'io_error'
        when Error then 'command_failed'
        else 'unexpected_error'
        end
      end

      # Callers supply only identifiers, counters and fixed classifications.
      # Exception messages, metadata, child output and configuration are excluded.
      def self.emit(event, **fields)
        warn JSON.generate({ time: Time.now.utc.iso8601(3), component: 'server', pid: Process.pid, event: }.merge(fields))
        true
      rescue IOError, SystemCallError
        # A failed diagnostic sink must not prevent transport cleanup.
        false
      end
    end
  end
end
