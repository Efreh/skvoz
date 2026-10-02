# frozen_string_literal: true
require 'async/condition'

module Skvoz
  module Server
    class Budget
      attr_reader :count, :bytes, :peak_count, :peak_bytes
      def initialize(count:, bytes:)
        @max_count, @max_bytes = count, bytes
        @count = @bytes = @peak_count = @peak_bytes = 0
      end

      def reserve(bytes)
        return false if @count >= @max_count || @bytes + bytes > @max_bytes
        @count += 1
        @bytes += bytes
        @peak_count = [@peak_count, @count].max
        @peak_bytes = [@peak_bytes, @bytes].max
        true
      end

      def release(bytes)
        @count -= 1
        @bytes -= bytes
        raise ProtocolError, 'Budget accounting underflow' if @count.negative? || @bytes.negative?
      end
    end

    class Queue
      def initialize(count:, bytes:, global: nil)
        @budget = Budget.new(count:, bytes:)
        @global = global
        @items = []
        @ready = Async::Condition.new
        @space = Async::Condition.new
        @closed = false
      end

      attr_reader :budget

      def push(item, bytes:)
        return false if @closed || !@budget.reserve(bytes)
        if @global && !@global.reserve(bytes)
          @budget.release(bytes)
          return false
        end
        @items << [item, bytes]
        @ready.signal
        true
      end

      def pop
        @ready.wait while @items.empty? && !@closed
        @items.shift
      end

      def release(bytes)
        @budget.release(bytes)
        @global&.release(bytes)
        @space.signal
      end

      def wait_for_space = @space.wait
      def wake = @ready.signal
      def empty? = @items.empty?

      def close
        @closed = true
        @items.each { |_item, bytes| release(bytes) }
        @items.clear
        @ready.signal
        @space.signal
      end
    end
  end
end
