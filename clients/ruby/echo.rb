#!/usr/bin/env ruby
# frozen_string_literal: true
# Send binary stdin to a provisioned peer and print its reverse reply.
require 'optparse'
require_relative 'skvoz_ipc'
options = {}
OptionParser.new do |parser|
  parser.on('--socket PATH') { |p| options[:socket] = p }
  parser.on('--peer ID', Integer) { |p| options[:peer] = p }
end.parse!
client = SkvozIPC::Client.new(options.fetch(:socket))
peer = [options.fetch(:peer)].pack('Q>')
deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 15
client.request(10, 0, peer)
until client.request(11, 0, peer)[3] == "\1".b
  raise IOError, 'peer readiness deadline' if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
  sleep 0.01
end
code, _, handle, = client.request(2, 0, peer + 'echo'.b)
raise IOError, "open rejected: code #{code}" unless code.zero?
pending = (STDIN.read(65_537) || '').b
raise IOError, 'example input limit is 65536 bytes' if pending.bytesize > 65_536
finished = false
expected_bytes = pending.bytesize
received_bytes = 0
loop do
  unless pending.empty?
    code, n, = client.request(5, handle, pending)
    raise IOError, "send rejected: code #{code}" unless [0, 1, 7].include?(code)
    pending = pending[n..]
  end
  if pending.empty? && !finished
    finished = client.request(7, handle)[0].zero?
  end
  if !client.events.empty? || IO.select([client.socket], nil, nil, 0.01)
    kind, _, key, payload = client.event
    raise IOError, 'unexpected stream' unless key == handle
    if kind == SkvozIPC::DATA
      received_bytes += payload.bytesize - 8
      offset = payload[0, 8].unpack1('Q>')
      STDOUT.binmode.write(payload[8..])
      STDOUT.flush
      client.consume(handle, offset + payload.bytesize - 8)
    end
    raise IOError, 'remote rejected stream' if kind == SkvozIPC::REJECTED
    if [SkvozIPC::REMOTE_FINISHED, SkvozIPC::CLOSED].include?(kind)
      reason = kind == SkvozIPC::CLOSED ? payload.unpack1('n') : 0
      raise IOError, "early reply end: reason #{reason}, received #{received_bytes}/#{expected_bytes}, pending #{pending.bytesize}" unless received_bytes == expected_bytes && pending.empty?
      break
    end
  end
  raise IOError, 'example transfer deadline' if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
end
client.close
