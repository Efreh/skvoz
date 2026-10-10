#!/usr/bin/env ruby
# frozen_string_literal: true
require 'json'
require 'socket'

if ARGV == ['--version']
  puts 'skvoz-network-runtime 0.5.0 network=5 api=1 core=4.1.0'
  exit
end

socket = UNIXSocket.for_fd(Integer(ARGV.fetch(ARGV.index('--control-fd') + 1)))
profile = ARGV.fetch(ARGV.index('--config') + 1)
pid_path = File.join(File.dirname(profile), 'control-owner.pid')
File.write(pid_path, Process.pid.to_s)
File.chmod(0o600, pid_path)
mode = File.basename($PROGRAM_NAME)
armed = false
Signal.trap('USR1') { armed = true }

begin
  loop do
    header = socket.read(4)
    raise EOFError unless header && header.bytesize == 4
    size = header.unpack1('N')
    raise IOError unless size.between?(1, 32768)
    request = JSON.parse(socket.read(size), allow_duplicate_key: false)
    if armed && request.fetch('op') == 'STATUS'
      if mode == 'malformed-runtime'
        bytes = '{"v":1,"v":1}'
        socket.write([bytes.bytesize].pack('N') + bytes)
      else
        socket.close
      end
      if mode == 'delayed-sigkill-runtime'
        sleep 0.1
        Process.kill('KILL', Process.pid)
      end
      raise EOFError
    end
    result = case request.fetch('op')
             when 'HELLO'
               { api: 1, network: 5, role: 'server', capabilities: { profiles: ['tcp'], families: [], max_mtu: 1500, max_channels: 1 } }
             when 'STATUS'
               names = %w[tcp_open ip_sessions packet_in packet_out packet_dropped queue_bytes queue_records buffer_bytes buffer_records errors uploaded downloaded]
               { lifecycle: 'ready', mode: 'server', session: nil, routing: { control_ready: true, eligible_exits: 1 }, counters: names.to_h { |name| [name, 0] } }
             when 'PREPARE_SHUTDOWN' then {}
             else raise IOError
             end
    bytes = JSON.generate(v: 1, id: request.fetch('id'), result:, error: nil, fd_count: 0)
    socket.write([bytes.bytesize].pack('N') + bytes)
  end
rescue EOFError, IOError, SystemCallError
  # A live owner must be stopped by supervision, independently of IPC EOF.
  sleep 60
end
