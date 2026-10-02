# frozen_string_literal: true
require_relative 'spec_helper'

RSpec.describe Skvoz::Server::IPCSession do
  def exact(socket, count)
    result = ''.b
    result << socket.readpartial(count - result.bytesize) while result.bytesize < count
    result
  end

  def read_command(socket)
    length = exact(socket, 4).unpack1('N')
    body = exact(socket, length)
    _magic, _version, kind, request, high, low = body.unpack('a4nnQ>Q>Q>')
    [kind, request, (high << 64) | low, body.byteslice(32..)]
  end

  def respond(socket, command, value: 0, extra: ''.b)
    socket.write(Skvoz::Server::Protocol.encode(0x8000, command[1], command[2], [0, value].pack('nQ>') + extra))
  end

  def hello(socket)
    command = read_command(socket)
    expect(command[0]).to eq(1)
    capabilities = [1, 7, 9, 15, 65_536, 512, 8192, 64, 2048, 1_048_576].pack('nQ>Q>NNNNNNN')
    respond(socket, command, extra: capabilities)
  end

  it 'routes reversed concurrent replies without allowing multiple socket readers' do
    Dir.mktmpdir do |directory|
      path = File.join(directory, 'core.sock')
      listener = UNIXServer.new(path)
      Async do |root|
        server = root.async do
          socket = listener.accept
          hello(socket)
          commands = 12.times.map { read_command(socket) }
          commands.reverse_each { |command| respond(socket, command, value: command[2]) }
          socket.close
        end
        session = described_class.new(path) { |_frame, _error| nil }.start(root)
        requests = 12.times.map { |number| root.async { session.request(6, number + 1, [0].pack('Q>')) } }
        expect(requests.map(&:wait).map(&:value)).to eq((1..12).to_a)
        session.stop
        server.wait
      ensure
        listener.close
      end.wait
    end
  end

  it 'fails queued and pending requests immediately on owner shutdown' do
    Dir.mktmpdir do |directory|
      path = File.join(directory, 'core.sock')
      listener = UNIXServer.new(path)
      Async do |root|
        peer = nil
        server = root.async do
          peer = listener.accept
          hello(peer)
          root.sleep(30)
        end
        session = described_class.new(path, timeout: 10) { |_frame, _error| nil }.start(root)
        requests = 128.times.map do |number|
          root.async do
            session.request(number < 112 ? 5 : 8, number + 1, number < 112 ? 'x' * 1024 : ''.b)
          rescue Skvoz::Server::Error
            :closed
          end
        end
        root.sleep(0.05)
        started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        session.stop
        expect(requests.map(&:wait).uniq).to eq([:closed])
        expect(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).to be < 1
        server.stop
        peer.close
      ensure
        listener.close
      end.wait
    end
  end

  it 'reserves control admission while canceled SENDs retain ownership until replies' do
    Dir.mktmpdir do |directory|
      path = File.join(directory, 'core.sock')
      listener = UNIXServer.new(path)
      Async do |root|
        commands = []
        ready = Async::Condition.new
        socket = nil
        peer = root.async do
          socket = listener.accept
          hello(socket)
          loop do
            commands << read_command(socket)
            ready.signal
          end
        rescue EOFError, IOError
          nil
        end
        session = described_class.new(path, timeout: 10) { |_frame, _error| nil }.start(root)
        sends = 112.times.map { root.async { session.request(5, 1, 'x') } }
        ready.wait until commands.length == 112
        sends.each(&:stop)
        extra = root.async { session.request(5, 1, 'y') }
        control = root.async { session.request(8, 1) }
        ready.wait until commands.length == 113
        expect(commands.last[0]).to eq(8)
        respond(socket, commands.last)
        expect(control.wait.code).to eq(0)
        root.sleep(0.02)
        expect(commands.length).to eq(113)
        respond(socket, commands.first, value: 1)
        ready.wait until commands.length == 114
        expect(commands.last[0]).to eq(5)
        respond(socket, commands.last, value: 1)
        expect(extra.wait.value).to eq(1)
        session.stop
        peer.stop
        socket.close
      ensure
        listener.close
      end.wait
    end
  end
end
