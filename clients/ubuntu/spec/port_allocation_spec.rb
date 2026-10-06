# frozen_string_literal: true
require_relative '../../../connectors/server/spec/spec_helper'
require_relative '../../../connectors/server/spec/support/system'
require_relative 'support/application'

RSpec.describe 'System fixture port allocation' do
  before do
    sockets = []
    begin
      8.times { sockets << TCPServer.open('127.0.0.1', 0) }
      @preferred_ports = sockets.map { |socket| socket.addr[1] }
    ensure
      sockets.each(&:close)
    end
    # Model a valid OS policy: immediately reuse the first available released port.
    allow(TCPServer).to receive(:open).and_wrap_original do |factory, host, port, &block|
      next factory.call(host, port, &block) unless host == '127.0.0.1' && port.zero?
      socket = nil
      @preferred_ports.each do |candidate|
        begin
          socket = factory.call(host, candidate)
          break
        rescue Errno::EADDRINUSE
          next
        end
      end
      socket ||= factory.call(host, port)
      next socket unless block
      begin
        block.call(socket)
      ensure
        socket.close
      end
    end
  end

  it 'returns distinct ports that are released for all four fixture listeners' do
    ports = ServerSystem.free_ports(4)
    expect(ports.uniq.length).to eq(4)
    listeners = []
    ports.each { |port| listeners << TCPServer.new('127.0.0.1', port) }
    expect(listeners.map { |socket| socket.addr[1] }).to eq(ports)
  ensure
    listeners&.each(&:close)
  end

  it 'excludes explicitly selected ports even when they are currently unbound' do
    ports = ServerSystem.free_ports(2, except: [@preferred_ports.first])
    expect(ports.uniq.length).to eq(2)
    expect(ports).not_to include(@preferred_ports.first)
  end

  it 'releases earlier reservations when a later socket allocation fails' do
    first = nil
    port = nil
    allow(TCPServer).to receive(:open).and_wrap_original do |factory, *arguments|
      raise IOError, 'Injected allocation failure' if first
      first = factory.call(*arguments)
      port = first.addr[1]
      first
    end
    expect { ServerSystem.free_ports(2) }.to raise_error(IOError, 'Injected allocation failure')
    listener = TCPServer.new('127.0.0.1', port)
  ensure
    listener&.close
    first&.close unless first&.closed?
  end

  it 'starts the real server and client when released ports are reused immediately', integration: true do
    Dir.mktmpdir('skvoz-reused-ports-') do |directory|
      directory = Pathname.new(directory)
      ServerSystem.certificates(directory)
      target = ServerSystem::Target.new { |socket| ServerSystem.after_fin(socket) }
      server = ServerSystem::Server.new(directory,
        allow: [{ 'cidr' => '127.0.0.1/32', 'protocols' => [6], 'ports' => [target.port] }])
      server.command('add', 'shared', 'process-test-password')
      app = UbuntuSystem::Application.new(directory.join('client'), server)
      expect(app.ready).to include('ready' => true)
      expect([server.port, server.monitor, app.http, app.socks].uniq.length).to eq(4)
      payload = "port-reuse\0\xff".b
      socket = nil
      Timeout.timeout(15) do
        socket = TCPSocket.new('127.0.0.1', app.http)
        socket.write("CONNECT 127.0.0.1:#{target.port} HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
        header = ''.b
        header << (socket.read(1) || raise(EOFError)) until header.end_with?("\r\n\r\n")
        expect(header).to include('200 Connection Established')
        socket.write(payload); socket.close_write
        expect(socket.read).to eq('after-fin:'.b + payload.reverse)
        socket.close
        socket = TCPSocket.new('127.0.0.1', app.socks)
        socket.write("\5\1\0".b)
        expect(socket.read(2)).to eq("\5\0".b)
        socket.write([5, 1, 0, 1, 127, 0, 0, 1, target.port].pack('C8n'))
        expect(socket.read(10)).to eq([5, 0, 0, 1, 0, 0, 0, 0, 0, 0].pack('C*'))
        socket.write(payload); socket.close_write
        expect(socket.read).to eq('after-fin:'.b + payload.reverse)
      end
    ensure
      socket&.close
      app&.close
      server&.close
      target&.close
    end
  end
end
