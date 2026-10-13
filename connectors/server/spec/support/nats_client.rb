# frozen_string_literal: true
require_relative '../../lib/skvoz/server/tls'
require 'timeout'

module ServerSystem
  class NatsClient
    attr_reader :socket

    def initialize(server, username, password)
      @socket = Skvoz::Server::BrokerTLS.connect(TCPSocket.new('127.0.0.1', server.port), identity: 'localhost', ca: server.directory.join('ca.pem').to_s)
      @socket.write('CONNECT ' + JSON.generate(user: username, pass: password, verbose: false, protocol: 1) + "\r\n")
      flush
    rescue Exception
      close
      raise
    end

    def flush
      Timeout.timeout(5) do
        @socket.write("PING\r\n")
        loop do
          line = @socket.gets("\r\n", 4096)
          raise IOError, 'Native client rejected or closed' if line.nil? || line.start_with?('-ERR')
          @socket.write("PONG\r\n") if line == "PING\r\n"
          return if line == "PONG\r\n"
        end
      end
    end

    def subscribe(subject)
      @socket.write("SUB #{subject} 1\r\n")
      flush
    end

    def closed?
      flush
      false
    rescue IOError, EOFError, SystemCallError, OpenSSL::SSL::SSLError
      true
    end

    def close
      @socket&.close unless @socket&.closed?
    rescue IOError
      nil
    end
  end
end
