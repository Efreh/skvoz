# frozen_string_literal: true
require 'async/http/client'
require 'async/http/endpoint'
require 'async/http/protocol/http1'
require 'protocol/http/response'
require 'io/stream'

module Skvoz
  module Client
    module HeaderBudget
      def read_headers
        @header_bytes = 0
        @reading_headers = true
        Async::Task.current.with_timeout(10) { super }
      ensure
        @reading_headers = false
      end
      def read_line
        line = super
        if @reading_headers
          @header_bytes += line.bytesize + 2
          raise ::Protocol::HTTP::BadRequest, 'Header budget exceeded' if @header_bytes > 65536
        end
        line
      end
      def read_chunked_body(headers) = NoTrailers.new(self, headers)
    end
    class NoTrailers < ::Protocol::HTTP1::Body::Chunked
      private
      def read_trailer
        raise ::Protocol::HTTP::BadRequest, 'Trailers unsupported' unless @connection.read_line.empty?
      end
    end
    class HttpClient < Async::HTTP::Protocol::HTTP1::Client
      include HeaderBudget
      def read_response(*arguments) = Async::Task.current.with_timeout(15) { super }
    end
    class HttpServer < Async::HTTP::Protocol::HTTP1::Server
      include HeaderBudget
      def next_request = Async::Task.current.with_timeout(10) { super }
    end
    module Http1
      def self.client(peer, **options)
        peer.extend(WriteDeadline)
        HttpClient.new(::IO::Stream(peer), 'HTTP/1.1', **options)
      end
      def self.names = ['http/1.1']
    end
    module WriteDeadline
      def write(*args) = Async::Task.current.with_timeout(30) { super }
    end
    class StreamingBody < ::Protocol::HTTP::Body::Readable
      def initialize(body, request: false)
        @body, @request = body, request
      end
      def length = @body.length
      def empty? = @body.empty?
      def ready? = false
      def read
        chunk = @request ? Async::Task.current.with_timeout(30) { @body.read } : @body.read
        raise Error, 'body_chunk_oversized' if chunk && chunk.bytesize > 65536
        chunk
      end
      def close(error = nil) = @body.close(error)
    end
    class CoreEndpoint
      def initialize(adapters, host, port)
        @adapters, @host, @port = adapters, host, port
      end
      def connect
        socket = @adapters.open(@host, @port)
        return socket unless block_given?
        begin
          yield socket
        ensure
          socket.close
        end
      end
    end
    module SetupDeadline
      def connect(&block)
        socket = Async::Task.current.with_timeout(15) { super() }
        return socket unless block
        begin
          block.call(socket)
        ensure
          socket.close
        end
      end
    end
    class Adapters
      HOP = %w[connection keep-alive proxy-authenticate proxy-authorization te trailer transfer-encoding upgrade].freeze
      attr_reader :active
      def initialize(config, control, task)
        @config, @control, @task = config, control, task
        @listeners, @tasks, @connections, @clients = [], [], {}, {}
        @active, @opening = 0, 0
        config.tcp.each_value { |route| bind(route, :tcp) }
        config.http.each_value { |route| bind(route, :http) }
      rescue Exception
        stop
        raise
      end
      def start
        @listeners.each do |listener, route, kind|
          @tasks << @task.async do
            loop do
              socket = listener.accept
              if @active >= 128
                socket.close
                next
              end
              @active += 1
              child = @task.async do |connection_task|
                @connections[socket] = connection_task
                kind == :tcp ? relay(socket, route, connection_task) : http(socket, route)
              rescue StandardError
                nil
              ensure
                socket.close unless socket.closed?
                @connections.delete(socket)
                @active -= 1
              end
            end
          end
        end
      end
      def open(host, port)
        raise Error, 'handshake_overloaded' if @opening >= 16
        @opening += 1
        admitted = true
        Async::Task.current.with_timeout(15) { @control.open(host, port) }
      ensure
        @opening -= 1 if admitted
      end
      def stop
        @tasks.each { |task| task.stop if task.alive? }
        @listeners.each { |listener, _, _| listener.close unless listener.closed? }
        @connections.keys.each { |socket| socket.close unless socket.closed? }
        @connections.values.each { |task| task.stop if task.alive? }
        @clients.each_value { |client| client.close rescue IOError }
        @listeners.clear
        @tasks.clear
      end
      private
      def bind(route, kind)
        host, port = @config.address(route.fetch('listen'))
        @listeners << [TCPServer.new(host, port), route, kind]
      end
      def relay(socket, route, task)
        remote = open(route.fetch('host'), route.fetch('port'))
        copies = []
        [[socket, remote], [remote, socket]].each do |source, destination|
          copies << task.async do
            loop do
              bytes = source.readpartial(16384)
              Async::Task.current.with_timeout(30) { destination.write(bytes) }
            end
          rescue EOFError
            destination.close_write rescue IOError
          rescue StandardError => error
            copies.each { |copy| copy.stop if copy != Async::Task.current && copy.alive? }
            error
          end
        end
        copies.each do |copy|
          result = copy.wait
          raise result if result.is_a?(StandardError)
        end
      ensure
        copies&.each { |copy| copy.stop if copy.alive? }
        remote&.close
      end
      def filtered(headers)
        tokens = headers['connection'].to_s.downcase.split(',').map(&:strip)
        headers.each.reject { |name, _| (HOP + tokens + ['host']).include?(name.downcase) }
      end
      def upstream(route)
        @clients[route] ||= begin
          uri = route.fetch('uri')
          context = OpenSSL::SSL::SSLContext.new
          context.ca_file = route['ca_file'] if route['ca_file']
          context.set_params(verify_mode: OpenSSL::SSL::VERIFY_PEER, verify_hostname: true)
          endpoint = Async::HTTP::Endpoint.parse(uri.to_s, CoreEndpoint.new(self, uri.hostname, uri.port), protocol: Http1, ssl_context: context)
          endpoint.extend(SetupDeadline)
          Async::HTTP::Client.new(endpoint, retries: 0, limit: 16)
        end
      end
      def gateway(request, route)
        unless request.version == 'HTTP/1.1' && !request.scheme && !request.protocol && request.method != 'CONNECT' && request.path.start_with?('/') && !request.path.start_with?('//') && !request.headers['upgrade'] && !request.headers['trailer']
          return ::Protocol::HTTP::Response[400, { 'content-type' => 'text/plain' }, ['Unsupported HTTP request']]
        end
        uri = route.fetch('uri')
        path = uri.path.delete_suffix('/') + '/' + request.path.delete_prefix('/')
        body = request.body && StreamingBody.new(request.body, request: true)
        headers = ::Protocol::HTTP::Headers.new(filtered(request.headers))
        outgoing = ::Protocol::HTTP::Request.new(uri.scheme, nil, request.method, path, 'HTTP/1.1', headers, body)
        response = upstream(route).call(outgoing)
        if response.status == 101 || response.headers['upgrade'] || response.headers['trailer']
          response.close
          raise Error, 'unsupported_upstream_response'
        end
        tokens = response.headers['connection'].to_s.downcase.split(',').map(&:strip)
        (HOP + tokens).each { |name| response.headers.delete(name) }
        response.body = StreamingBody.new(response.body) if response.body
        response
      rescue StandardError
        request.body&.close rescue IOError
        ::Protocol::HTTP::Response[502, { 'content-type' => 'text/plain' }, ['Upstream unavailable']]
      end
      def http(socket, route)
        socket.extend(WriteDeadline)
        connection = HttpServer.new(::IO::Stream(socket), 'HTTP/1.1')
        connection.each do |request|
          if (expect = request.headers.delete('expect'))
            if expect.to_s.downcase == '100-continue'
              connection.write_interim_response(request.version, 100, ::Protocol::HTTP::Headers.new)
            else
              next ::Protocol::HTTP::Response[417, {}, ['Unsupported expectation']]
            end
          end
          gateway(request, route)
        end
      rescue ::Protocol::HTTP::BadRequest, Async::TimeoutError, IOError, SystemCallError
        nil
      ensure
        connection&.close
      end
    end
  end
end
