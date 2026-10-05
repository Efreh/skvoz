# frozen_string_literal: true
require 'json'
require 'open3'
require 'pathname'
require 'timeout'

module UbuntuSystem
  ROOT = Pathname.new(__dir__).join('../../../..').expand_path
  CLIENT = ENV.fetch('SKVOZ_TEST_CLIENT', ROOT.join('target/release/skvoz-client-test-driver').to_s)
  RUNTIME = ENV.fetch('SKVOZ_TEST_RUNTIME', ROOT.join('target/release/skvoz-network-runtime').to_s)

  class Application
    attr_reader :ready, :http, :socks, :process, :directory
    def initialize(directory, server, username: 'shared', password: 'process-test-password', ca: true, host: 'localhost', server_port: nil, http_port: nil, socks_port: nil, runtime: RUNTIME, wait_ready: true, request_log: true)
      @directory = Pathname.new(directory)
      @http, @socks = http_port || ServerSystem.free_port, socks_port || ServerSystem.free_port
      @stdin, @stdout, @stderr, @process = Open3.popen3(CLIENT)
      @buffer = ''.b
      @errors = ''.b
      @error_lock = Mutex.new
      @error_reader = Thread.new do
        begin
          loop do
            chunk = @stderr.readpartial(4096)
            @error_lock.synchronize do
              @errors << chunk
              @errors = @errors.byteslice(-8192, 8192) if @errors.bytesize > 8192
            end
          end
        rescue EOFError, IOError
          nil
        end
      end
      @stdin.puts(JSON.generate(directory: @directory.to_s, runtime:, host:, port: server_port || server.port,
                               username:, password:, ca_file: ca ? server.directory.join('ca.pem').to_s : '',
                               http_port: @http, socks_port: @socks, request_log:))
      @stdin.flush
      @ready = wait_ready ? event { |value| value['ready'] || value['failed'] } : nil
    rescue Exception
      close
      raise
    end

    def event(timeout: 35)
      deadline = ServerSystem.monotonic + timeout
      loop do
        unless @buffer.include?("\n")
          remaining = deadline - ServerSystem.monotonic
          raise Timeout::Error, "Application event deadline exceeded: #{diagnostics}" if remaining <= 0
          next unless IO.select([@stdout], nil, nil, [remaining, 0.5].min)
          @buffer << @stdout.readpartial(4096)
        end
        next unless @buffer.include?("\n")
        line, @buffer = @buffer.split("\n", 2)
        value = JSON.parse(line)
        @last_event = value.slice('state', 'error', 'failed', 'ready', 'peer_id', 'pid', 'info')
        return value if yield value
      end
    rescue EOFError
      @error_reader&.join(1)
      raise IOError, "Application exited before expected event: #{diagnostics}"
    end

    def diagnostics
      errors = @error_lock.synchronize { @errors.dup }
      "process_alive=#{@process.alive?}, last_event=#{JSON.generate(@last_event)}, stderr=#{errors.inspect}"
    end

    def command(command)
      @stdin.puts(JSON.generate(command:)); @stdin.flush
    end

    def info(timeout: 35)
      command('info')
      event(timeout:) { |value| value['info'] }
    end

    def close
      if @process&.alive?
        begin
          command('quit')
        rescue IOError, Errno::EPIPE
          nil
        end
        Timeout.timeout(12) { @process.join }
      end
    rescue Timeout::Error
      Process.kill('KILL', @process.pid) rescue Errno::ESRCH
      @process.join(5)
    ensure
      [@stdin, @stdout].compact.each { |io| io.close unless io.closed? }
      @error_reader&.join(1)
      @stderr.close if @stderr && !@stderr.closed?
    end
  end
end
