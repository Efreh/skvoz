# frozen_string_literal: true
require 'json'
require 'open3'
require 'pathname'
require 'timeout'

module UbuntuSystem
  ROOT = Pathname.new(__dir__).join('../../../..').expand_path
  CLIENT = ENV.fetch('SKVOZ_TEST_CLIENT', ROOT.join('target/release/skvoz-client-test-driver').to_s)
  CORE = ENV.fetch('SKVOZ_TEST_CORE', ROOT.join('target/release/skvoz-core-daemon').to_s)

  class Application
    attr_reader :ready, :http, :socks, :process, :directory
    def initialize(directory, server, username: 'shared', password: 'process-test-password', ca: true, host: 'localhost', server_port: nil, budgets: {}, http_port: nil, socks_port: nil, daemon: CORE, wait_ready: true, request_log: true)
      @directory = Pathname.new(directory)
      @http, @socks = http_port || ServerSystem.free_port, socks_port || ServerSystem.free_port
      @stdin, @stdout, @stderr, @process = Open3.popen3(CLIENT)
      @buffer = ''.b
      @errors = Thread.new do
        begin
          @stderr.read
        rescue IOError
          ''
        end
      end
      @stdin.puts(JSON.generate(directory: @directory.to_s, daemon:, host:, port: server_port || server.port,
                               username:, password:, ca_file: ca ? server.directory.join('ca.pem').to_s : '',
                               http_port: @http, socks_port: @socks, budgets:, request_log:))
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
          raise Timeout::Error, 'Application event deadline exceeded' if remaining <= 0
          next unless IO.select([@stdout], nil, nil, [remaining, 0.5].min)
          @buffer << @stdout.readpartial(4096)
        end
        next unless @buffer.include?("\n")
        line, @buffer = @buffer.split("\n", 2)
        value = JSON.parse(line)
        return value if yield value
      end
    rescue EOFError
      raise IOError, 'Application exited before expected event: ' + (@errors&.value || '')
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
      @stdin&.close; @stdout&.close
      @errors&.join(1)
      @stderr&.close
    rescue Timeout::Error
      Process.kill('KILL', @process.pid) rescue Errno::ESRCH
      @process.join(5)
    end
  end
end
