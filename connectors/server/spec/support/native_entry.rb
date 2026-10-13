# frozen_string_literal: true
require_relative 'system'

module ServerSystem
  # Runs the public stock configuration without any SKVOZ payload process.
  class NativeEntry
    attr_reader :directory, :port, :leaf_port, :monitor, :log

    def initialize(directory, public_keys)
      @directory = Pathname.new(directory)
      @directory.mkdir(0o700)
      ServerSystem.certificates(@directory)
      @port, @leaf_port, @monitor = ServerSystem.free_ports(3)
      env = { 'SKVOZ_NATS_STATE' => @directory.to_s, 'SKVOZ_ADDRESS' => 'localhost',
        'SKVOZ_ISSUER' => public_keys.fetch('issuer'), 'SKVOZ_XKEY' => public_keys.fetch('xkey'),
        'SKVOZ_NAMESPACE' => public_keys.fetch('namespace'), 'SKVOZ_CLIENT_PORT' => @port.to_s,
        'SKVOZ_LEAF_PORT' => @leaf_port.to_s }
      output, error, status = ServerSystem.capture('bash', ROOT.join('deploy/nats/configure.sh'), env:)
      raise IOError, "Native entry configuration failed: #{output} #{error}" unless status.success?
      generation = @directory.join('tls/current')
      generation.mkdir
      FileUtils.cp(@directory.join('server.pem'), generation.join('certificate.pem'))
      FileUtils.cp(@directory.join('server.key'), generation.join('key.pem'))
      config = @directory.join('nats.conf')
      # Monitor is local-only; allocate it to avoid conflicts between test nodes.
      config.write(config.read.sub('http: "127.0.0.1:8222"', "http: \"127.0.0.1:#{@monitor}\""))
      output, error, status = ServerSystem.capture(NATS, '-t', '--config', config)
      raise IOError, "Native NATS rejected public template: #{output} #{error}" unless status.success?
      @log = @directory.join('nats.log')
      @output = File.open(@log, 'ab')
      @process = Process.spawn(NATS, '--config', config.to_s, out: @output, err: @output)
      ServerSystem.wait_until do
        raise IOError, "Native entry startup failed: #{@log.read}" if ServerSystem.process_dead(@process)
        monitor('/varz')['server_id']
      rescue Errno::ECONNREFUSED
        false
      end
    rescue Exception
      close
      raise
    end

    def monitor(path)
      http = Net::HTTP.new('127.0.0.1', @monitor, nil)
      http.open_timeout = http.read_timeout = 1
      body = http.get(path).body
      raise IOError, 'Native monitor body exceeds limit' if body.bytesize > 1_048_576
      JSON.parse(body, allow_duplicate_key: false)
    end

    def transport
      value = JSON.parse(@directory.join('leaf-remotes.json').read, allow_duplicate_key: false)
      value.fetch('entries').first['ca_pem'] = @directory.join('ca.pem').read
      value
    end

    def connections = monitor('/connz?auth=1&acc=APP&limit=1312').fetch('connections')

    def close
      ServerSystem.terminate(@process) if @process
      @process = nil
      @output&.close unless @output&.closed?
    end
  end
end
