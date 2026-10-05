# frozen_string_literal: true
require_relative 'spec_helper'
require_relative 'support/system'

RSpec.describe 'Server deployment through Compose', compose: true do
  include ServerSystem

  def execute(*argv, timeout: 30, input: '', env: {})
    stdout, stderr, status = ServerSystem.capture(*argv, timeout:, input:, env:)
    raise IOError, "Command failed #{argv.first}: #{stderr}" unless status.success?
    stdout.strip
  end

  it 'reads installation settings from one YAML and publishes the advertised port' do
    compose = ENV['SKVOZ_TEST_COMPOSE'] ? [ENV.fetch('SKVOZ_TEST_COMPOSE')] : %w[docker compose]
    Dir.mktmpdir('skvoz-compose-settings-') do |directory|
      file = File.join(directory, 'compose.yaml')
      template = ServerSystem::COMPONENT.join('compose.yaml').read
      File.write(file, template.sub('&listen_port "4222"', '&listen_port "31422"'))
      config = JSON.parse(execute(*compose, '-p', 'skvoz-settings', '-f', file, 'config', '--format', 'json'))
      server = config.fetch('services').fetch('server')
      expect(server.fetch('environment').fetch('SKVOZ_ADVERTISED_PORT')).to eq('31422')
      expect(server.fetch('ports').find { |port| port.fetch('target') == 4222 }.fetch('published')).to eq('31422')
      expect(server.fetch('environment').fetch('SKVOZ_ACME_TERMS_AGREED')).to eq('false')
      expect(server.fetch('environment')).not_to have_key('SKVOZ_NETWORK')
    end
  end

  [false, true].each do |source|
    it "preserves users and recovers its process tree with #{source ? 'a local source build' : 'the prebuilt image'}" do
      image = ENV.fetch('SKVOZ_TEST_IMAGE', 'skvoz-server:qualification')
      compose = ENV['SKVOZ_TEST_COMPOSE'] ? [ENV.fetch('SKVOZ_TEST_COMPOSE')] : %w[docker compose]
      token = SecureRandom.hex(6)
      project, volume, helper, network_volume = %w[skvoz-spec skvoz-state skvoz-input skvoz-network-state].map { |prefix| "#{prefix}-#{token}" }
      target = ServerSystem::Target.new(host: '0.0.0.0') { |socket| after_fin(socket) }
      devices = []
      base = nil
      directory = nil
      begin
        execute('docker', 'volume', 'create', volume)
        execute('docker', 'volume', 'create', network_volume)
        directory = Pathname.new(Dir.mktmpdir('skvoz-compose-'))
        certificates(directory)
        gateway = JSON.parse(execute('docker', 'network', 'inspect', 'bridge')).first.fetch('IPAM').fetch('Config').first.fetch('Gateway')
        published_port = free_port
        private_json(directory.join('server.json'), {
          advertised_port: published_port, state_dir: '/var/lib/skvoz', address: 'localhost',
          **(source ? { network: { ipv4: { pool: '10.203.0.0/24', egress: 'nat44', interface: 'eth0' },
                                  dns_servers: ['1.1.1.1'] } } : {}),
          allow: [{ cidr: "#{gateway}/32", protocols: [6], ports: [target.port] }],
          tls: { mode: 'provided', certificate: '/var/lib/skvoz/input/server.pem',
                 key: '/var/lib/skvoz/input/server.key', ca: '/var/lib/skvoz/input/ca.pem' }
        })
        overlay = directory.join('qualification.yaml')
        overlay.write(<<~YAML)
          services:
            server:
              image: #{JSON.generate(image)}
              command: [serve, --config, /var/lib/skvoz-network/server.json]
              ports: !override ["127.0.0.1:#{published_port}:4222"]
              extra_hosts: ["host.docker.internal:host-gateway"]
          volumes:
            network-state:
              external: true
              name: #{network_volume}
            state:
              external: true
              name: #{volume}
        YAML
        base = compose + ['--project-name', project, '-f', ServerSystem::COMPONENT.join('compose.yaml').to_s]
        base += ['-f', ServerSystem::COMPONENT.join('compose.source.yaml').to_s] if source
        base += ['-f', overlay.to_s]
        command = ->(*arguments, timeout: 30) { execute(*base, *arguments, timeout:) }
        health = lambda do
          _, _, status = ServerSystem.capture(*base, 'exec', '-T', 'server', 'skvoz-server', 'health', timeout: 8)
          status.success?
        end
        admin = lambda do |operation, login = nil|
          argv = base + %w[exec -T server skvoz-server user] + [operation]
          argv << login if login
          JSON.parse(execute(*argv))
        end
        execute('docker', 'run', '-d', '--name', helper, '--user', '0', '--entrypoint', 'sleep',
                '-v', "#{volume}:/var/lib/skvoz", '-v', "#{network_volume}:/var/lib/skvoz-network", image, '120')
        execute('docker', 'exec', helper, 'mkdir', '-m', '0700', '/var/lib/skvoz/input')
        %w[server.pem server.key ca.pem].each do |name|
          execute('docker', 'cp', directory.join(name), "#{helper}:/var/lib/skvoz/input/#{name}")
        end
        execute('docker', 'exec', helper, 'chown', '-R', '10001:10001', '/var/lib/skvoz')
        execute('docker', 'exec', helper, 'mkdir', '-p', '/var/lib/skvoz-network')
        execute('docker', 'exec', helper, 'chmod', '0700', '/var/lib/skvoz-network')
        execute('docker', 'cp', directory.join('server.json'), "#{helper}:/var/lib/skvoz-network/server.json")
        execute('docker', 'exec', helper, 'chown', '0:0', '/var/lib/skvoz-network/server.json')
        execute('docker', 'exec', helper, 'chmod', '0600', '/var/lib/skvoz-network/server.json')
        execute('docker', 'rm', '-f', helper)
        command.call('config', '--quiet')
        command.call('build', 'server', timeout: 1800) if source
        command.call('up', '-d', '--pull', 'never')
        wait_until(timeout: 30, &health)
        cid = command.call('ps', '-q', 'server')
        links = JSON.parse(execute('docker', 'exec', cid, 'ip', '-d', '-j', 'address', 'show'))
        expect(links.find { |link| link.fetch('ifname') == 'skvoz0' }.fetch('ifalias')).to start_with('skvoz:')
        expect(execute('docker', 'exec', cid, 'nft', 'list', 'tables')).to include('table inet skvoz_network')
        port = command.call('port', 'server', '4222').split(':').last.to_i
        bundle = admin.call('add', 'shared')
        expect(bundle.fetch('port')).to eq(published_port)
        expect(port).to eq(published_port)
        device = ServerSystem::Device.new(directory.join('first'), bundle, port)
        devices << device
        expect(transfer(device.path, target.port, 'container', host: 'host.docker.internal')).to eq('after-fin:reniatnoc')
        users = admin.call('list')

        command.call('stop', '--timeout', '15')
        expect(JSON.parse(execute('docker', 'inspect', cid)).first.fetch('State').fetch('ExitCode')).to eq(0)
        command.call('up', '-d', '--pull', 'never')
        wait_until(timeout: 30, &health)
        wait_until { device_ready(device.path) }
        expect(admin.call('list')).to eq(users)
        expect(transfer(device.path, target.port, 'recreate', host: 'host.docker.internal')).to eq('after-fin:etaercer')

        old_pids = execute('docker', 'top', cid, '-eo', 'pid,comm').lines.drop(1).map { |line| Integer(line.split.first) }
        expect(old_pids).not_to be_empty
        execute('docker', 'exec', '--user', '10001:10001', cid, 'ruby', '-rjson', '-e',
                'owner = JSON.parse(File.read("/var/lib/skvoz/admin-owner.json")).fetch("pid"); children = File.read("/proc/#{owner}/task/#{owner}/children").split.map(&:to_i); runtime = children.find { |pid| File.read("/proc/#{pid}/comm").strip == "skvoz-network-r" }; abort "Runtime child missing" unless runtime && runtime != 1; Process.kill("KILL", runtime)')
        wait_until(timeout: 10) { old_pids.all? { |pid| process_dead(pid) } }
        wait_until(timeout: 30, &health)
        wait_until { device_ready(device.path) }
        expect(admin.call('list')).to eq(users)
        expect(transfer(device.path, target.port, 'kill', host: 'host.docker.internal')).to eq('after-fin:llik')
      rescue Exception
        if base
          stdout, stderr, = ServerSystem.capture(*base, 'logs', '--tail', '30')
          warn stdout + stderr
        end
        raise
      ensure
        original_error = $!
        cleanup_errors = []
        cleanups = devices.reverse.map { |device| -> { device.close } }
        cleanups << -> { execute(*base, 'down', '--timeout', '15') } if base
        cleanups << -> { ServerSystem.capture('docker', 'rm', '-f', helper, timeout: 10) }
        cleanups << -> { execute('docker', 'volume', 'rm', volume) }
        cleanups << -> { execute('docker', 'volume', 'rm', network_volume) }
        cleanups << -> { target.close }
        cleanups << -> { FileUtils.remove_entry(directory) } if directory
        cleanups.each do |cleanup|
          cleanup.call
        rescue StandardError => error
          cleanup_errors << error
        end
        raise cleanup_errors.first if !original_error && !cleanup_errors.empty?
      end
    end
  end
end
