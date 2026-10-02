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

  [false, true].each do |source|
    it "preserves users and recovers its process tree with #{source ? 'a local source build' : 'the prebuilt image'}" do
      image = ENV.fetch('SKVOZ_TEST_IMAGE', 'skvoz-server:qualification')
      compose = ENV['SKVOZ_TEST_COMPOSE'] ? [ENV.fetch('SKVOZ_TEST_COMPOSE')] : %w[docker compose]
      token = SecureRandom.hex(6)
      project, volume, helper = %w[skvoz-spec skvoz-state skvoz-input].map { |prefix| "#{prefix}-#{token}" }
      target = ServerSystem::Target.new(host: '0.0.0.0') { |socket| after_fin(socket) }
      devices = []
      base = nil
      directory = nil
      env = ENV.to_h.merge('SKVOZ_IMAGE' => image, 'SKVOZ_ADDRESS' => 'localhost',
                         'SKVOZ_ACME_EMAIL' => 'qualification@example.com', 'SKVOZ_ACME_TERMS_AGREED' => 'true')
      begin
        execute('docker', 'volume', 'create', volume)
        directory = Pathname.new(Dir.mktmpdir('skvoz-compose-'))
        certificates(directory)
        gateway = JSON.parse(execute('docker', 'network', 'inspect', 'bridge')).first.fetch('IPAM').fetch('Config').first.fetch('Gateway')
        published_port = free_port
        private_json(directory.join('server.json'), {
          advertised_port: published_port, state_dir: '/var/lib/skvoz', address: 'localhost',
          allow: [{ cidr: "#{gateway}/32", ports: [target.port] }],
          tls: { mode: 'provided', certificate: '/var/lib/skvoz/input/server.pem',
                 key: '/var/lib/skvoz/input/server.key', ca: '/var/lib/skvoz/input/ca.pem' }
        })
        overlay = directory.join('qualification.yaml')
        overlay.write(<<~YAML)
          services:
            server:
              image: #{JSON.generate(image)}
              command: [serve, --config, /var/lib/skvoz/input/server.json]
              ports: !override ["127.0.0.1:#{published_port}:4222"]
              extra_hosts: ["host.docker.internal:host-gateway"]
          volumes:
            state:
              external: true
              name: #{volume}
        YAML
        base = compose + ['--project-name', project, '-f', ServerSystem::COMPONENT.join('compose.yaml').to_s]
        base += ['-f', ServerSystem::COMPONENT.join('compose.source.yaml').to_s] if source
        base += ['-f', overlay.to_s]
        command = ->(*arguments, timeout: 30) { execute(*base, *arguments, timeout:, env:) }
        health = lambda do
          _, _, status = ServerSystem.capture(*base, 'exec', '-T', 'server', 'skvoz-server', 'health', timeout: 8, env:)
          status.success?
        end
        admin = lambda do |operation, login = nil|
          argv = base + %w[exec -T server skvoz-server user] + [operation]
          argv << login if login
          JSON.parse(execute(*argv, env:))
        end
        execute('docker', 'run', '-d', '--name', helper, '--user', '0', '--entrypoint', 'sleep',
                '-v', "#{volume}:/var/lib/skvoz", image, '120')
        execute('docker', 'exec', helper, 'mkdir', '-m', '0700', '/var/lib/skvoz/input')
        %w[server.pem server.key ca.pem server.json].each do |name|
          execute('docker', 'cp', directory.join(name), "#{helper}:/var/lib/skvoz/input/#{name}")
        end
        execute('docker', 'exec', helper, 'chown', '-R', '10001:10001', '/var/lib/skvoz')
        execute('docker', 'rm', '-f', helper)
        command.call('config', '--quiet')
        command.call('build', 'server', timeout: 1800) if source
        command.call('up', '-d', '--pull', 'never')
        wait_until(timeout: 30, &health)
        cid = command.call('ps', '-q', 'server')
        port = command.call('port', 'server', '4222').split(':').last.to_i
        bundle = admin.call('add', 'shared')
        expect(bundle.fetch('port')).to eq(published_port)
        expect(port).to eq(published_port)
        device = ServerSystem::Device.new(directory.join('first'), bundle, ServerSystem::CORE, port)
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
        execute('docker', 'exec', cid, 'ruby', '-rjson', '-e',
                'Process.kill("KILL", JSON.parse(File.read("/var/lib/skvoz/admin-owner.json")).fetch("pid"))')
        wait_until(timeout: 10) { old_pids.all? { |pid| process_dead(pid) } }
        wait_until(timeout: 30, &health)
        wait_until { device_ready(device.path) }
        expect(admin.call('list')).to eq(users)
        expect(transfer(device.path, target.port, 'kill', host: 'host.docker.internal')).to eq('after-fin:llik')
      rescue Exception
        if base
          stdout, stderr, = ServerSystem.capture(*base, 'logs', '--tail', '30', env:)
          warn stdout + stderr
        end
        raise
      ensure
        original_error = $!
        cleanup_errors = []
        cleanups = devices.reverse.map { |device| -> { device.close } }
        cleanups << -> { execute(*base, 'down', '--timeout', '15', env:) } if base
        cleanups << -> { ServerSystem.capture('docker', 'rm', '-f', helper, timeout: 10) }
        cleanups << -> { execute('docker', 'volume', 'rm', volume) }
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
