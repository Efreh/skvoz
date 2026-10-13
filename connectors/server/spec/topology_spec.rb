# frozen_string_literal: true
require 'net/http'
require_relative 'spec_helper'
require_relative 'support/system'
require_relative 'support/native_entry'

RSpec.describe 'Shared local standalone and leaf topology', integration: true do
  def directory(root, name)
    path = File.join(root, name)
    Dir.mkdir(path, 0o700)
    path
  end

  def configured_server(root, name, **options)
    path = directory(root, name)
    ServerSystem.certificates(path)
    ServerSystem::Server.new(path, **options)
  end

  it 'serves standalone TCP through its local peer0 and completes native half-close' do
    Dir.mktmpdir('skvoz-standalone-') do |root|
      target = ServerSystem::Target.new { |socket| ServerSystem.after_fin(socket) }
      allow_rule = [{ 'cidr' => '127.0.0.0/8', 'protocols' => [6], 'ports' => [target.port] }]
      server = configured_server(root, 'server', allow: allow_rule)
      bundle = server.command('add', 'standalone', 'standalone secret password')
      device = ServerSystem::Device.new(File.join(root, 'device'), bundle)
      bytes = (0...65536).map { |n| (n % 251).chr }.join.b
      expect(ServerSystem.transfer(device.path, target.port, bytes)).to eq('after-fin:'.b + bytes.reverse)
      ServerSystem.wait_until { server.command('health').dig('connector', 'tcp_open').zero? }
    ensure
      device&.close
      server&.close
      target&.close
    end
  end

  it 'reaps the runtime cleanly when the owner closes immediately after shutdown acknowledgment' do
    Dir.mktmpdir('skvoz-owner-shutdown-') do |root|
      target = ServerSystem::Target.new { |socket| ServerSystem.after_fin(socket) }
      server = configured_server(root, 'server', allow: [{ 'cidr' => '127.0.0.0/8', 'protocols' => [6], 'ports' => [target.port] }])
      device = ServerSystem::Device.new(File.join(root, 'device'), server.command('add', 'shutdown'))
      expect(ServerSystem.transfer(device.path, target.port, 'owned bytes')).to eq('after-fin:setyb denwo')
      ServerSystem.wait_until { server.command('health').dig('connector', 'tcp_open').zero? }
      response, = device.path.request('PREPARE_SHUTDOWN')
      expect(response['error']).to be_nil
      device.path.close
      status = Timeout.timeout(8) { Process.wait2(device.process).last }
      expect(status.success?).to be(true), device.log.read
      expect(device.log.read).not_to include('Broken pipe')
    ensure
      device&.close
      server&.close
      target&.close
    end
  end
  %w[server-first entry-first].each do |order|
    it "uses one full server and the public native entry template in #{order} order" do
      Dir.mktmpdir('skvoz-native-topology-') do |root|
        target = ServerSystem::Target.new { |socket| ServerSystem.after_fin(socket) }
        path = directory(root, 'server')
        ServerSystem.certificates(path)
        server = ServerSystem::Server.new(path, autostart: order == 'server-first',
          allow: [{ 'cidr' => '127.0.0.0/8', 'protocols' => [6], 'ports' => [target.port] }])
        public_keys = if order == 'server-first'
          server.command('auth-public')
        else
          output, error, status = ServerSystem.capture(*server.cli, 'auth-public', '--offline', '--config', server.config)
          expect(status.success?).to be(true), error
          JSON.parse(output, allow_duplicate_key: false)
        end
        expect(public_keys.keys.sort).to eq(%w[issuer namespace xkey])
        entry = ServerSystem::NativeEntry.new(File.join(root, 'entry'), public_keys)
        # Ordinary callers cannot borrow leaf-only credentials as clients,
        # including while the remote Auth Callout is unavailable.
        entry.transport.fetch('entries').first.fetch('users').each_value do |credential|
          expect(ServerSystem.credentials_work(entry, credential.fetch('username'), credential.fetch('password'))).to be(false)
        end
        server.stop if order == 'server-first'
        transport = File.join(path, 'native-leaf.json')
        ServerSystem.private_json(transport, entry.transport)
        server.configure(leaf_remotes: transport, client_address: 'localhost',
          client_ca: entry.directory.join('ca.pem').to_s, advertised_port: entry.port)
        server.start
        ServerSystem.wait_until { entry.monitor('/leafz').fetch('leafnodes') == 3 }
        bundle = server.command('add', 'nativeclient', 'native-topology-fixture-password')
        expect(bundle.fetch('port')).to eq(entry.port)
        device = ServerSystem::Device.new(File.join(root, 'device'), bundle)
        bytes = ('native-entry:' + 'x' * 65520).b
        expect(ServerSystem.transfer(device.path, target.port, bytes)).to eq('after-fin:'.b + bytes.reverse)
        ServerSystem.wait_until { server.command('health').dig('connector', 'tcp_open').zero? }
        expect(server.command('health')).to include('healthy' => true)
      ensure
        ServerSystem.preserve_artifacts(root) if $!
        device&.close
        server&.close
        entry&.close
        target&.close
      end
    end
  end
end
