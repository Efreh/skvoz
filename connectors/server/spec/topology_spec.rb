# frozen_string_literal: true
require 'net/http'
require_relative 'spec_helper'
require_relative 'support/system'

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

  it 'assigns standalone TCP to its local egress and completes native half-close' do
    Dir.mktmpdir('skvoz-standalone-') do |root|
      target = ServerSystem::Target.new { |socket| ServerSystem.after_fin(socket) }
      allow_rule = [{ 'cidr' => '127.0.0.0/8', 'protocols' => [6], 'ports' => [target.port] }]
      server = configured_server(root, 'server', allow: allow_rule)
      bundle = server.command('add', 'standalone', 'standalone secret password')
      device = ServerSystem::Device.new(File.join(root, 'device'), bundle)
      # The first request precedes membership confirmation and must progress
      # through bounded capacity-only attempts without an application replay.
      bytes = (0...65536).map { |n| (n % 251).chr }.join.b
      expect(ServerSystem.transfer(device.path, target.port, bytes)).to eq('after-fin:'.b + bytes.reverse)
      ServerSystem.wait_until { server.command('health').dig('connector', 'tcp_open').zero? }
    ensure
      device&.close
      server&.close
      target&.close
    end
  end

  it 'runs entry without DATA and distributes simultaneous streams over two initiating exits' do
    Dir.mktmpdir('skvoz-leaf-pool-') do |root|
      target = ServerSystem::Target.new { |socket| ServerSystem.after_fin(socket) }
      entry = configured_server(root, 'entry', overrides: { 'components' => ['control'] })
      expect(entry.command('health').dig('connector', 'routing')).to eq('control_ready' => true, 'eligible_exits' => 0)
      exits = 2.times.map do |index|
        join = entry.command('node-add')
        path = File.join(root, "join-#{index}.json")
        ServerSystem.private_json(path, join)
        configured_server(root, "exit-#{index}", overrides: { 'components' => ['egress'], 'upstream' => path },
          allow: [{ 'cidr' => '127.0.0.0/8', 'protocols' => [6], 'ports' => [target.port] }])
      end
      bundle = entry.command('add', 'pooled', 'pooled secret password')
      device = ServerSystem::Device.new(File.join(root, 'device'), bundle)
      first = ('immediate:' + 'y' * 65526).b
      expect(ServerSystem.transfer(device.path, target.port, first)).to eq('after-fin:'.b + first.reverse)
      # Observe both confirmed exits, rather than assuming identical renewal
      # timing. The first request above must already work before this barrier.
      ServerSystem.wait_until { entry.command('health').dig('connector', 'routing', 'eligible_exits') == 2 }
      ServerSystem.wait_until { exits.all? { |server| server.command('health').dig('connector', 'tcp_open').zero? } }
      sockets = 2.times.map { device.path.open('127.0.0.1', target.port) }
      expect(entry.command('health').dig('connector', 'tcp_open')).to eq(0)
      ServerSystem.wait_until { exits.all? { |server| server.command('health').dig('connector', 'tcp_open') == 1 } }
      sockets.each_with_index do |socket, index|
        bytes = ("node-#{index}:".b + ('x' * 65527).b)
        socket.write(bytes)
        socket.close_write
        expect(Timeout.timeout(15) { socket.read }).to eq('after-fin:'.b + bytes.reverse)
        socket.close
      end
      ServerSystem.wait_until { exits.all? { |server| server.command('health').dig('connector', 'tcp_open').zero? } }
      exits.each do |server|
        state = JSON.parse(File.read(server.state.join('state.json')))
        expect(state).not_to have_key('users')
        expect(state).not_to have_key('nodes_passwords')
        expect(state.fetch('tls').fetch('key')).to start_with(server.state.to_s)
      end
    ensure
      sockets&.each { |socket| socket.close rescue IOError }
      device&.close
      exits&.reverse_each(&:close)
      entry&.close
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
  it 'closes live leaf credentials on rotation and revoke and admits the retained current pool again' do
    Dir.mktmpdir('skvoz-node-lifecycle-') do |root|
      target = ServerSystem::Target.new { |socket| ServerSystem.after_fin(socket) }
      entry = configured_server(root, 'entry', overrides: { 'components' => ['control'] })
      joins = 2.times.map { entry.command('node-add') }
      paths = joins.each_with_index.map do |join, index|
        path = File.join(root, "join-#{index}.json")
        ServerSystem.private_json(path, join)
        path
      end
      exits = paths.each_with_index.map do |path, index|
        configured_server(root, "exit-#{index}", overrides: { 'components' => ['egress'], 'upstream' => path },
          allow: [{ 'cidr' => '127.0.0.0/8', 'protocols' => [6], 'ports' => [target.port] }])
      end
      bundle = entry.command('add', 'lifecycle', 'private lifecycle password')
      device = ServerSystem::Device.new(File.join(root, 'device'), bundle)
      ServerSystem.wait_until { entry.command('health').dig('connector', 'routing', 'eligible_exits') == 2 }
      expect(ServerSystem.transfer(device.path, target.port, 'before rotation')).to eq('after-fin:noitator erofeb')
      metadata = entry.command('node-show', joins[0].fetch('identity'))
      expect(metadata.keys.sort).to eq(%w[address identity node_id port])
      children = File.read("/proc/#{entry.process}/task/#{entry.process}/children").split.map(&:to_i)
      broker = children.find { |pid| File.read("/proc/#{pid}/comm").strip == 'nats-server' }
      http = Net::HTTP.new('127.0.0.1', entry.monitor, nil)
      http.open_timeout = http.read_timeout = 1
      expect(JSON.parse(http.get('/leafz').body).fetch('leafnodes')).to eq(2)
      rotated = entry.command('node-rotate', joins[0].fetch('identity'))
      expect(rotated.values_at('identity', 'node_id')).to eq(joins[0].values_at('identity', 'node_id'))
      expect(rotated.fetch('password')).not_to eq(joins[0].fetch('password'))
      ServerSystem.wait_until(timeout: 5) { ServerSystem.process_dead(broker) }
      # The documented managed hub restart interrupts all incarnations. Normal
      # supervised hosts restart them; retained credentials need no reissue.
      device.wait_for_termination; device = nil
      exits.each { |server| server.stop(kill: true) }
      entry.stop(kill: true); entry.start
      expect(ServerSystem.leaf_credentials_work(rotated)).to be(true)
      expect(ServerSystem.leaf_credentials_work(joins[0])).to be(false)
      ServerSystem.private_json(paths[0], rotated)
      exits.each(&:start)
      device = ServerSystem::Device.new(File.join(root, 'rotated-client'), bundle)
      ServerSystem.wait_until { entry.command('health').dig('connector', 'routing', 'eligible_exits') == 2 }
      expect(ServerSystem.transfer(device.path, target.port, 'after rotation')).to eq('after-fin:noitator retfa')
      children = File.read("/proc/#{entry.process}/task/#{entry.process}/children").split.map(&:to_i)
      broker = children.find { |pid| File.read("/proc/#{pid}/comm").strip == 'nats-server' }
      expect(entry.command('node-revoke', rotated.fetch('identity'))).to eq('revoked' => rotated.fetch('identity'))
      ServerSystem.wait_until(timeout: 5) { ServerSystem.process_dead(broker) }
      device.wait_for_termination; device = nil
      exits.each { |server| server.stop(kill: true) }
      entry.stop(kill: true); entry.start
      expect(ServerSystem.leaf_credentials_work(joins[1])).to be(true)
      expect(ServerSystem.leaf_credentials_work(rotated)).to be(false)
      exits[1].start
      device = ServerSystem::Device.new(File.join(root, 'retained-client'), bundle)
      ServerSystem.wait_until { entry.command('health').dig('connector', 'routing', 'eligible_exits') == 1 }
      expect(ServerSystem.transfer(device.path, target.port, 'retained exit')).to eq('after-fin:tixe deniater')
      expect(entry.command('node-list')).to eq([{ 'identity' => joins[1].fetch('identity'), 'node_id' => joins[1].fetch('node_id') }])
    ensure
      device&.close
      exits&.reverse_each(&:close)
      entry&.close
      target&.close
    end
  end

end
