# frozen_string_literal: true
require_relative 'spec_helper'
require_relative 'support/system'

RSpec.describe 'Joint Rust network server', integration: true do
  include ServerSystem

  around do |example|
    Dir.mktmpdir('skvoz-server-network-') do |temporary|
      @directory = Pathname.new(temporary)
      certificates(@directory)
      @targets, @devices = [], []
      example.run
    ensure
      @devices.reverse_each(&:close)
      @server&.close
      @targets.reverse_each(&:close)
    end
  end

  def target(&operation)
    ServerSystem::Target.new(&operation).tap { |item| @targets << item }
  end

  def start_server(ports, deny: [])
    @server = ServerSystem::Server.new(@directory, allow: [{ 'cidr' => '127.0.0.0/8', 'protocols' => [6], 'ports' => ports }], overrides: { 'deny' => deny })
  end

  def device(login)
    bundle = @server.command('add', login)
    ServerSystem::Device.new(@directory.join(login), bundle).tap { |item| @devices << item }
  end

  it 'preserves opaque binary TCP bytes and a reply after local EOF across independent clients' do
    echo = target { |socket| after_fin(socket) }
    start_server([echo.port])
    devices = 2.times.map { |index| device("user#{index}") }
    payload = (0..255).to_a.pack('C*') * 128
    answers = devices.map { |item| Thread.new { transfer(item.path, echo.port, payload) } }.map(&:value)
    expect(answers).to eq(['after-fin:'.b + payload.reverse] * 2)
    expect(@server.command('health')).to include('healthy' => true)
  end

  it 'keeps management endpoints denied even when an explicit allow includes their ports' do
    echo = target { |socket| after_fin(socket) }
    start_server([echo.port, 4222])
    @server.stop
    @server.value['allow'].first['ports'] = [echo.port, @server.port, @server.monitor]
    private_json(@server.config, @server.value)
    @server.start
    client = device('policy')
    expect { client.path.open('127.0.0.1', @server.port) }.to raise_error(IOError, 'forbidden')
    expect { client.path.open('127.0.0.1', @server.monitor) }.to raise_error(IOError, 'forbidden')
    expect(transfer(client.path, echo.port, 'healthy')).to eq('after-fin:yhtlaeh')
  end

  it 'isolates a slow native writer and preserves healthy traffic and credential revocation' do
    slow = target { |socket| sleep 3; after_fin(socket) }
    echo = target { |socket| after_fin(socket) }
    start_server([slow.port, echo.port])
    first, victim = device('slow'), device('victim')
    writer = Thread.new { transfer(first.path, slow.port, 'x' * 65536, timeout: 20) }
    expect(transfer(victim.path, echo.port, 'independent')).to eq('after-fin:tnednepedni')
    expect(writer.value.bytesize).to eq(65546)
    @server.command('remove', 'slow')
    expect(credentials_work(@server, 'slow', JSON.parse(first.profile.read).fetch('core').fetch('password'))).to be(false)
    expect(transfer(victim.path, echo.port, 'after-revoke')).to eq('after-fin:ekover-retfa')
    first.wait_for_termination
  end

  it 'requires a fresh application process after runtime death and retains committed users' do
    echo = target { |socket| after_fin(socket) }
    start_server([echo.port])
    users = @server.command('add', 'persistent')
    runtime = child_pid(@server.process, 'skvoz-network-r')
    expect(runtime).not_to be_nil
    Process.kill('KILL', runtime)
    wait_until { process_dead(@server.process) }
    @server.stop(kill: true)
    log = @server.log.read
    expect(log).to include('"event":"supervised_exit","failure":"network_signal_9"')
    expect(log).to include('"failure":"network_signal_9"')
    expect(log).not_to include(users.fetch('password'))
    @server.start
    expect(@server.command('show', 'persistent')['devices']).to include(users.fetch('peer_id'))
    expect(@server.command('health')['healthy']).to be(true)
  end

  def control_owner(mode)
    runtime = @directory.join(mode + '-runtime')
    FileUtils.cp(Pathname.new(__dir__).join('fixtures/control_owner.rb'), runtime)
    runtime.chmod(0o700)
    @server = ServerSystem::Server.new(@directory, overrides: { 'runtime_binary' => runtime.to_s })
    Integer(@server.state.join('control-owner.pid').read)
  end

  it 'records the actual child signal when control EOF precedes the child exit status' do
    runtime = control_owner('delayed-sigkill')
    Process.kill('USR1', runtime)
    wait_until(timeout: 3) { process_dead(@server.process) }
    @server.stop(kill: true)
    expect(@server.log.read).to include('"event":"supervised_exit","failure":"network_signal_9"')
    expect(@server.log.read).to include('"event":"runtime_fatal","stage":"running","failure":"network_signal_9"')
    expect(process_dead(runtime)).to be(true)
  end

  it 'promptly stops a live owner after EOF or malformed control without inventing a child-death cause' do
    %w[live-eof malformed].each do |mode|
      runtime = control_owner(mode)
      Process.kill('USR1', runtime)
      wait_until(timeout: 3) { process_dead(@server.process) }
      @server.stop(kill: true)
      expect(@server.log.read).to include('"event":"runtime_fatal","stage":"running","failure":"runtime_control_failed"')
      expect(@server.log.read).not_to include('"event":"supervised_exit"')
      expect(process_dead(runtime)).to be(true)
      @server.close
    end
  end
end
