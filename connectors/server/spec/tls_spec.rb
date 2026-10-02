# frozen_string_literal: true
require_relative 'spec_helper'
require_relative 'support/system'

RSpec.describe 'Verified broker TLS identity', integration: true do
  include ServerSystem

  before(:context) do
    @directory = Pathname.new(Dir.mktmpdir('skvoz-tls-spec-'))
    @directory.chmod(0o700)
    @ca, @ca_key, @key = certificates(@directory, identity: 'server.invalid', san: 'IP:203.0.113.9,DNS:server.invalid')
    wrong_key = OpenSSL::PKey::RSA.new(2048)
    @directory.join('wrong.pem').write(certificate(key: wrong_key, name: 'Unrelated CA', ca: true).to_pem)
    @directory.join('wrong.pem').chmod(0o600)
    @directory.join('empty.pem').write("not a PEM root certificate\n")
    @directory.join('empty.pem').chmod(0o600)
    @password = SecureRandom.urlsafe_base64(32)
  end

  after do
    terminate(@core) if @core
    terminate(@broker, timeout: 5) if @broker
    @output&.close
    @core = @broker = nil
  end

  after(:context) { FileUtils.remove_entry(@directory) }

  def broker(leaf = @directory.join('server.pem'))
    @port = free_port
    config = @directory.join('nats.conf')
    config.write(<<~CONF)
      listen: "127.0.0.1:#{@port}"
      max_payload: 65588
      tls { cert_file: #{JSON.generate(leaf.to_s)}, key_file: #{JSON.generate(@directory.join('server.key').to_s)}, handshake_first: false }
      authorization { users: [{user: "tls-check", password: #{JSON.generate(@password)}}] }
    CONF
    config.chmod(0o600)
    @output = File.open(@directory.join('nats.log'), 'wb')
    @broker = Process.spawn(ServerSystem::NATS, '-c', config.to_s, out: @output, err: @output)
    wait_until do
      raise IOError, 'Test broker exited before readiness' if process_dead(@broker)
      @directory.join('nats.log').read.include?('Server is ready')
    end
  end

  def daemon_case(updates = {}, succeeds: false, message: 'TLS failed', env: {})
    directory = @directory.join(SecureRandom.hex(8))
    directory.mkdir(0o700)
    profile = { 'ipc_path' => directory.join('core.sock').to_s, 'url' => "tls://127.0.0.1:#{@port}",
                'tls_server_name' => '203.0.113.9', 'trust' => 'managed_ca', 'ca_file' => @directory.join('ca.pem').to_s,
                'username' => 'tls-check', 'password' => @password, 'namespace' => 'tls.acceptance',
                'peer_id' => 1, 'allowed_peers' => [0], 'initiate' => [0] }.merge(updates)
    profile.delete('tls_server_name') unless profile['tls_server_name']
    profile.delete('ca_file') if profile['trust'] == 'system'
    path = directory.join('profile.json')
    private_json(path, profile)
    log = directory.join('core.log')
    File.open(log, 'wb') do |output|
      @core = Process.spawn(env, ServerSystem::CORE, '--config', path.to_s, out: output, err: output)
      if succeeds
        wait_until(timeout: 8) { log.read.include?('READY ipc=1') || process_dead(@core) }
        expect(process_dead(@core)).to be(false), log.read
        expect(log.read).to include('READY ipc=1')
      else
        _, status = Timeout.timeout(8) { Process.wait2(@core) }
        @core = nil
        expect([2, 3]).to include(status.exitstatus), log.read
        expect(log.read).to include(message)
      end
      expect(log.read).not_to include(@password)
    end
  end

  {
    'verifies a public IP SAN through a loopback dial' => [{}, true],
    'verifies a DNS SAN through a loopback dial' => [{ 'tls_server_name' => 'server.invalid' }, true],
    'checks the dial identity when the override is absent' => [{ 'tls_server_name' => nil }, false],
    'rejects the wrong verification identity' => [{ 'tls_server_name' => 'different.invalid' }, false],
    'rejects an unrelated trust root' => [:wrong_root, false],
    'rejects an empty trust root file' => [:empty_root, false],
    'rejects a malformed bracketed verification identity' => [{ 'tls_server_name' => '[::1]' }, false],
    'rejects a private CA with ordinary System roots' => [{ 'trust' => 'system' }, false]
  }.each do |description, (updates, succeeds)|
    it description do
      broker
      updates = { 'ca_file' => @directory.join(updates == :wrong_root ? 'wrong.pem' : 'empty.pem').to_s } if updates.is_a?(Symbol)
      message = updates['tls_server_name'] == '[::1]' ? 'invalid runtime profile' : 'TLS failed'
      daemon_case(updates, succeeds:, message:)
    end
  end

  it 'uses the native System root loader without relaxing the verified IP identity' do
    broker
    roots = @directory.join('empty-root-directory'); roots.mkdir(0o700)
    daemon_case({ 'trust' => 'system' }, succeeds: true,
      env: { 'SSL_CERT_FILE' => @directory.join('ca.pem').to_s, 'SSL_CERT_DIR' => roots.to_s })
  end

  it 'rejects an expired leaf over an actual NATS handshake' do
    leaf = @directory.join('expired.pem')
    leaf.write(certificate(key: @key, issuer: @ca, issuer_key: @ca_key, name: 'server.invalid',
      san: 'IP:203.0.113.9,DNS:server.invalid', expires: Time.now - 1).to_pem)
    leaf.chmod(0o600)
    broker(leaf)
    daemon_case
  end

  [['CN-only', nil, 'serverAuth'], ['wrong server EKU', 'IP:203.0.113.9,DNS:server.invalid', 'clientAuth']].each do |name, san, eku|
    it "rejects #{name} in both Core handshake and Ruby TLS material" do
      leaf = @directory.join(SecureRandom.hex(8) + '.pem')
      leaf.write(certificate(key: @key, issuer: @ca, issuer_key: @ca_key, name: 'server.invalid', san:, eku:).to_pem)
      leaf.chmod(0o600)
      broker(leaf)
      daemon_case({ 'tls_server_name' => 'server.invalid' })
      expect do
        Skvoz::Server::TLSMaterial.new(certificate: leaf.to_s, key: @directory.join('server.key').to_s,
          ca: @directory.join('ca.pem').to_s, identity: 'server.invalid')
      end.to raise_error(Skvoz::Server::Error)
    end
  end
end
