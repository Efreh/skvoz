# frozen_string_literal: true
require_relative 'spec_helper'
require_relative 'support/system'
require 'net/http'

RSpec.describe 'ACME certificate request contract' do
  ['203.0.113.9', '2001:db8::9', 'server.example.org'].each do |identity|
    it "preserves a signed SAN for #{identity} without an IP Common Name" do
      key = OpenSSL::PKey::EC.generate('prime256v1')
      issuer = Skvoz::Server::CertificateIssuer.new({}, nil)
      request = issuer.send(:certificate_request, identity, key).csr
      expect(request.verify(key)).to be(true)
      names = request.subject.to_a.select { |name, _value, _type| name == 'CN' }.map { |_name, value, _type| value }
      if identity == 'server.example.org'
        expect(names).to eq([identity])
      else
        expect(names).to be_empty
      end
      extension_request = request.attributes.find { |attribute| attribute.oid == 'extReq' }.value
      extensions = extension_request.value.first.value
      san = extensions.map { |extension| OpenSSL::X509::Extension.new(extension.to_der) }.find { |extension| extension.oid == 'subjectAltName' }
      values = OpenSSL::ASN1.decode(OpenSSL::ASN1.decode(san.to_der).value.last.value).value
      expected = identity == 'server.example.org' ? [2, identity] : [7, IPAddr.new(identity).hton]
      expect(values.map { |value| [value.tag, value.value] }).to eq([expected])
    end
  end
end

RSpec.describe 'Automatic ACME certificate lifecycle', integration: true do
  include ServerSystem

  before do
    @directory = Pathname.new(Dir.mktmpdir('skvoz-acme-spec-'))
    @directory.chmod(0o700)
    certificates(@directory)
  end

  after do
    begin
      preserve_artifacts(@directory) if RSpec.current_example.exception
      @device&.close
      @server&.close
    ensure
      terminate(@issuer, timeout: 5) if @issuer
      @issuer_output&.close
      FileUtils.remove_entry(@directory)
    end
  end

  def committed_certificate
    state = JSON.parse(@server.state.join('state.json').read)
    leaf = OpenSSL::X509::Certificate.new(File.read(state.fetch('tls').fetch('certificate')))
    [state, leaf]
  end

  def start_issuer
    acme, management, challenge = free_port, free_port, free_port
    config = @directory.join('pebble.json')
    private_json(config, 'pebble' => {
      'listenAddress' => "127.0.0.1:#{acme}", 'managementListenAddress' => "127.0.0.1:#{management}",
      'certificate' => @directory.join('server.pem').to_s, 'privateKey' => @directory.join('server.key').to_s,
      'httpPort' => challenge, 'tlsPort' => free_port, 'externalAccountBindingRequired' => false,
      'retryAfter' => { 'authz' => 1, 'order' => 1 },
      'profiles' => { 'shortlived' => { 'description' => 'bounded system test', 'validityPeriod' => 30 } }
    })
    @issuer_output = File.open(@directory.join('pebble.log'), 'wb')
    @issuer = Process.spawn({ 'PEBBLE_VA_NOSLEEP' => '1', 'PEBBLE_WFE_NONCEREJECT' => '0' },
      ServerSystem::PEBBLE, '-config', config.to_s, out: @issuer_output, err: @issuer_output)
    ca = @directory.join('issued-ca.pem')
    roots = wait_until do
      raise IOError, "Local ACME issuer exited: #{@directory.join('pebble.log').read}" if process_dead(@issuer)
      begin
        Net::HTTP.start('localhost', management, use_ssl: true, ca_file: @directory.join('ca.pem').to_s,
          open_timeout: 0.5, read_timeout: 0.5) do |http|
          response = http.get('/roots/0')
          response.is_a?(Net::HTTPSuccess) && response.body.bytesize <= 65_536 && response.body
        end
      rescue IOError, SystemCallError, OpenSSL::SSL::SSLError, Timeout::Error
        false
      end
    end
    ca.write(roots); ca.chmod(0o600)
    { 'mode' => 'acme', 'directory' => "https://localhost:#{acme}/dir", 'email' => 'qualification@example.com',
      'terms_agreed' => true, 'challenge_host' => '127.0.0.1', 'challenge_port' => challenge,
      'challenge_public_port' => challenge, 'order_timeout' => 20, 'renewal_interval' => 1,
      'issuer_ca' => @directory.join('ca.pem').to_s, 'certificate_ca' => ca.to_s }
  end

  it 'issues IP SAN via HTTP01, renews without losing concurrent users and recovers expiry and uncertain reload safely' do
    tls = start_issuer
    @server = ServerSystem::Server.new(@directory, overrides: { 'address' => '127.0.0.1', 'tls' => tls })
    _, first = committed_certificate
    expect(first.extensions.find { |extension| extension.oid == 'subjectAltName' }.value).to include('IP Address:127.0.0.1')
    expect(first.not_after - first.not_before).to be <= 31
    mutations = 2.times.map { |index| Thread.new { @server.command('add', "acme#{index}") } }
    wait_until(timeout: 20) { committed_certificate.last.serial != first.serial }
    profiles = mutations.map(&:value)
    @device = ServerSystem::Device.new(@directory.join('device'), profiles.first, ServerSystem::CORE, @server.port)
    expect(@server.command('list').length).to eq(2)
    expect(@server.state.glob('tls-*').length).to be <= 3
    users = committed_certificate.first.fetch('users')
    nats = child_pid(@server.process, 'nats-server')
    Process.kill('STOP', nats)
    begin
      # Hold the broker before reload so the pending candidate cannot commit between polls.
      wait_until(timeout: 20) { @server.state.join('candidate.json').exist? }
      wait_until(timeout: 12) { @server.command('health')['failure'] == 'configuration_apply_uncertain' }
      committed = committed_certificate.first
      pending = JSON.parse(@server.state.join('candidate.json').read)
      expect(committed.fetch('tls')).not_to eq(pending.fetch('tls'))
      expect(committed.fetch('users')).to eq(users)
      expect(@server.command('health')['healthy']).to be(false)
    ensure
      Process.kill('CONT', nats) rescue Errno::ESRCH
    end
    wait_until(timeout: 25) { @server.command('health')['healthy'] }
    @server.stop
    tls['renewal_interval'] = 86_400
    private_json(@server.config, @server.value)
    _, expired = committed_certificate
    wait_until(timeout: 40) { Time.now > expired.not_after + 1 }
    @server.start
    current, replacement = committed_certificate
    expect(replacement.serial).not_to eq(expired.serial)
    expect(current.fetch('users')).to eq(users)
    profile = @device.profile.binread
    ca_path = JSON.parse(profile).fetch('ca_file')
    ca = File.binread(ca_path)
    # A raw daemon can become terminal after the deliberately long broker outage.
    @device.restart
    expect(@device.profile.binread).to eq(profile)
    expect(File.binread(ca_path)).to eq(ca)
    expect(device_ready(@device.path)).to be(true)
    profiles.each { |profile| expect(profile.fetch('ca_pem')).to eq(@directory.join('issued-ca.pem').read) }
    expect(@server.state.glob('tls-*').length).to be <= 2
    @server.stop
    _, last = committed_certificate
    terminate(@issuer, timeout: 5)
    @issuer = nil
    wait_until(timeout: 40) { Time.now > last.not_after + 1 }
    _, _, status = capture(*@server.cli, 'serve', '--config', @server.config, timeout: 40)
    expect(status.success?).to be(false)
    expect(committed_certificate.first.fetch('users')).to eq(users)
  end

  it 'keeps only committed and previous generations across repeated invalid supplied-certificate restarts' do
    @server = ServerSystem::Server.new(@directory)
    profile = @server.command('add', 'persistent')
    @server.stop
    @server.start
    @server.stop
    users = JSON.parse(@server.state.join('state.json').read).fetch('users')
    invalid = @server.value.merge('address' => '203.0.113.9')
    private_json(@server.config, invalid)
    3.times do
      _, _, status = capture(*@server.cli, 'serve', '--config', @server.config, timeout: 15)
      expect(status.success?).to be(false)
      expect(@server.state.glob('tls-*').length).to be <= 2
      expect(JSON.parse(@server.state.join('state.json').read).fetch('users')).to eq(users)
    end
    expect(JSON.generate(users)).not_to include(profile.fetch('password'))
  end
end
