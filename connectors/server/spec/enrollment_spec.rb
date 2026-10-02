# frozen_string_literal: true
require_relative 'spec_helper'
require_relative 'support/system'

RSpec.describe 'Authenticated device enrollment', integration: true do
  include ServerSystem

  def enrollment(server, login, password, token, payload: nil, reply_login: login, request_login: login, reply_target: nil)
    context = OpenSSL::SSL::SSLContext.new
    context.ca_file = server.directory.join('ca.pem').to_s
    context.verify_mode = OpenSSL::SSL::VERIFY_PEER
    raw = TCPSocket.new('127.0.0.1', server.port)
    tls = OpenSSL::SSL::SSLSocket.new(raw, context)
    tls.sync_close = true
    tls.hostname = 'localhost'
    Timeout.timeout(20) do
      tls.connect; tls.post_connection_check('localhost')
      reply = "skvoz.enroll.reply.#{reply_login}.#{SecureRandom.hex(16)}"
      tls.write("CONNECT #{JSON.generate(user: login, pass: password, verbose: false)}\r\nSUB #{reply} 1\r\nPING\r\n")
      loop do
        line = tls.gets("\r\n", 4096)
        raise IOError, 'Enrollment permission rejected' if !line || line.start_with?('-ERR')
        break if line == "PONG\r\n"
      end
      payload ||= JSON.generate(v: 1, device: token)
      tls.write("PUB skvoz.enroll.v1.#{request_login} #{reply_target || reply} #{payload.bytesize}\r\n#{payload}\r\nPING\r\n")
      loop do
        line = tls.gets("\r\n", 4096)
        raise IOError, 'Enrollment permission rejected' if !line || line.start_with?('-ERR')
        if line == "PING\r\n"
          tls.write("PONG\r\n")
          next
        end
        next unless line.start_with?('MSG ')
        size = line.split.last.to_i
        raise IOError, 'Enrollment response exceeds limit' unless size.between?(1, 512)
        bytes = tls.read(size)
        raise IOError, 'Invalid enrollment terminator' unless tls.read(2) == "\r\n"
        return JSON.parse(bytes)
      end
    end
  ensure
    tls&.close; raw&.close unless raw&.closed?
  end

  around do |example|
    Dir.mktmpdir('skvoz-enrollment-') do |directory|
      certificates(directory)
      @server = ServerSystem::Server.new(directory, overrides: { 'devices_per_user' => 4, 'max_identities' => 16 })
      @server.command('add', 'shared', 'process-test-password')
      @server.command('add', 'other', 'process-other-password')
      example.run
    ensure
      @server&.close
    end
  end

  it 'allocates once for concurrent retries and grants only assigned Core subjects' do
    state = JSON.parse(@server.state.join('state.json').read)
    unassigned = state.fetch('users').fetch('shared').fetch('ids')[1]
    subject = "skvoz.application.join.#{unassigned}.*"
    expect(credentials_work(@server, 'shared', 'process-test-password', subject:)).to be(false)
    answers = 8.times.map { Thread.new { enrollment(@server, 'shared', 'process-test-password', 'a' * 32) } }.map(&:value)
    expect(answers.map { |answer| answer.fetch('peer_id') }.uniq).to eq([unassigned])
    expect(credentials_work(@server, 'shared', 'process-test-password', subject:)).to be(true)
    @server.stop; @server.start
    expect(enrollment(@server, 'shared', 'process-test-password', 'a' * 32)['peer_id']).to eq(unassigned)
  end

  it 'rejects cross-login subjects/replies, malformed bodies and bounded exhaustion' do
    expect { enrollment(@server, 'shared', 'process-test-password', 'a' * 32, request_login: 'other') }.to raise_error(IOError)
    expect { enrollment(@server, 'shared', 'process-test-password', 'a' * 32, reply_login: 'other') }.to raise_error(IOError)
    expect(enrollment(@server, 'shared', 'process-test-password', 'a' * 32, payload: '{"v":1,"device":"bad","login":"other"}')).to include('error' => 'enrollment_failed')
    expect(enrollment(@server, 'shared', 'process-test-password', 'a' * 32, payload: '{"v":1,"v":1,"device":"' + 'a' * 32 + '"}')).to include('error' => 'enrollment_failed')
    expect(enrollment(@server, 'shared', 'process-test-password', 'a' * 32, payload: 'x' * 65_000)).to include('error' => 'invalid_request')
    %w[a b c].each { |letter| expect(enrollment(@server, 'shared', 'process-test-password', letter * 32)).to have_key('peer_id') }
    expect(enrollment(@server, 'shared', 'process-test-password', 'd' * 32)).to include('error' => 'device_limit')
    expect(@server.command('health')['healthy']).to be(true)
  end

  it 'preserves allocation across password reset and revokes removed login' do
    id = enrollment(@server, 'shared', 'process-test-password', 'a' * 32).fetch('peer_id')
    @server.command('reset-password', 'shared', 'replacement-test-password')
    expect(credentials_work(@server, 'shared', 'process-test-password')).to be(false)
    expect(enrollment(@server, 'shared', 'replacement-test-password', 'a' * 32)['peer_id']).to eq(id)
    @server.command('remove', 'shared')
    expect(credentials_work(@server, 'shared', 'replacement-test-password')).to be(false)
    expect(enrollment(@server, 'other', 'process-other-password', 'b' * 32)).to have_key('peer_id')
  end

  it 'keeps an established other-login stream and owned processes intact across abusive control traffic' do
    target = ServerSystem::Target.new do |socket|
      loop { socket.write(socket.readpartial(1024)) }
    rescue EOFError
      nil
    end
    @server.stop
    @server.value['allow'] = [{ 'cidr' => '127.0.0.1/32', 'ports' => [target.port] }]
    private_json(@server.config, @server.value)
    @server.start
    bundle = @server.command('device-add', 'other', 'process-other-password')
    device = ServerSystem::Device.new(@server.directory.join('device'), bundle)
    client, handle = open_client(device.path, target.port)
    children = File.read("/proc/#{@server.process}/task/#{@server.process}/children").split
    exchange = lambda do |bytes|
      expect(client.request(5, handle, bytes)[1]).to eq(bytes.bytesize)
      Timeout.timeout(5) do
        loop do
          kind, _, actual, payload = client.event
          next if kind == SkvozIPC::WRITABLE
          expect(actual).to eq(handle)
          expect(kind).to eq(SkvozIPC::DATA)
          expect(payload.byteslice(8..)).to eq(bytes)
          client.consume(handle, payload.unpack1('Q>') + bytes.bytesize)
          break
        end
      end
    end
    exchange.call('before-control'.b)
    expect(enrollment(@server, 'shared', 'process-test-password', 'a' * 32, payload: 'x' * 65_000)).to include('error' => 'invalid_request')
    expect do
      Timeout.timeout(1) { enrollment(@server, 'shared', 'process-test-password', 'a' * 32, reply_target: 'skvoz.application.join.0.2') }
    end.to raise_error(Timeout::Error)
    exchange.call('after-control'.b)
    expect(File.read("/proc/#{@server.process}/task/#{@server.process}/children").split).to eq(children)
    expect(@server.command('health')['healthy']).to be(true)
  ensure
    client&.close
    device&.close
    target&.close
  end
end
