# frozen_string_literal: true
require_relative '../../../connectors/server/spec/spec_helper'
require_relative '../../../connectors/server/spec/support/system'

RSpec.describe 'Embedded Android JNI host on the Linux JVM', integration: true do
  include ServerSystem

  it 'loads the native library, enrolls with verified TLS and carries real HTTP/CONNECT/SOCKS bytes' do
    Dir.mktmpdir('skvoz-android-jni-') do |directory|
      directory = Pathname.new(directory)
      certificates(directory, san: 'DNS:localhost,IP:127.0.0.1,IP:::1')
      wrong_key = OpenSSL::PKey::RSA.new(2048)
      directory.join('wrong-ca.pem').write(certificate(key: wrong_key, name: 'Wrong test CA', ca: true).to_pem)
      directory.join('wrong-ca.pem').chmod(0o600)
      target = ServerSystem::Target.new do |socket|
        bytes = ''.b
        bytes << socket.readpartial(16_384) while bytes.bytesize <= 8 * 1024 * 1024
        raise IOError, 'Test target budget exceeded'
      rescue EOFError
        socket.write('after-fin:'.b + bytes.reverse)
      end
      http_target = ServerSystem::Target.new do |socket|
        header = ''.b
        header << (socket.read(1) || raise(EOFError)) until header.end_with?("\r\n\r\n") || header.bytesize > 16_384
        raise IOError, 'Test HTTP header budget exceeded' if header.bytesize > 16_384
        length = header[/\r\nContent-Length: (\d+)/i, 1].to_i
        raise IOError, 'Test HTTP body budget exceeded' if length > 8 * 1024 * 1024
        body = socket.read(length) || raise(EOFError)
        raise IOError, 'Incomplete test HTTP body' if body.bytesize != length
        socket.write("HTTP/1.1 200 OK\r\nContent-Length: #{body.bytesize}\r\nConnection: close\r\n\r\n".b + body)
      end
      server = ServerSystem::Server.new(directory, allow: [{ 'cidr' => '127.0.0.0/8', 'protocols' => [6], 'ports' => [target.port, http_target.port] }])
      server.command('add', 'android', 'process-test-password')
      fixture = Pathname.new(__dir__).join('../native/tests/fixtures/NativeBridge.java')
      capture('javac', '-d', directory, fixture).then { |_, error, status| expect(status.success?).to be(true), error }
      profile = { host: 'localhost', port: server.port, username: 'android', password: 'process-test-password', ca_file: directory.join('ca.pem').to_s, device: SecureRandom.hex(16) }
      path = directory.join('profile.json'); private_json(path, profile)
      native = ENV.fetch('SKVOZ_ANDROID_NATIVE', ServerSystem::ROOT.join('target/release').to_s)
      http, socks = free_ports(2, except: [server.port, target.port])
      Open3.popen3('java', '-Xmx128m', "-Djava.library.path=#{native}", '-cp', directory.to_s, 'org.skvoz.android.NativeBridge', path.to_s, http.to_s, socks.to_s) do |input, output, error, process|
        line = Timeout.timeout(35) { output.gets&.strip }
        expect(line).to eq('READY'), (line.nil? ? error.read : line)
        payload = ("\0b\xff1".b * 1_048_576)
        stdout, stderr, status = capture('curl', '--silent', '--show-error', '--fail', '--max-time', '45', '--noproxy', '', '--proxy', "http://127.0.0.1:#{http}", '--data-binary', '@-', "http://127.0.0.1:#{http_target.port}/", input: payload, timeout: 50)
        expect(status.success?).to be(true), stderr
        expect(stdout.b).to eq(payload)
        [false, true].each do |socks_mode|
          socket = TCPSocket.new('127.0.0.1', socks_mode ? socks : http)
          if socks_mode
            socket.write("\5\1\0".b); expect(socket.read(2)).to eq("\5\0".b)
            socket.write([5, 1, 0, 1, 127, 0, 0, 1].pack('C*') + [target.port].pack('n'))
            expect(socket.read(10)).to eq([5, 0, 0, 1, 0, 0, 0, 0, 0, 0].pack('C*'))
          else
            socket.write("CONNECT 127.0.0.1:#{target.port} HTTP/1.1\r\nHost: 127.0.0.1:#{target.port}\r\n\r\n")
            header = ''.b; header << socket.read(1) until header.end_with?("\r\n\r\n")
            expect(header).to include('200 Connection Established')
          end
          socket.write(payload); socket.close_write
          expect(Timeout.timeout(45) { socket.read }).to eq('after-fin:'.b + payload.reverse)
          socket.close
        end
        input.puts('STOP'); input.flush
        expect(Timeout.timeout(10) { output.gets&.strip }).to eq('STOPPED')
        expect(Timeout.timeout(10) { process.value.success? }).to be(true), error.read
      ensure
        begin
          Process.kill('KILL', process.pid) if process.alive?
        rescue Errno::ESRCH
          # The JVM may exit between the liveness check and signal delivery.
        end
      end
      [profile.merge(password: 'incorrect-test-password'), profile.merge(ca_file: directory.join('wrong-ca.pem').to_s)].each do |negative|
        private_json(path, negative)
        stdout, _, status = capture('java', '-Xmx128m', "-Djava.library.path=#{native}", '-cp', directory, 'org.skvoz.android.NativeBridge', path, 'negative', timeout: 30)
        expect(status.success?).to be(true)
        expect(stdout).to match(/authentication_failed|certificate_failed/)
        expect(stdout).not_to include(negative[:password])
      end
      pending = ServerSystem::Target.new do |socket|
        socket.write("INFO {\"tls_required\":true,\"tls_available\":true}\r\n")
        socket.read
      end
      pending_path = directory.join('pending-profile.json'); private_json(pending_path, profile.merge(port: pending.port))
      private_json(path, profile)
      stdout, stderr, status = capture('java', '-Xmx128m', "-Djava.library.path=#{native}", '-cp', directory, 'org.skvoz.android.NativeBridge', pending_path, 'cancel', path, timeout: 30)
      expect(status.success?).to be(true), stderr
      expect(stdout).to include('CANCELLED_RECONNECTED')
      stdout, stderr, status = capture('java', '-Xmx128m', "-Djava.library.path=#{native}", '-cp', directory, 'org.skvoz.android.NativeBridge', path, 'closure', timeout: 40)
      expect(status.success?).to be(true), stderr
      expect(stdout).to include('CLOSED_RECOVERABLE_REPLACED_HANDLES0_DIAGNOSTICS_RESET_BOUNDED')
      warn stdout.strip
      stdout, stderr, status = capture('java', '-Xmx128m', "-Djava.library.path=#{native}", '-cp', directory, 'org.skvoz.android.NativeBridge', path, 'cycles', timeout: 90)
      expect(status.success?).to be(true), stderr
      expect(stdout).to include('CYCLES50_HANDLES0_FD=')
      warn stdout.strip
    ensure
      pending&.close
      server&.close; target&.close; http_target&.close
    end
  end
end
