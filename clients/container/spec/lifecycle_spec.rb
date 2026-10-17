# frozen_string_literal: true
require_relative '../lib/skvoz/client'
require 'tmpdir'

RSpec.describe 'VPN session health' do
  it 'rejects a closed session even when the control owner is alive and STATUS says ready/ip' do
    Dir.mktmpdir do |directory|
      state = File.join(directory, 'state')
      run = File.join(directory, 'run')
      Dir.mkdir(run, 0o700)
      old = ENV.values_at('SKVOZ_CLIENT_STATE', 'SKVOZ_CLIENT_RUN')
      ENV['SKVOZ_CLIENT_STATE'], ENV['SKVOZ_CLIENT_RUN'] = state, run
      config = Skvoz::Client::Config.new({ 'server' => 'entry.example:4222', 'username' => 'example', 'password' => 'example-password', 'mode' => 'vpn', 'families' => [4] }.to_yaml)
      service = Skvoz::Client::Service.new(config, '192.0.2.1')
      enrollment = instance_double(Skvoz::Client::Enrollment, call: {})
      allow(Skvoz::Client::Enrollment).to receive(:new).and_return(enrollment)
      native = Process.spawn(RbConfig.ruby, '-e', 'sleep 30')
      alive_at_status = false
      control = Object.new
      control.define_singleton_method(:call) do |operation, *_arguments, **_options|
        if operation == 'STATUS'
          alive_at_status = Process.kill(0, native) == 1
          [{ 'lifecycle' => 'ready', 'mode' => 'ip', 'session' => { 'handle' => 'current', 'state' => 'closed' } }, nil]
        else
          Process.kill('TERM', native) if operation == 'PREPARE_SHUTDOWN'
          [{}, nil]
        end
      end
      control.define_singleton_method(:stop) { nil }
      allow(service).to receive(:start_runtime) do
        service.instance_variable_set(:@runtime_pid, native)
        service.instance_variable_set(:@control, control)
      end
      allow(service).to receive(:start_vpn) { service.instance_variable_set(:@ip_handle, 'current') }
      Async do |task|
        expect { service.send(:connect, task) }.to raise_error(Skvoz::Client::Error, 'ip_session_closed')
      end.wait
      expect(alive_at_status).to be true
      expect(JSON.parse(File.read(File.join(run, 'health.json')))['healthy']).to be false
      expect { Process.waitpid(native, Process::WNOHANG) }.to raise_error(Errno::ECHILD)
    ensure
      service&.state&.close
      service&.instance_variable_get(:@stop_read)&.close
      service&.instance_variable_get(:@stop_write)&.close
      ENV['SKVOZ_CLIENT_STATE'], ENV['SKVOZ_CLIENT_RUN'] = old if old
    end
  end
end

RSpec.describe 'Atomic listener startup' do
  it 'closes earlier listeners and never marks healthy when a later listener is occupied' do
    Dir.mktmpdir do |directory|
      occupied = TCPServer.new('127.0.0.1', 0)
      first = TCPServer.new('127.0.0.1', 0)
      first_port = first.local_address.ip_port
      first.close
      run = File.join(directory, 'run'); Dir.mkdir(run, 0o700)
      old = ENV.values_at('SKVOZ_CLIENT_STATE', 'SKVOZ_CLIENT_RUN')
      ENV['SKVOZ_CLIENT_STATE'], ENV['SKVOZ_CLIENT_RUN'] = File.join(directory, 'state'), run
      profile = { 'server' => 'entry.example:4222', 'username' => 'example', 'password' => 'example-password', 'proxy' => false,
        'tcp' => { 'first' => { 'listen' => "127.0.0.1:#{first_port}", 'target' => 'example.org:443' },
          'occupied' => { 'listen' => "127.0.0.1:#{occupied.local_address.ip_port}", 'target' => 'example.org:443' } } }
      service = Skvoz::Client::Service.new(Skvoz::Client::Config.new(profile.to_yaml), '192.0.2.1')
      allow(Skvoz::Client::Enrollment).to receive(:new).and_return(instance_double(Skvoz::Client::Enrollment, call: {}))
      native = Process.spawn(RbConfig.ruby, '-e', 'sleep 30')
      control = Object.new
      control.define_singleton_method(:call) do |operation, *_arguments, **_options|
        Process.kill('TERM', native) if operation == 'PREPARE_SHUTDOWN'
        [operation == 'START_PROXY' ? { 'http' => nil, 'socks' => nil } : {}, nil]
      end
      control.define_singleton_method(:stop) { nil }
      allow(service).to receive(:start_runtime) do
        service.instance_variable_set(:@runtime_pid, native)
        service.instance_variable_set(:@control, control)
      end
      Async do |task|
        expect { service.send(:connect, task) }.to raise_error(Errno::EADDRINUSE)
        replacement = TCPServer.new('127.0.0.1', first_port)
        replacement.close
      end.wait
      expect(JSON.parse(File.read(File.join(run, 'health.json')))['healthy']).to be false
      expect(service.instance_variable_get(:@adapters)).to be_nil
      expect { Process.waitpid(native, Process::WNOHANG) }.to raise_error(Errno::ECHILD)
    ensure
      occupied&.close
      service&.state&.close
      service&.instance_variable_get(:@stop_read)&.close
      service&.instance_variable_get(:@stop_write)&.close
      ENV['SKVOZ_CLIENT_STATE'], ENV['SKVOZ_CLIENT_RUN'] = old if old
    end
  end
end
