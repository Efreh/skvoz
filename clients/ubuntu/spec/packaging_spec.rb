# frozen_string_literal: true
require 'json'
require 'open3'
require 'fileutils'
require_relative '../../../connectors/server/spec/spec_helper'

RSpec.describe 'Ubuntu package removal' do
  let(:root) { File.expand_path('../../..', __dir__) }

  around do |example|
    Dir.mktmpdir('skvoz-package-') do |directory|
      @directory = directory
      @config = File.join(directory, 'config')
      @bin = File.join(directory, 'bin')
      @system = File.join(directory, 'usr/lib/systemd/system')
      FileUtils.mkdir_p([@config, @bin, @system])
      File.write(File.join(@config, '1000.json'), '{}')
      File.write(File.join(@config, '1001.json'), '{}')
      FileUtils.cp(Dir[File.join(root, 'network/helper/systemd/*.{socket,service}')], @system)
      %w[systemctl skvoz-network-helper].each do |name|
        FileUtils.cp(File.join(__dir__, 'support/packaging_commands.rb'), File.join(@bin, name))
        File.chmod(0o755, File.join(@bin, name))
      end
      @calls = File.join(directory, 'calls.jsonl')
      @state = File.join(directory, 'state.json')
      File.write(@calls, '')
      File.write(@state, JSON.generate(sockets: {
        'skvoz-network-helper@1000.socket' => true,
        'skvoz-network-helper@1001.socket' => false
      }))
      %w[1000 1001].each do |uid|
        output, status = Open3.capture2e('/usr/bin/systemctl', "--root=#{directory}", 'enable', "skvoz-network-helper@#{uid}.socket")
        raise output unless status.success?
      end
      # Redirect hook filesystem paths to a private fixture without host privileges.
      script = File.read(File.join(root, 'clients/ubuntu/packaging/prerm'))
      script = script.gsub('/etc/skvoz-network-helper', @config)
                     .gsub('/usr/libexec/skvoz-network-helper', File.join(@bin, 'skvoz-network-helper'))
                     .gsub('/run/systemd/system', @system)
      @script = File.join(directory, 'prerm')
      File.write(@script, script)
      example.run
    end
  end

  def remove(**overrides)
    value = JSON.parse(File.read(@state)).merge(overrides.transform_keys(&:to_s))
    File.write(@state, JSON.generate(value))
    env = { 'PATH' => "#{@bin}:#{ENV.fetch('PATH')}", 'SKVOZ_PACKAGE_ROOT' => @directory,
            'SKVOZ_PACKAGE_STATE' => @state, 'SKVOZ_PACKAGE_CALLS' => @calls }
    Open3.capture2e(env, 'sh', @script, 'remove')
  end

  def commands(operation)
    File.readlines(@calls).map { |line| JSON.parse(line) }
        .select { |call| call[0..1] == ['systemctl', operation] }.map { |call| call.fetch(2) }
  end

  it 'stops active and inactive instances and disables all template enablement' do
    output, status = remove
    expect(status.success?).to be(true), output
    expect(commands('stop')).to contain_exactly('skvoz-network-helper@1000.socket', 'skvoz-network-helper@1001.socket')
    expect(commands('disable')).to include('skvoz-network-helper@.socket')
    expect(Dir[File.join(@directory, 'etc/systemd/system/sockets.target.wants/skvoz-network-helper@*.socket')]).to be_empty
  end

  it 'removes safely when no socket instances are loaded' do
    output, status = remove(sockets: {})
    expect(status.success?).to be(true), output
    expect(commands('stop')).to be_empty
    expect(Dir[File.join(@directory, 'etc/systemd/system/sockets.target.wants/skvoz-network-helper@*.socket')]).to be_empty
  end

  it 'rejects removal while a helper is active or retained state is unsafe' do
    [{ active_helper: true }, { fail_check: 1 }].each do |failure|
      File.write(@calls, '')
      output, status = remove(active_helper: false, **failure)
      expect(status.success?).to be(false), output
      expect(commands('stop')).to be_empty
      expect(commands('disable')).to be_empty
    end
  end

  it 'restores previously active admission when state changes after sockets stop' do
    output, status = remove(fail_check: 3)
    expect(status.success?).to be(false), output
    expect(commands('stop')).to contain_exactly('skvoz-network-helper@1000.socket', 'skvoz-network-helper@1001.socket')
    expect(commands('start')).to eq(['skvoz-network-helper@1000.socket'])
    expect(commands('disable')).to be_empty
  end
end
