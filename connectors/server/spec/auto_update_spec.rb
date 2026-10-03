# frozen_string_literal: true
require_relative 'spec_helper'
require 'open3'
require 'shellwords'
require 'json'
require 'fileutils'

RSpec.describe 'Server auto-update command' do
  around do |example|
    Dir.mktmpdir('skvoz-update-') do |directory|
      @directory = directory
      @settings = File.join(directory, 'settings.sh')
      @env = { 'UPDATE_TEST_CALLS' => File.join(directory, 'calls.jsonl'),
               'UPDATE_TEST_PULLED' => File.join(directory, 'pulled') }
      helper = File.expand_path('support/auto_update_docker.rb', __dir__)
      # A private executable copy avoids requiring executable mode on the helper.
      docker = File.join(directory, 'docker')
      FileUtils.cp(helper, docker)
      File.chmod(0700, docker)
      settings = { docker_bin: docker, env_file: File.join(directory, 'deployment $01.env'),
                   compose_file: File.join(directory, 'compose file.yaml'), project_name: 'existing-skvoz' }
      File.write(@settings, settings.map { |key, value| "#{key}=#{Shellwords.escape(value)}\n" }.join)
      example.run
    end
  end

  def run_update(overrides = {})
    script = File.expand_path('../deployment/auto-update-enable.sh', __dir__)
    Open3.capture3(@env.merge(overrides), 'bash', script, '--run', @settings)
  end

  def calls
    File.readlines(@env.fetch('UPDATE_TEST_CALLS')).map { |line| JSON.parse(line) }
  end

  it 'keeps the selected project and file paths and emits no successful progress' do
    stdout, stderr, status = run_update
    expect(status.success?).to be(true)
    expect(stdout + stderr).to eq('')
    calls.each do |arguments|
      expect(arguments).to include('existing-skvoz', File.join(@directory, 'deployment $01.env'),
                                  File.join(@directory, 'compose file.yaml'))
    end
    expect(calls.last.drop_while { |argument| argument != 'up' }).to eq(%w[up -d --no-deps --no-build --pull never server])
  end

  it 'reports registry failure and never replaces the running container after a failed pull' do
    stdout, stderr, status = run_update('UPDATE_TEST_PULL_FAIL' => '1')
    expect(status.exitstatus).to eq(8)
    expect(stdout).to eq('')
    expect(stderr).to include('SKVOZ auto-update failed', 'Registry unavailable')
    expect(calls.none? { |arguments| arguments.include?('up') }).to be(true)
  end

  it 'leaves a manually stopped server stopped, including a stop during image download' do
    [{ 'UPDATE_TEST_STOPPED' => '1' }, { 'UPDATE_TEST_STOP_AFTER_PULL' => '1' }].each do |overrides|
      FileUtils.rm_f(@env.values)
      stdout, stderr, status = run_update(overrides)
      expect(status.success?).to be(true)
      expect(stdout + stderr).to eq('')
      expect(calls.none? { |arguments| arguments.include?('up') }).to be(true)
    end
  end
end
