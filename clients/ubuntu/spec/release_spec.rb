# frozen_string_literal: true
require 'json'
require 'open3'
require 'fileutils'
require 'yaml'
require 'digest'
require_relative '../../../connectors/server/spec/spec_helper'

RSpec.describe 'Ubuntu release publication' do
  let(:root) { File.expand_path('../../..', __dir__) }
  let(:workflow) { YAML.load_file(File.join(root, '.github/workflows/ubuntu-client.yml')) }
  let(:steps) { workflow.fetch('jobs').fetch('package').fetch('steps') }
  let(:publication) { steps.find { |step| step.fetch('name', '').start_with?('Publish') } }
  let(:build_tag) { 'ubuntu-v0.4.0-build.42' }
  let(:package_name) { 'skvoz-client_0.4.0_amd64.deb' }

  around do |example|
    Dir.mktmpdir('skvoz-release-') do |directory|
      @directory = directory
      FileUtils.mkdir_p(File.join(directory, 'clients/ubuntu/dist'))
      FileUtils.mkdir_p(File.join(directory, 'package/DEBIAN'))
      FileUtils.mkdir_p(File.join(directory, 'bin'))
      File.write(File.join(directory, 'package/DEBIAN/control'), <<~CONTROL)
        Package: skvoz-client
        Version: 0.4.0
        Architecture: amd64
        Maintainer: SKVOZ tests <tests@example.invalid>
        Description: Disposable release fixture
      CONTROL
      package = File.join(directory, 'clients/ubuntu/dist', package_name)
      output, status = Open3.capture2e('dpkg-deb', '--build', '--root-owner-group', File.join(directory, 'package'), package)
      raise output unless status.success?
      File.write("#{package}.sha256", "#{Digest::SHA256.file(package).hexdigest}  #{package_name}\n")
      FileUtils.cp(File.join(__dir__, 'support/github_cli.rb'), File.join(directory, 'bin/gh'))
      File.chmod(0o755, File.join(directory, 'bin/gh'))
      File.write(File.join(directory, 'state.json'), '[]')
      File.write(File.join(directory, 'calls.jsonl'), '')
      example.run
    end
  end

  def release(tag, id, draft: false)
    { 'id' => id, 'tag_name' => tag, 'draft' => draft,
      'published_at' => draft ? nil : format('2026-09-%02dT12:00:00Z', id % 28 + 1),
      'assets' => %w[old.deb old.deb.sha256] }
  end

  def state
    JSON.parse(File.read(File.join(@directory, 'state.json'), encoding: 'UTF-8'))
  end

  def seed(releases)
    File.write(File.join(@directory, 'state.json'), JSON.generate(releases))
  end

  def calls
    File.readlines(File.join(@directory, 'calls.jsonl'), encoding: 'UTF-8').map { |line| JSON.parse(line) }
  end

  def run_publication(overrides = {})
    env = { 'PATH' => "#{@directory}/bin:#{ENV.fetch('PATH')}",
            'SKVOZ_RELEASE_STATE' => File.join(@directory, 'state.json'),
            'SKVOZ_RELEASE_CALLS' => File.join(@directory, 'calls.jsonl'),
            'SKVOZ_RELEASE_FAIL' => '', 'GITHUB_REPOSITORY' => 'example/skvoz',
            'GITHUB_REF' => 'refs/heads/main', 'GITHUB_REF_NAME' => 'main',
            'GITHUB_SHA' => '0123456789abcdef0123456789abcdef01234567',
            'GITHUB_RUN_NUMBER' => '42', 'RUNNER_TEMP' => @directory }.merge(overrides)
    Open3.capture2e(env, 'bash', '-e', '-o', 'pipefail', '-c', publication.fetch('run'), chdir: @directory)
  end

  it 'attaches the checked deb and checksum before publishing at the exact build commit' do
    output, status = run_publication
    expect(status.success?).to be(true), output
    expect(state).to contain_exactly(include('tag_name' => build_tag, 'draft' => false,
                                           'assets' => [package_name, "#{package_name}.sha256"]))
    create = calls.find { |call| call[1] == 'create' }
    expect(create).to include('--draft', '--target', '0123456789abcdef0123456789abcdef01234567')
    expect(calls.map { |call| call[0..1] }).to eq([
      %w[release view], %w[release create], %w[release upload], %w[release edit], ['api', 'repos/example/skvoz/releases?per_page=100']
    ])
  end

  it 'keeps five latest published client releases across API pages while preserving server releases and drafts' do
    clients = (1..8).map { |id| release("ubuntu-v0.3.0-build.#{id}", id) }
    servers = (100..204).map { |id| release("v1.#{id}.0", id) }
    drafts = [release('ubuntu-v0.5.0', 300, draft: true)]
    seed((clients + servers + drafts).reverse)
    output, status = run_publication
    expect(status.success?).to be(true), output
    expect(state.select { |entry| entry['tag_name'].start_with?('ubuntu-v') && !entry['draft'] }.map { |entry| entry['tag_name'] })
      .to contain_exactly(build_tag, *clients.last(4).map { |entry| entry['tag_name'] })
    expect(state).to include(*servers, *drafts)
    expect(calls.select { |call| call[1] == 'delete' }.map { |call| call[2] })
      .to contain_exactly(*clients.first(4).map { |entry| entry['tag_name'] })
  end

  %w[create upload edit api].each do |operation|
    it "preserves all previous releases when #{operation} fails" do
      previous = (1..8).map { |id| release("ubuntu-v0.3.0-build.#{id}", id) }
      seed(previous)
      output, status = run_publication('SKVOZ_RELEASE_FAIL' => operation)
      expect(status.success?).to be(false), output
      expect(state).to include(*previous)
      expect(calls.none? { |call| call[1] == 'delete' }).to be(true)
      if operation == 'upload' || operation == 'edit'
        expect(state).to include(include('tag_name' => build_tag, 'draft' => true))
      end
    end
  end

  it 'continues a failed draft and leaves an already published release unchanged on rerun' do
    _, failed = run_publication('SKVOZ_RELEASE_FAIL' => 'upload')
    expect(failed.success?).to be(false)
    output, resumed = run_publication
    expect(resumed.success?).to be(true), output
    published = state
    File.write(File.join(@directory, 'calls.jsonl'), '')
    output, repeated = run_publication
    expect(repeated.success?).to be(true), output
    expect(state).to eq(published)
    expect(calls.map { |call| call[1] }).not_to include('create', 'upload', 'edit', 'delete')
  end

  it 'publishes a matching explicit version tag without creating a different tag' do
    output, status = run_publication('GITHUB_REF' => 'refs/tags/ubuntu-v0.4.0', 'GITHUB_REF_NAME' => 'ubuntu-v0.4.0')
    expect(status.success?).to be(true), output
    expect(state.first.fetch('tag_name')).to eq('ubuntu-v0.4.0')
    expect(calls.find { |call| call[1] == 'create' }).to include('--verify-tag')
    _, mismatch = run_publication('GITHUB_REF' => 'refs/tags/ubuntu-v0.9.0', 'GITHUB_REF_NAME' => 'ubuntu-v0.9.0')
    expect(mismatch.success?).to be(false)
    expect(state.size).to eq(1)
  end

  it 'refuses a package whose checksum changed before any remote mutation' do
    File.open(File.join(@directory, 'clients/ubuntu/dist', package_name), 'ab') { |file| file.write('changed') }
    _, status = run_publication
    expect(status.success?).to be(false)
    expect(calls).to be_empty
  end

  it 'limits publication to primary branches/version tags and serializes release runs without duplicate artifacts' do
    condition = publication.fetch('if')
    allowed = lambda do |event, ref|
      # Evaluate only the fixed GitHub expression grammar used by this condition.
      expression = condition.gsub('github.event_name', event.inspect).gsub('github.ref', ref.inspect)
      expression = expression.gsub(/startsWith\(("[^"]*"), '([^']*)'\)/, '\1.start_with?(\'\2\')')
      eval(expression) # rubocop:disable Security/Eval
    end
    %w[push workflow_dispatch].each do |event|
      %w[refs/heads/main refs/heads/master refs/tags/ubuntu-v0.4.0].each { |ref| expect(allowed.call(event, ref)).to be(true) }
      %w[refs/heads/feature refs/tags/v0.4.0].each { |ref| expect(allowed.call(event, ref)).to be(false) }
    end
    expect(allowed.call('pull_request', 'refs/heads/main')).to be(false)
    expect(workflow.fetch('concurrency')).to eq(
      'group' => "ubuntu-client-${{ github.event_name == 'pull_request' && github.ref || 'release' }}",
      'cancel-in-progress' => "${{ github.event_name == 'pull_request' }}"
    )
    expect(steps.none? { |step| step.fetch('uses', '').start_with?('actions/upload-artifact@') }).to be(true)
  end
end
