# frozen_string_literal: true
require 'yaml'
require_relative '../../../connectors/server/spec/spec_helper'

RSpec.describe 'Component CI trigger isolation' do
  ROOT = File.expand_path('../../..', __dir__)
  def events(name)
    value = YAML.load_file(File.join(ROOT, '.github/workflows', name))
    value.fetch('on', value[true])
  end
  def path_trigger?(workflow, path)
    events(workflow).fetch('push').fetch('paths').any? do |pattern|
      pattern.end_with?('/**') ? path.start_with?(pattern.delete_suffix('**')) : File.fnmatch?(pattern, path)
    end
  end
  it 'triggers only the client for normal Linux UI/package/source changes' do
    %w[clients/ubuntu/src/ui.rs clients/ubuntu/src/ipc.rs clients/ubuntu/packaging/build-deb.sh clients/ubuntu/packaging/org.skvoz.Client.desktop].each do |path|
      expect(path_trigger?('ubuntu-client.yml', path)).to be(true)
      expect(path_trigger?('server.yml', path)).to be(false)
    end
  end
  it 'checks both dependencies on shared Core/network/Cargo changes' do
    %w[core/src/runtime.rs network/src/runtime.rs network/helper/src/service.rs Cargo.lock Cargo.toml].each do |path|
      %w[ubuntu-client.yml server.yml].each { |name| expect(path_trigger?(name, path)).to be(true) }
    end
    expect(path_trigger?('ubuntu-client.yml', 'connectors/server/lib/skvoz/server/enrollment.rb')).to be(true)
    expect(path_trigger?('ubuntu-client.yml', 'daemon/src/driver.rs')).to be(false)
    expect(path_trigger?('ubuntu-client.yml', 'clients/ruby/skvoz.rb')).to be(false)
  end
  it 'keeps release tags separate because GitHub does not apply path filters to tags' do
    server = events('server.yml').fetch('push').fetch('tags')
    client = events('ubuntu-client.yml').fetch('push').fetch('tags')
    expect(client.any? { |pattern| File.fnmatch?(pattern, 'ubuntu-v0.1.0') }).to be(true)
    expect(server.any? { |pattern| File.fnmatch?(pattern, 'ubuntu-v0.1.0') }).to be(false)
    expect(server.any? { |pattern| File.fnmatch?(pattern, 'v0.1.0') }).to be(true)
    expect(client.any? { |pattern| File.fnmatch?(pattern, 'v0.1.0') }).to be(false)
  end
end
