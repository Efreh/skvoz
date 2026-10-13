# frozen_string_literal: true
require_relative 'spec_helper'
require_relative '../lib/skvoz/server/migration'

RSpec.describe 'Explicit offline authority conversion' do
  def config(directory)
    Skvoz::Server::Configuration.new('address' => 'localhost', 'bind' => '127.0.0.1', 'state_dir' => directory,
                                     'devices_per_user' => 2, 'max_identities' => 8, 'network' => {}, 'tls' => { 'mode' => 'local' })
  end

  def source(root)
    path = File.join(root, 'old')
    Dir.mkdir(path, 0o700)
    hash = Skvoz::Server::State.password_hash('migration-user-password')
    value = { 'v' => 2, 'namespace' => 'skvoz.application', 'components' => ['control', 'egress'],
              'revision' => 9, 'next_id' => 21, 'users' => { 'alice' => { 'hash' => hash, 'ids' => [19, 20], 'assigned' => [19, 20], 'devices' => { 'a' * 32 => 20 } } },
              'internal_password' => 'discard-this-secret', 'nodes' => { 'old' => {} }, 'revoked_nodes' => [], 'tls' => nil }
    Skvoz::Server::PrivateFiles.write(File.join(path, 'state.json'), JSON.generate(value))
    [path, value]
  end

  it 'preserves valid allocations and token bindings while creating new independent keys and a startup fence' do
    Dir.mktmpdir do |root|
      old_path, old = source(root)
      bytes = File.binread(File.join(old_path, 'state.json'))
      target = config(File.join(root, 'new'))
      expect(Skvoz::Server::Migration.copy(old_path, target)).to include('v' => 3, 'devices' => 2, 'security_pending' => true)
      state = Skvoz::Server::State.new(target)
      expect(state.value.fetch('users').fetch('alice')).to include(old.fetch('users').fetch('alice'))
      expect(state.value.fetch('next_id')).to eq(21)
      expect(state.value.fetch('revision')).to eq(10)
      expect(state.access_ready?).to be(false)
      expect(state.value.keys).not_to include('nodes', 'revoked_nodes', 'components', 'internal_password')
      identity = state.identity('alice')
      services = state.value.fetch('services')
      state.close
      state = Skvoz::Server::State.new(target)
      expect(state.identity('alice')).to eq(identity)
      expect(state.value.fetch('services')).to eq(services)
      state.close
      expect(File.binread(File.join(old_path, 'state.json'))).to eq(bytes)
      second = config(File.join(root, 'second'))
      Skvoz::Server::Migration.copy(old_path, second)
      state = Skvoz::Server::State.new(second)
      expect(state.identity('alice')).not_to eq(identity)
      expect(state.value.fetch('services')).not_to eq(services)
      state.close
      expect { Skvoz::Server::Migration.copy(old_path, target) }.to raise_error(Errno::EEXIST)
    end
  end

  it 'rejects active source, egress-only data, overlapping allocations and unsafe destinations' do
    Dir.mktmpdir do |root|
      old_path, old = source(root)
      target = config(File.join(root, 'new'))
      File.open(File.join(old_path, 'server.lock'), File::RDWR | File::CREAT, 0o600) do |lock|
        lock.flock(File::LOCK_EX)
        expect { Skvoz::Server::Migration.copy(old_path, target) }.to raise_error(Skvoz::Server::Error, 'Source server state is in use')
      end
      old['components'] = ['egress']
      Skvoz::Server::PrivateFiles.write(File.join(old_path, 'state.json'), JSON.generate(old))
      expect { Skvoz::Server::Migration.copy(old_path, target) }.to raise_error(Skvoz::Server::Error)
      expect(File.exist?(target['state_dir'])).to be(false)
      old['components'] = ['control']
      old['users']['bob'] = old['users']['alice']
      Skvoz::Server::PrivateFiles.write(File.join(old_path, 'state.json'), JSON.generate(old))
      expect { Skvoz::Server::Migration.copy(old_path, target) }.to raise_error(Skvoz::Server::Error)
      expect(File.exist?(target['state_dir'])).to be(false)
      File.symlink(old_path, target['state_dir'])
      expect { Skvoz::Server::Migration.copy(old_path, target) }.to raise_error(Errno::EEXIST)
      expect(File.symlink?(target['state_dir'])).to be(true)
    end
  end
end
