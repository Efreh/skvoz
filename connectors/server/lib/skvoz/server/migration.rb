# frozen_string_literal: true
require_relative 'state'
require_relative 'tls'
require 'fileutils'

module Skvoz
  module Server
    # Explicit offline copy; old node roles and transport credentials are discarded.
    module Migration
      def self.copy(source, config)
        PrivateFiles.directory(source)
        lock = File.open(File.join(source, 'server.lock'), File::RDWR | File::CREAT | File::NOFOLLOW, 0o600)
        raise Error, 'Source server state is in use' unless lock.flock(File::LOCK_EX | File::LOCK_NB)
        old = JSON.parse(PrivateFiles.read(File.join(source, 'state.json'), maximum: 1_048_576), max_nesting: 16, allow_duplicate_key: false)
        unless old.is_a?(Hash) && old['v'] == 2 && old['namespace'] == config['namespace'] &&
               [['control'], ['control', 'egress']].include?(old['components']) && old['users'].is_a?(Hash) &&
               old['revision'].is_a?(Integer) && old['revision'].positive? && old['revision'] < (1 << 63) - 1
          raise Error, 'Migration requires a version 2 client authority state'
        end
        users = old.fetch('users').each_with_index.to_h do |(login, user), index|
          raise Error, 'Invalid migration user' unless user.is_a?(Hash) && (user.keys - %w[hash ids assigned devices]).empty?
          [login, { 'hash' => user.fetch('hash'), 'ids' => user.fetch('ids'), 'assigned' => user.fetch('assigned'),
                    'devices' => user.fetch('devices', {}), 'access_revision' => index + 1 }]
        end
        destination = config['state_dir']
        PrivateFiles.ancestors(File.dirname(destination))
        Dir.mkdir(destination, 0o700)
        created = true
        PrivateFiles.write(File.join(destination, 'migration-incomplete'), "offline conversion\n")
        state = State.new(config, migration: true)
        candidate = state.candidate
        candidate.merge!('revision' => old.fetch('revision') + 1, 'next_id' => old.fetch('next_id'),
                         'users' => users, 'access_revision' => users.size, 'security_pending' => true)
        state.commit(candidate)
        if old['tls']
          material = old.fetch('tls')
          raise Error, 'Invalid migration TLS material' unless material.is_a?(Hash) && (material.keys - %w[certificate key ca]).empty?
          material.values.compact.each do |path|
            raise Error, 'Migration TLS must belong to the source state' unless path.is_a?(String) && path.start_with?(source + '/')
          end
          valid = TLSMaterial.new(certificate: material.fetch('certificate'), key: material.fetch('key'), ca: material['ca'], identity: config['address'])
          raise Error, 'Migration TLS has expired' unless valid.valid?
          generation = File.join(destination, 'tls-' + SecureRandom.hex(8))
          PrivateFiles.create_directory(generation)
          candidate = state.candidate
          candidate['tls'] = material.to_h do |kind, path|
            next [kind, nil] unless path
            target = File.join(generation, kind + '.pem')
            PrivateFiles.write(target, PrivateFiles.read(path, maximum: kind == 'key' ? 16_384 : 1_048_576))
            [kind, target]
          end
          state.commit(candidate)
        end
        File.unlink(File.join(destination, 'migration-incomplete'))
        File.open(destination, File::RDONLY) { |directory| directory.fsync }
        completed = true
        { 'v' => 3, 'users' => users.size, 'devices' => state.access_set.fetch('devices').size, 'security_pending' => true }
      ensure
        state&.close
        lock&.close
        FileUtils.remove_entry_secure(destination) if created && !completed
      end
    end
  end
end
