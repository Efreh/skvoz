# frozen_string_literal: true
require 'bcrypt'
require 'securerandom'
require_relative 'tls'
require_relative 'policy'

module Skvoz
  module Server
    class Configuration
      DEFAULTS = { 'state_dir' => '/var/lib/skvoz', 'address' => nil, 'port' => 4222, 'advertised_port' => nil, 'bind' => '0.0.0.0',
                   'namespace' => 'skvoz.application', 'core_binary' => 'skvoz-core-daemon', 'nats_binary' => 'nats-server',
                   'monitor_port' => 8222, 'admin_timeout' => 5, 'stop_timeout' => 8,
                   'max_streams' => 64, 'stream_queue_frames' => 32, 'stream_queue_bytes' => 16384, 'tcp_buffer_bytes' => 16384, 'max_identities' => 128, 'devices_per_user' => 8,
                   'allow' => [], 'deny' => [], 'tls' => {} }.freeze
      attr_reader :value

      def initialize(value)
        raise Error, 'Invalid server configuration' unless value.is_a?(Hash) && (value.keys - DEFAULTS.keys).empty?
        @value = DEFAULTS.merge(value)
        %w[state_dir core_binary nats_binary namespace bind].each do |key|
          item = @value[key]
          raise Error, 'Invalid server configuration string' unless item.is_a?(String) && item.bytesize.between?(1, 4096) && !item.include?("\0")
        end
        raise Error, 'State directory must be absolute' unless @value['state_dir'].start_with?('/')
        @value['advertised_port'] = @value['port'] if @value['advertised_port'].nil?
        address = @value['address']
        Destination.new(Protocol.metadata(v: 1, type: 'tcp', host: address, port: @value['port']))
        bind = IPAddr.new(@value['bind'])
        raise Error, 'Bind must accept the internal IPv4 loopback dial' unless bind.ipv4? && (bind.to_i.zero? || bind.to_s == '127.0.0.1')
        { 'port' => 1..65_535, 'advertised_port' => 1..65_535, 'tcp_buffer_bytes' => 1024..262144, 'monitor_port' => 1..65_535, 'max_streams' => 1..1024, 'stream_queue_frames' => 1..128, 'stream_queue_bytes' => 8192..131072, 'max_identities' => 1..512,
          'devices_per_user' => 1..64, 'admin_timeout' => 1..30, 'stop_timeout' => 1..60 }.each do |key, range|
          raise Error, 'Invalid server configuration limit' unless @value[key].is_a?(Integer) && range.cover?(@value[key])
        end
        raise Error, 'Identity allocation exceeds configured budget' if @value['devices_per_user'] > @value['max_identities']
        raise Error, 'Port settings conflict' if @value['port'] == @value['monitor_port']
        raise Error, 'Invalid namespace' unless @value['namespace'].match?(/\A[a-zA-Z0-9_-]+(?:\.[a-zA-Z0-9_-]+)*\z/) && @value['namespace'].bytesize <= 256
        raise Error, 'Invalid policy configuration' unless @value['allow'].is_a?(Array) && @value['deny'].is_a?(Array) && @value['allow'].length + @value['deny'].length <= 128
        Policy.new(allow: @value['allow'], deny: @value['deny'])
        validate_tls
      rescue IPAddr::InvalidAddressError, KeyError, TypeError, URI::InvalidURIError
        raise Error, 'Invalid server configuration'
      end

      def [](key) = @value.fetch(key)

      def tls
        { 'mode' => 'acme', 'directory' => 'https://acme-v02.api.letsencrypt.org/directory', 'challenge_host' => '0.0.0.0',
          'challenge_port' => 8080, 'challenge_public_port' => 80, 'renewal_interval' => 86400, 'order_timeout' => 120 }.merge(self['tls']).merge('identity' => self['address'])
      end

      private

      def validate_tls
        options = @value['tls']
        keys = %w[mode directory challenge_host challenge_port challenge_public_port renewal_interval order_timeout email terms_agreed issuer_ca certificate_ca certificate key ca]
        raise Error, 'Invalid TLS configuration' unless options.is_a?(Hash) && (options.keys - keys).empty?
        mode = options.fetch('mode', 'acme')
        raise Error, 'Unsupported TLS mode' unless %w[acme provided].include?(mode)
        %w[issuer_ca certificate_ca certificate key ca].each do |key|
          next unless options.key?(key)
          path = options[key]
          raise Error, 'Invalid TLS file path' unless path.is_a?(String) && path.start_with?('/') && !path.include?("\0") && path.bytesize <= 4096
        end
        if mode == 'provided'
          raise Error, 'Provided TLS paths required' unless %w[certificate key].all? { |key| options[key].is_a?(String) }
        else
          raise Error, 'ACME contact and agreement required' unless options['email'].is_a?(String) && options['email'].match?(/\A[^\s@]+@[^\s@]+\.[^\s@]+\z/) && options['terms_agreed'] == true
          directory = options.fetch('directory', 'https://acme-v02.api.letsencrypt.org/directory')
          raise Error, 'Invalid ACME directory' unless directory.is_a?(String) && directory.bytesize <= 2048
          uri = URI(directory)
          raise Error, 'ACME directory must use HTTPS' unless uri.scheme == 'https' && uri.host && uri.userinfo.nil? && uri.fragment.nil?
          host = options.fetch('challenge_host', '0.0.0.0')
          raise Error, 'Invalid ACME challenge host' unless host.is_a?(String) && host.bytesize <= 64
          IPAddr.new(host)
          { 'challenge_port' => [8080, 1..65_535], 'challenge_public_port' => [80, 1..65_535], 'order_timeout' => [120, 10..600], 'renewal_interval' => [86400, 1..86400] }.each do |key, (default, range)|
            item = options.fetch(key, default)
            raise Error, 'Invalid ACME limit' unless item.is_a?(Integer) && range.cover?(item)
          end
          raise Error, 'ACME listener conflicts with broker' if [@value['port'], @value['monitor_port']].include?(options.fetch('challenge_port', 8080))
        end
      end
    end

    class State
      LOGIN = /\A[a-zA-Z0-9_-]{1,64}\z/
      INTERNAL = '__skvoz_server'
      attr_reader :directory, :value, :revision

      def initialize(config)
        @config = config
        @directory = PrivateFiles.create_directory(config['state_dir'])
        @lock = File.open(File.join(@directory, 'server.lock'), File::RDWR | File::CREAT | File::NOFOLLOW, 0o600)
        raise Error, 'Server state is already in use' unless @lock.flock(File::LOCK_EX | File::LOCK_NB)
        @path = File.join(@directory, 'state.json')
        if File.exist?(@path)
          @value = JSON.parse(PrivateFiles.read(@path, maximum: 1_048_576))
          raise Error, 'Unsupported persistent state' unless @value['v'] == 1 && @value['namespace'] == config['namespace']
        else
          password = SecureRandom.urlsafe_base64(32)
          @value = { 'v' => 1, 'revision' => 0, 'next_id' => 1, 'namespace' => config['namespace'],
                     'internal_password' => password, 'internal_hash' => BCrypt::Password.create(password, cost: 12).to_s,
                     'users' => {}, 'tls' => nil }
          commit(@value)
        end
        @revision = @value.fetch('revision')
      rescue JSON::ParserError, KeyError
        raise Error, 'Persistent state invalid'
      end

      def close = @lock.close
      def candidate = Marshal.load(Marshal.dump(@value))

      def commit(value)
        if @value && @value['tls'] != value['tls']
          value['previous_tls'] = @value['tls']
        end
        PrivateFiles.write(@path, JSON.generate(value))
        @value = value
        @revision = value.fetch('revision')
      end

      def mutate(operation, login, password: nil)
        raise Error, 'Invalid login' unless login.is_a?(String) && login.match?(LOGIN) && login != INTERNAL
        current = candidate
        users = current.fetch('users')
        exported_id = nil
        case operation
        when 'add'
          raise Error, 'Login already exists' if users.key?(login)
          active = users.values.sum { |user| user.fetch('ids').length }
          count = @config['devices_per_user']
          raise Error, 'Identity pool admission exceeded' if active + count > @config['max_identities']
          start = current.fetch('next_id')
          raise Error, 'Identity allocator exhausted' if start + count >= 1 << 64
          ids = (start...(start + count)).to_a
          current['next_id'] += count
          exported_id = ids.first
          users[login] = { 'hash' => password_hash(password), 'ids' => ids, 'assigned' => [exported_id] }
        when 'device-add'
          user = users.fetch(login) { raise Error, 'Login does not exist' }
          raise Error, 'Password verification failed' unless BCrypt::Password.new(user.fetch('hash')) == password
          exported_id = (user.fetch('ids') - user.fetch('assigned')).first
          raise Error, 'Device pool exhausted' unless exported_id
          user.fetch('assigned') << exported_id
        when 'reset-password'
          user = users.fetch(login) { raise Error, 'Login does not exist' }
          user['hash'] = password_hash(password)
        when 'remove'
          raise Error, 'Login does not exist' unless users.delete(login)
        else
          raise Error, 'Unsupported user operation'
        end
        current['revision'] += 1
        [current, exported_id]
      end

      def nats_config(state = @value, tls: state.fetch('tls'))
        raise Error, 'TLS material missing' unless tls
        namespace = @config['namespace']
        server_permissions = { 'publish' => ["#{namespace}.join.*.0", "#{namespace}.lane.*.*.0.*.0.*"],
                               'subscribe' => ["#{namespace}.join.0.*", "#{namespace}.lane.0.*.*.*.*.*"] }
        users = [{ 'user' => INTERNAL, 'password' => state.fetch('internal_hash'), 'permissions' => server_permissions }]
        state.fetch('users').each do |login, user|
          publish, subscribe = [], []
          user.fetch('ids').each do |id|
            publish.concat(["#{namespace}.join.0.#{id}", "#{namespace}.lane.0.*.#{id % 8}.*.#{id}.*"])
            subscribe.concat(["#{namespace}.join.#{id}.*", "#{namespace}.lane.#{id}.*.*.*.*.*"])
          end
          users << { 'user' => login, 'password' => user.fetch('hash'), 'permissions' => { 'publish' => publish, 'subscribe' => subscribe } }
        end
        <<~CONF
          server_name: "skvoz-server"
          listen: #{JSON.generate(@config['bind'] + ':' + @config['port'].to_s)}
          http: #{JSON.generate('127.0.0.1:' + @config['monitor_port'].to_s)}
          max_payload: 65588
          max_pending: 4MB
          max_connections: #{@config['max_identities'] * 10 + 16}
          write_deadline: "2s"
          tls {
            cert_file: #{JSON.generate(tls.fetch('certificate'))}
            key_file: #{JSON.generate(tls.fetch('key'))}
            handshake_first: true
          }
          authorization { users: #{JSON.generate(users)} }
        CONF
      end

      def profile
        tls = @value.fetch('tls')
        window, frame, streams = 8192, 1024, @config['max_streams']
        profile = { 'ipc_path' => File.join(@directory, 'core.sock'), 'url' => "tls://127.0.0.1:#{@config['port']}",
                    'tls_server_name' => @config['address'], 'trust' => tls['ca'] ? 'managed_ca' : 'system',
                    'username' => INTERNAL, 'password' => @value.fetch('internal_password'), 'namespace' => @config['namespace'],
                    'peer_id' => 0, 'broker_authorized' => true,
                    'limits' => { 'owners' => 8, 'streams_per_owner' => [streams + 16, 1024].min, 'streams' => streams + 16,
                                  'streams_per_peer' => streams + 16, 'peers' => @config['max_identities'],
                                  'receive_window' => window, 'max_frame' => frame,
                                  'receive_bytes' => window * (streams + 16), 'receive_bytes_per_peer' => window * (streams + 16),
                                  'send_bytes' => 8192 * streams, 'send_bytes_per_peer' => 8192 * streams,
                                  'output_frames' => [streams * 32, 4096].min, 'output_bytes' => [streams * 16_384, 8_388_608].min,
                                  'subscription_frames' => [streams * (window / frame + 4), 128].max, 'join_frames' => 1024 } }
        profile['ca_file'] = tls['ca'] if tls['ca']
        profile
      end

      def export(login, id, password)
        { 'v' => 1, 'address' => @config['address'], 'port' => @config['advertised_port'], 'username' => login,
          'password' => password, 'namespace' => @config['namespace'], 'peer_id' => id,
          'allowed_peers' => [0], 'initiate' => [0], 'shards' => 8,
          'trust' => @value.fetch('tls')['ca'] ? 'managed_ca' : 'system' }
      end

      private

      def password_hash(password)
        raise Error, 'Password must contain 12 to 72 UTF-8 bytes' unless password.is_a?(String) && password.valid_encoding? && password.bytesize.between?(12, 72)
        BCrypt::Password.create(password, cost: 12).to_s
      end
    end
  end
end
