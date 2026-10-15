# frozen_string_literal: true
require 'bcrypt'
require 'securerandom'
require 'json'
require 'uri'
require_relative 'process'
require_relative 'network_configuration'
require_relative 'nats_auth'
require_relative 'native_links'

module Skvoz
  module Server
    class Configuration
      DEFAULTS = { 'v' => 5, 'leaf_remotes' => nil, 'client_address' => nil, 'client_ca' => nil,
                   'state_dir' => '/var/lib/skvoz', 'address' => nil, 'port' => 4222, 'advertised_port' => nil, 'bind' => '0.0.0.0',
                   'namespace' => 'skvoz.application', 'runtime_binary' => 'skvoz-network-runtime', 'helper_binary' => 'skvoz-network-helper', 'nats_binary' => 'nats-server',
                   'monitor_port' => 8222, 'admin_timeout' => 5, 'stop_timeout' => 8,
                   'max_identities' => 128, 'devices_per_user' => 8,
                   'allow' => [], 'deny' => [],
                   'network' => { 'ipv4' => { 'pool' => '10.203.0.0/16', 'egress' => 'nat44', 'interface' => 'eth0' }.freeze,
                                  'dns_servers' => ['1.1.1.1'].freeze }.freeze,
                   'tls' => {} }.freeze
      attr_reader :value

      def initialize(value)
        raise Error, 'Invalid server configuration' unless value.is_a?(Hash) && (value.keys - DEFAULTS.keys).empty?
        @value = DEFAULTS.merge(value)
        @links = NativeLinks.read(@value['leaf_remotes']) if @value['leaf_remotes']
        %w[state_dir runtime_binary helper_binary nats_binary namespace bind].each do |key|
          item = @value[key]
          raise Error, 'Invalid server configuration string' unless item.is_a?(String) && item.bytesize.between?(1, 4096) && !item.include?("\0")
        end
        raise Error, 'State directory must be absolute' unless @value['state_dir'].start_with?('/')
        @value['advertised_port'] = @value['port'] if @value['advertised_port'].nil?
        address = @value['address']
        raise Error, 'Invalid server address' unless address.is_a?(String) && address.bytesize.between?(1, 253) && address.ascii_only? && !address.include?('%') && !address.include?('/') && !address.include?('[')
        begin
          IPAddr.new(address)
        rescue IPAddr::InvalidAddressError
          raise Error, 'Invalid server address' if address.match?(/\A[\d.]+\z/) || !address.split('.').all? { |label| label.match?(/\A[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\z/) }
        end
        raise Error, 'Unsupported server configuration version' unless @value['v'].is_a?(Integer) && @value['v'] == 5
        bind = IPAddr.new(@value['bind'])
        raise Error, 'Bind must accept the internal IPv4 loopback dial' unless bind.ipv4? && (bind.to_i.zero? || bind.to_s == '127.0.0.1')
        { 'port' => 1..65_535, 'advertised_port' => 1..65_535, 'monitor_port' => 1..65_535, 'max_identities' => 1..128,
          'devices_per_user' => 1..64, 'admin_timeout' => 1..30, 'stop_timeout' => 1..60 }.each do |key, range|
          raise Error, 'Invalid server configuration limit' unless @value[key].is_a?(Integer) && range.cover?(@value[key])
        end
        raise Error, 'Identity allocation exceeds configured budget' if @value['devices_per_user'] > @value['max_identities']
        raise Error, 'Port settings conflict' if @value['port'] == @value['monitor_port']
        raise Error, 'Invalid namespace' unless @value['namespace'].match?(/\A[a-zA-Z0-9_-]+(?:\.[a-zA-Z0-9_-]+)*\z/) && @value['namespace'].bytesize <= 256 && @value['namespace'].split('.').all? { |part| part.bytesize <= 64 }
        raise Error, 'Invalid policy configuration' unless @value['allow'].is_a?(Array) && @value['deny'].is_a?(Array) && @value['allow'].length + @value['deny'].length <= 128
        @value['client_address'] ||= address
        client_uri = URI("tls://#{@value['client_address'].include?(':') ? "[#{@value['client_address']}]" : @value['client_address']}:#{@value['advertised_port']}")
        raise Error, 'Invalid client address' unless client_uri.host && client_uri.userinfo.nil? && client_uri.path.empty? && client_uri.query.nil? && client_uri.fragment.nil? && @value['client_address'].bytesize <= 253
        if @value['client_ca']
          path = @value['client_ca']
          raise Error, 'Invalid client CA path' unless path.is_a?(String) && path.start_with?('/') && !path.include?("\0") && path.bytesize <= 4096
        end
        NetworkConfiguration.new(self)
        validate_tls
      rescue IPAddr::InvalidAddressError, KeyError, TypeError, NoMethodError, URI::InvalidURIError
        raise Error, 'Invalid server configuration'
      end

      def [](key) = @value.fetch(key)
      def links = @links

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
        return if mode == 'local' && self['bind'] == '127.0.0.1'
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
      TOKEN = /\A[0-9a-f]{32}\z/
      VERSIONS = { 'network' => 6, 'api' => 1, 'version' => '0.7.0', 'core' => '5.0.0' }.freeze
      SERVICES = { 'runtime' => 'APP', 'enrollment' => 'APP', 'probe' => 'APP', 'auth' => 'AUTH', 'sys' => 'SYS' }.freeze
      attr_reader :directory, :value, :revision

      def initialize(config, migration: false)
        @config = config
        @directory = PrivateFiles.create_directory(config['state_dir'])
        raise Error, 'Offline migration incomplete' if !migration && File.exist?(File.join(@directory, 'migration-incomplete'))
        @lock = File.open(File.join(@directory, 'server.lock'), File::RDWR | File::CREAT | File::NOFOLLOW, 0o600)
        raise Error, 'Server state is already in use' unless @lock.flock(File::LOCK_EX | File::LOCK_NB)
        @path = File.join(@directory, 'state.json')
        @value = File.exist?(@path) ? JSON.parse(PrivateFiles.read(@path, maximum: 1_048_576), allow_duplicate_key: false) : fresh
        validate!(@value)
        commit(@value) unless File.exist?(@path)
        @revision = @value.fetch('revision')
      rescue StandardError
        @lock&.close
        raise
      end

      def close = @lock.close
      def candidate = Marshal.load(Marshal.dump(@value))
      def access_ready? = !@value.fetch('security_pending')
      def identity(login, state = @value) = "skvoz:#{state.fetch('authority_nonce')}:#{state.fetch('users').fetch(login).fetch('access_revision')}:#{login}"

      def commit(value)
        validate!(value)
        value['previous_tls'] = @value['tls'] if @value && @value['tls'] != value['tls']
        PrivateFiles.write(@path, JSON.generate(value))
        @value, @revision = value, value.fetch('revision')
      end

      def service(kind, state = @value)
        state.fetch('services').fetch(kind).merge('username' => "__skvoz_#{kind}")
      end

      def access_set(state = @value, previous: nil)
        devices = state.fetch('users').values.flat_map { |user| user.fetch('assigned') }.sort
        retired = previous ? previous.fetch('users').values.flat_map { |user| user.fetch('assigned') } - devices : []
        { 'revision' => state.fetch('revision'), 'devices' => devices, 'revoke' => retired.sort }
      end

      def mutate(operation, login, password: nil, hash: nil, verified: false)
        raise Error, 'Invalid login' unless self.class.login?(login)
        current = candidate
        users = current.fetch('users')
        exported_id = nil
        case operation
        when 'add'
          raise Error, 'Login already exists' if users.key?(login)
          count = @config['devices_per_user']
          raise Error, 'Identity pool admission exceeded' if users.values.sum { |user| user.fetch('ids').length } + count > @config['max_identities']
          ids = allocate(current, count)
          exported_id = ids.first
          users[login] = { 'hash' => hash || self.class.password_hash(password), 'ids' => ids, 'assigned' => [exported_id],
                           'devices' => {}, 'access_revision' => next_access_revision(current) }
        when 'device-add'
          user = users.fetch(login) { raise Error, 'Login does not exist' }
          raise Error, 'Password verification failed' unless verified
          exported_id = (user.fetch('ids') - user.fetch('assigned')).first
          raise Error, 'Device pool exhausted' unless exported_id
          user.fetch('assigned') << exported_id
        when 'reset-password'
          user = users.fetch(login) { raise Error, 'Login does not exist' }
          old_ids = user.fetch('ids')
          ids = allocate(current, old_ids.length)
          replacements = old_ids.zip(ids).to_h
          user['hash'] = hash || self.class.password_hash(password)
          user['ids'] = ids
          user['assigned'] = user.fetch('assigned').map { |id| replacements.fetch(id) }
          user['devices'] = user.fetch('devices').transform_values { |id| replacements.fetch(id) }
          user['access_revision'] = next_access_revision(current)
        when 'remove'
          raise Error, 'Login does not exist' unless users.delete(login)
        else
          raise Error, 'Unsupported user operation'
        end
        current['revision'] += 1
        validate!(current)
        [current, exported_id]
      end

      def enroll(login, token)
        raise Error, 'Invalid device token' unless token.is_a?(String) && token.match?(TOKEN)
        current = candidate
        user = current.fetch('users').fetch(login) { raise Error, 'Login does not exist' }
        return [nil, user.fetch('devices').fetch(token)] if user.fetch('devices').key?(token)
        id = (user.fetch('ids') - user.fetch('assigned')).first
        raise Error, 'Device pool exhausted' unless id
        user.fetch('assigned') << id
        user.fetch('devices')[token] = id
        current['revision'] += 1
        validate!(current)
        [current, id]
      end

      def permissions(login, state = @value, ids: nil)
        namespace = @config['namespace']
        publish, subscribe = ["skvoz.enroll.v4.#{login}"], ["skvoz.enroll.reply.#{login}.*"]
        (ids || state.fetch('users').fetch(login).fetch('assigned')).each do |id|
          publish.concat(["#{namespace}.join.0.#{id}", "#{namespace}.lane.0.*.#{id % 8}.*.#{id}.*"])
          subscribe.concat(["#{namespace}.join.#{id}.*", "#{namespace}.lane.#{id}.*.*.*.*.*"])
        end
        { 'publish' => publish, 'subscribe' => subscribe }
      end

      def service_permissions(kind)
        namespace = @config['namespace']
        case kind
        when 'runtime'
          { 'publish' => ["#{namespace}.join.*.0", "#{namespace}.lane.*.*.0.*.0.*"],
            'subscribe' => ["#{namespace}.join.0.*", "#{namespace}.lane.0.*.*.*.*.*"] }
        when 'enrollment'
          { 'publish' => ['skvoz.enroll.reply.*.*'], 'subscribe' => ['skvoz.enroll.v4.*'] }
        when 'auth'
          { 'publish' => ['$SYS._INBOX.>', '_INBOX.>'], 'subscribe' => ['$SYS.REQ.USER.AUTH'] }
        when 'sys'
          { 'publish' => ['$SYS.REQ.SERVER.*.KICK', '$SYS.REQ.SERVER.*.CONNZ', '$SYS.REQ.SERVER.*.LEAFZ', '$SYS.REQ.SERVER.PING.IDZ'],
            'subscribe' => ['_INBOX.skvoz.sys.>', '$SYS.ACCOUNT.APP.CONNECT', '$SYS.ACCOUNT.APP.DISCONNECT'] }
        when 'probe'
          { 'publish' => [], 'subscribe' => [] }
        else
          raise Error, 'Unknown service identity'
        end
      end

      def auth_public
        auth = @value.fetch('auth')
        { 'issuer' => NatsAuth::Key.new(:account, auth.fetch('issuer_seed')).public_key,
          'xkey' => NatsAuth::Key.new(:curve, auth.fetch('xkey_seed')).public_key }
      end

      def nats_config(state = @value, tls: state.fetch('tls'))
        raise Error, 'TLS material missing' unless tls
        accounts = %w[APP AUTH SYS].to_h { |name| [name, { 'users' => [] }] }
        SERVICES.each do |kind, account|
          credential = service(kind, state)
          accounts.fetch(account).fetch('users') << { 'user' => credential.fetch('username'), 'password' => credential.fetch('hash'),
            'permissions' => service_permissions(kind), 'allowed_connection_types' => ['STANDARD'] }
        end
        auth = state.fetch('auth')
        value = { 'server_name' => "skvoz-#{state.fetch('authority_nonce')}", 'listen' => "#{@config['bind']}:#{@config['port']}",
          'http' => "127.0.0.1:#{@config['monitor_port']}", 'max_payload' => 65_588, 'max_pending' => 4_194_304,
          'max_connections' => @config['max_identities'] * 10 + 32, 'write_deadline' => '2s', 'system_account' => 'SYS',
          'tls' => { 'cert_file' => tls.fetch('certificate'), 'key_file' => tls.fetch('key'), 'handshake_first' => false },
          'accounts' => accounts, 'authorization' => { 'timeout' => '3s', 'auth_callout' => {
            'account' => 'AUTH', 'issuer' => NatsAuth::Key.new(:account, auth.fetch('issuer_seed')).public_key,
            'xkey' => NatsAuth::Key.new(:curve, auth.fetch('xkey_seed')).public_key,
            'auth_users' => SERVICES.keys.map { |kind| "__skvoz_#{kind}" } } } }
        value['leafnodes'] = { 'compression' => 'off', 'remotes' => @config.links.remotes(directory) } if @config.links
        JSON.generate(value)
      end

      def profile(policy, pending: false)
        credential = service('runtime')
        tls = @value.fetch('tls')
        access = pending ? { 'revision' => 0, 'devices' => [], 'revoke' => [] } : access_set
        ids = access.fetch('devices')
        { 'v' => 3, 'role' => 'server', 'network' => policy.network, 'server' => policy.server,
          'core' => { 'url' => "tls://127.0.0.1:#{@config['port']}", 'tls_server_name' => @config['address'],
            'trust' => tls['ca'] ? 'managed_ca' : 'system', 'ca_file' => tls['ca'],
            'username' => credential.fetch('username'), 'password' => credential.fetch('password'),
            'namespace' => @config['namespace'], 'peer_id' => '0', 'membership' => 'allowlist', 'allowed_peers' => ids.map(&:to_s), 'initiate' => [] },
          'access' => access }
      end

      def client_ca
        @config['client_ca'] || (@config['client_address'] == @config['address'] ? @value.fetch('tls')['ca'] : nil)
      end

      def export(login, id, password)
        { 'v' => 4, 'address' => @config['client_address'], 'port' => @config['advertised_port'], 'username' => login,
          'password' => password, 'namespace' => @config['namespace'], 'peer_id' => id, 'network_runtime' => VERSIONS,
          'allowed_peers' => [0], 'initiate' => [0], 'shards' => 8, 'trust' => client_ca ? 'managed_ca' : 'system' }
      end

      def self.login?(login) = login.is_a?(String) && login.match?(LOGIN) && !login.start_with?('__skvoz_')
      def self.password?(password) = password.is_a?(String) && password.valid_encoding? && !password.include?("\0") && password.bytesize.between?(12, 72)

      def self.password_hash(password)
        raise Error, 'Password must contain 12 to 72 UTF-8 bytes without NUL' unless password?(password)
        BCrypt::Password.create(password, cost: 12).to_s
      end

      private

      def fresh
        credentials = SERVICES.to_h do |kind, _account|
          password = SecureRandom.urlsafe_base64(32)
          [kind, { 'password' => password, 'hash' => self.class.password_hash(password) }]
        end
        { 'v' => 3, 'revision' => 1, 'access_revision' => 0, 'next_id' => 1, 'namespace' => @config['namespace'],
          'authority_nonce' => SecureRandom.hex(16), 'security_pending' => false, 'users' => {}, 'services' => credentials,
          'auth' => { 'issuer_seed' => NatsAuth::Key.new(:account).seed, 'xkey_seed' => NatsAuth::Key.new(:curve).seed }, 'tls' => nil }
      end

      def allocate(state, count)
        start = state.fetch('next_id')
        raise Error, 'Identity allocator exhausted' unless start + count < 1 << 63
        state['next_id'] += count
        (start...(start + count)).to_a
      end

      def next_access_revision(state)
        raise Error, 'Access revision exhausted' if state.fetch('access_revision') >= (1 << 63) - 1
        state['access_revision'] += 1
      end

      def validate!(state)
        keys = %w[v revision access_revision next_id namespace authority_nonce security_pending users services auth tls]
        raise Error, 'Unsupported persistent state' unless state.is_a?(Hash) && (state.keys - keys - ['previous_tls']).empty? && (keys - state.keys).empty? && state['v'] == 3 && state['namespace'] == @config['namespace']
        raise Error, 'Invalid state identity' unless state['authority_nonce'].is_a?(String) && state['authority_nonce'].match?(TOKEN) && [true, false].include?(state['security_pending'])
        %w[revision next_id].each { |key| raise Error, 'Invalid state allocator' unless state[key].is_a?(Integer) && state[key].positive? && state[key] < 1 << 63 }
        raise Error, 'Invalid access revision' unless state['access_revision'].is_a?(Integer) && state['access_revision'].between?(0, (1 << 63) - 1)
        users = state['users']
        raise Error, 'Invalid user database' unless users.is_a?(Hash) && users.size <= 128
        ids, revisions = [], []
        users.each do |login, user|
          raise Error, 'Invalid user record' unless self.class.login?(login) && user.is_a?(Hash) && user.keys.sort == %w[access_revision assigned devices hash ids]
          valid_hash!(user['hash'])
          reserved, assigned, devices = user.values_at('ids', 'assigned', 'devices')
          raise Error, 'Invalid device allocation' unless reserved.is_a?(Array) && reserved.size.between?(1, 64) && reserved.uniq == reserved && reserved.all? { |id| id.is_a?(Integer) && id.positive? && id < state['next_id'] } && assigned.is_a?(Array) && assigned.uniq == assigned && (assigned - reserved).empty?
          raise Error, 'Invalid device bindings' unless devices.is_a?(Hash) && devices.size <= 64 && devices.keys.all? { |token| token.is_a?(String) && token.match?(TOKEN) } && devices.values.uniq == devices.values && (devices.values - assigned).empty?
          revision = user['access_revision']
          raise Error, 'Invalid user access revision' unless revision.is_a?(Integer) && revision.positive? && revision <= state['access_revision']
          ids.concat(reserved); revisions << revision
        end
        raise Error, 'Duplicate or excessive identity allocation' unless ids.uniq == ids && ids.size <= @config['max_identities'] && revisions.uniq == revisions
        raise Error, 'Invalid service credentials' unless state['services'].is_a?(Hash) && state['services'].keys.sort == SERVICES.keys.sort
        state['services'].each_value do |credential|
          raise Error, 'Invalid service credential' unless credential.is_a?(Hash) && credential.keys.sort == %w[hash password] && credential['password'].is_a?(String) && credential['password'].match?(/\A[a-zA-Z0-9_-]{43}\z/)
          valid_hash!(credential['hash'])
        end
        passwords = state['services'].values.map { |item| item['password'] }
        raise Error, 'Service credentials shared' unless passwords.uniq == passwords
        auth = state['auth']
        raise Error, 'Invalid callout keys' unless auth.is_a?(Hash) && auth.keys.sort == %w[issuer_seed xkey_seed]
        issuer = NatsAuth::Key.new(:account, auth.fetch('issuer_seed'))
        curve = NatsAuth::Key.new(:curve, auth.fetch('xkey_seed'))
        request = { 'exp' => Time.now.to_i + 2, 'nats' => { 'user_nkey' => NatsAuth.encode(160, "\0" * 32),
                    'server_id' => { 'id' => NatsAuth.encode(104, "\0" * 32), 'xkey' => curve.public_key } } }
        users.each do |login, user|
          # Reserve capacity only when every reserved device fits one native grant.
          NatsAuth.response(request, issuer, curve, identity: identity(login, state),
                            permissions: permissions(login, state, ids: user.fetch('ids')))
        end
        %w[tls previous_tls].each do |kind|
          material = state[kind]
          next if material.nil?
          raise Error, 'Invalid TLS state' unless material.is_a?(Hash) && (material.keys - %w[certificate key ca]).empty? && %w[certificate key].all? { |key| material[key].is_a?(String) && material[key].start_with?('/') } && material.values.compact.all? { |path| path.is_a?(String) && path.bytesize <= 4096 && !path.include?("\0") }
        end
      rescue KeyError, ArgumentError, RbNaCl::CryptoError, BCrypt::Errors::InvalidHash
        raise Error, 'Persistent state invalid'
      end

      def valid_hash!(hash)
        raise Error, 'Invalid password hash' unless hash.is_a?(String) && hash.match?(/\A\$2[aby]\$12\$[.\/A-Za-z0-9]{53}\z/)
      end
    end
  end
end
