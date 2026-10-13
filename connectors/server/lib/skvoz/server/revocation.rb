# frozen_string_literal: true
require_relative 'nats_connection'
require_relative 'nats_auth'

module Skvoz
  module Server
    class Revocation
      MAX_CONNECTIONS = 1312
      PAGE_SIZE = 8
      MAX_SERVERS = 32
      attr_reader :failure

      def initialize(config, state)
        @config, @state = config, state
        @records, @servers, @scans, @inventory_seen = {}, {}, {}, {}
        @largest_page_count = 0
        @inventory_ready = false
        @unverified_entry_paths = entry_count
        @queries = Async::Semaphore.new(1)
      end

      def start(task)
        @connection&.stop
        @inventory_ready = false
        @connection = NatsConnection.new(@config, @state, 'sys', max_body: 65_588).start(task)
        %w[CONNECT DISCONNECT].each do |kind|
          @connection.subscribe("$SYS.ACCOUNT.APP.#{kind}") do |message|
            event = JSON.parse(message.fetch(:data), max_nesting: 16, allow_duplicate_key: false)
            server = event.fetch('server').fetch('id')
            cid = event.fetch('client').fetch('id')
            register_server(server)
            @records.delete([server, cid]) if kind == 'DISCONNECT'
            if kind == 'CONNECT' && (record = @records[[server, cid]])
              client = event.fetch('client')
              record[:bound] = true if client['kind'] == 'Client' && client['acc'] == 'APP' && client['user'] == record[:identity]
            end
            @dirty = true
          end
        end
        @kick_reply = "_INBOX.skvoz.sys.kick.#{SecureRandom.hex(16)}"
        # Stock KICK requires a reply subject. Replies trigger reconciliation;
        # only CONNZ absence or DISCONNECT can confirm physical closure.
        @connection.subscribe(@kick_reply) { |_message| @dirty = true }
        @failure = nil
        discover
        # Cold inventory can require many bounded pages over a leaf. Keep its
        # continuation for refresh; readiness, not a five-second restart loop,
        # fences admission until the complete authoritative pass finishes.
        revoke
        self
      rescue StandardError
        @connection&.stop
        raise
      end

      def connected? = @failure.nil? && @connection&.ready?
      def ready? = @inventory_ready && connected?

      def reserve(request, identity)
        return false unless ready?
        server = request.fetch('iss')
        cid = request.fetch('nats').fetch('client_info').fetch('id')
        key = [server, cid]
        return false if @records.size >= MAX_CONNECTIONS && !@records.key?(key)
        register_server(server)
        return false unless @servers.key?(server)
        existing = @records[key]
        return false if existing && (existing[:identity] != identity || existing[:user_nkey] != request.dig('nats', 'user_nkey'))
        @records[key] = { identity: identity, user_nkey: request.fetch('nats').fetch('user_nkey'),
                          expires: request.fetch('exp') + 5, bound: false }
        true
      end

      def pending
        current = current_names
        @records.count { |_key, row| !current.include?(row.fetch(:identity)) }
      end

      def statistics
        { 'connections' => @records.size, 'pending_connections' => pending,
          'native_servers' => @servers.size, 'unreachable_connections' => @records.count { |(server, _cid), _row| !@servers.key?(server) },
          'unverified_entry_paths' => @unverified_entry_paths,
          'largest_page_count' => @largest_page_count, 'inventory_complete' => @inventory_ready }
      end

      def revoke(inventory: !@inventory_ready)
        @priority_revoke = true
        deadline = clock + 5
        complete = false
        @queries.acquire do
          loop do
            retired = reconcile_retired(deadline: deadline)
            complete = if inventory && clock < deadline
                         @startup_scan = true
                         retired && reconcile(deadline: deadline)
                       else
                         retired
                       end
            if complete && pending.zero? && @discovery_complete
              @inventory_ready = true
              break
            end
            break if clock >= deadline
            Async::Task.current.sleep(0.05)
          end
        end
        { 'pending_connections' => pending, 'unverified_entry_paths' => @unverified_entry_paths,
          'reconciliation_complete' => complete && pending.zero? && @discovery_complete }
      ensure
        @startup_scan = @priority_revoke = false
      end

      def refresh
        return if @last_refresh && clock - @last_refresh < 1 && !@dirty
        @queries.acquire do
          discover
          deadline = clock + 2
          reconcile_retired(deadline: deadline)
          complete = reconcile(deadline: deadline)
          @inventory_ready = true if complete && pending.zero?
        end
        @last_refresh = clock
        @dirty = false
      rescue Error, Async::TimeoutError, IOError, SystemCallError
        @failure = 'revocation_service_unavailable'
      end

      def stop = @connection&.stop

      private

      def clock = Process.clock_gettime(Process::CLOCK_MONOTONIC)

      def current_names
        @state.value.fetch('users').keys.map { |login| @state.identity(login) }
      end

      def entry_count = @config.links ? @config.links.value.fetch('entries').size : 0

      def register_server(id)
        NatsAuth.decode(id, :server)
      end

      def discover
        reached = {}
        @connection.query('$SYS.REQ.SERVER.PING.IDZ', {}, timeout: 0.3, collect: true).each do |reply|
          id = reply.fetch('id')
          NatsAuth.decode(id, :server)
          name = reply.fetch('name')
          raise Error, 'Native server name invalid' unless name.is_a?(String) && name.valid_encoding? && name.bytesize.between?(1, 128)
          raise Error, 'Duplicate native server response' if reached.key?(id)
          reached[id] = name
        end
        raise Error, 'Native server inventory exceeds limit' if reached.size > MAX_SERVERS
        local = @connection.server_id
        NatsAuth.decode(local, :server)
        @unverified_entry_paths = entry_count
        if entry_count.positive?
          response = @connection.query("$SYS.REQ.SERVER.#{local}.LEAFZ", { account: 'SYS', subscriptions: false }, timeout: 0.3)
          data = response.fetch('data')
          rows = data.fetch('leafs')
          raise Error, 'Native local leaf inventory invalid' unless response.dig('server', 'id') == local && response['error'].nil? && data['server_id'] == local && rows.is_a?(Array) && rows.size <= entry_count && data['leafnodes'] == rows.size
          names = rows.map do |row|
            raise Error, 'Native local SYS leaf invalid' unless row.is_a?(Hash) && row['account'] == 'SYS' && row['is_spoke'] == true && row['name'].is_a?(String) && row['name'].valid_encoding? && row['name'].bytesize.between?(1, 128)
            row.fetch('name')
          end
          verified = names.count do |name|
            names.count(name) == 1 && reached.count { |id, server_name| id != local && server_name == name } == 1
          end
          @unverified_entry_paths -= verified
        end
        @discovery_complete = reached.key?(local) && @unverified_entry_paths.zero?
        # Discovery describes reachable brokers, not confirmed physical socket closure.
        # Retain their tracked CIDs separately, including pending partitioned revocations.
        @inventory_seen.delete_if { |server, _seen| !reached.key?(server) }
        @inventory_ready = false if reached.keys.any? { |server| !@servers.key?(server) }
        @inventory_ready = false unless @discovery_complete
        @servers = reached
        @scans.delete_if { |server, _scan| !reached.key?(server) }
      end

      def connection_page(server, options, deadline:)
        remaining = deadline - clock
        raise Error, 'Native reconciliation deadline exceeded' unless remaining.positive?
        response = Async::Task.current.with_timeout(remaining) do
          @connection.query("$SYS.REQ.SERVER.#{server}.CONNZ", { auth: true, acc: 'APP', sort: 'cid' }.merge(options), timeout: [remaining, 0.3].min)
        end
        validate_page(response, server, options)
      end

      def validate_page(response, server, options)
        raise Error, 'Native response server mismatch' unless response.dig('server', 'id') == server && response['error'].nil?
        data = response.fetch('data')
        rows, total = data.values_at('connections', 'total')
        limit = options.fetch(:limit)
        raise Error, 'Native connection inventory exceeds limit' unless data['server_id'] == server && rows.is_a?(Array) && rows.size <= limit && total.is_a?(Integer) && total.between?(0, MAX_CONNECTIONS) && data['num_connections'] == rows.size && data['offset'] == options.fetch(:offset, 0)
        rows.each do |row|
          raise Error, 'Native connection record invalid' unless row.is_a?(Hash) && row['cid'].is_a?(Integer) && row['cid'].positive? && row['cid'] < 1 << 64
        end
        data
      end

      def confirm_missing(server, rows, deadline:, tracked: @records)
        waiting = tracked.select { |(owner, cid), _row| owner == server && !rows.key?(cid) }
        return if waiting.empty?
        prefix = "_INBOX.skvoz.sys.scan.#{SecureRandom.hex(16)}"
        changed = Async::Condition.new
        # One protected subscription and a finite pending set avoid serial leaf RTTs.
        # Only a correlated authoritative response, never a KICK ACK, proves absence.
        sid = @connection.subscribe("#{prefix}.*") do |message|
          token = message.fetch(:subject).delete_prefix(prefix + '.')
          raise Error, 'Native scan correlation invalid' unless token.match?(/\A[1-9][0-9]{0,19}\z/)
          key = [server, Integer(token)]
          next unless (captured = waiting.delete(key))
          response = JSON.parse(message.fetch(:data), max_nesting: 32, allow_duplicate_key: false)
          data = validate_page(response, server, { cid: key[1], limit: 1 })
          if (target = data.fetch('connections').first)
            raise Error, 'Native CID mismatch' unless target['cid'] == key[1]
            observe(server, target)
          elsif @records[key].equal?(captured) && (captured[:bound] || captured[:expires] <= Time.now.to_i)
            @records.delete(key)
          end
          changed.signal
        end
        waiting.keys.each do |key|
          raise Error, 'Native reconciliation deadline exceeded' if clock >= deadline
          @connection.publish("$SYS.REQ.SERVER.#{server}.CONNZ", JSON.generate(auth: true, acc: 'APP', cid: key[1], limit: 1), reply: "#{prefix}.#{key[1]}")
        end
        remaining = deadline - clock
        raise Error, 'Native reconciliation deadline exceeded' unless remaining.positive?
        Async::Task.current.with_timeout(remaining) do
          @connection.flush
          changed.wait until waiting.empty? || !@connection.ready?
          raise Error, 'Native service response unavailable' unless waiting.empty?
        end
      ensure
        @connection.unsubscribe(sid) if sid
      end

      def connections(server, deadline:)
        scan = (@scans[server] ||= { offset: 0, rows: {}, total: nil, stable: true, pages: 0 })
        rows = scan.fetch(:rows)
        loop do
          return [rows, false] if @priority_revoke && !@startup_scan || deadline - clock <= 0.35
          data = connection_page(server, { offset: scan.fetch(:offset), limit: PAGE_SIZE }, deadline: deadline)
          page = data.fetch('connections')
          scan[:stable] &&= scan[:total].nil? || scan[:total] == data.fetch('total')
          scan[:total] ||= data.fetch('total')
          scan[:pages] += 1
          page.each { |row| rows[row.fetch('cid')] = row }
          raise Error, 'Native connection inventory exceeds limit' if @scans.values.sum { |value| value.fetch(:rows).size } > MAX_CONNECTIONS
          scan[:offset] += page.size
          break if scan.fetch(:offset) >= data.fetch('total')
          raise Error, 'Native connection pagination stalled' if page.empty? || scan.fetch(:offset) > MAX_CONNECTIONS
        end
        @largest_page_count = [@largest_page_count, scan.fetch(:pages)].max
        @scans.delete(server)
        [rows, scan.fetch(:stable) && rows.size == scan.fetch(:total)]
      end

      def observe(server, row)
        key = [server, row.fetch('cid')]
        name = row['authorized_user']
        return if row['kind'] == 'Leafnode'
        raise Error, 'Native application connection kind invalid' unless row['kind'] == 'Client'
        protected = State::SERVICES.filter_map { |kind, account| "__skvoz_#{kind}" if account == 'APP' }
        if protected.include?(name)
          @records.delete(key)
          return
        end
        raise Error, 'Native application identity invalid' unless name.is_a?(String) && name.bytesize <= 256
        # Read the current durable generation after each yielding native query.
        if !current_names.include?(name) || @records.size >= MAX_CONNECTIONS && !@records.key?(key)
          @connection.publish("$SYS.REQ.SERVER.#{server}.KICK", JSON.generate(cid: row.fetch('cid')), reply: @kick_reply)
        end
        raise Error, 'Native connection tracking budget exceeded' if @records.size >= MAX_CONNECTIONS && !@records.key?(key)
        if (existing = @records[key]) && existing[:identity] == name
          # Authoritative inventory confirms the same admitted generation;
          # retain the signed attempt's user NKey for any same-CID re-auth.
          existing[:bound] = true
        else
          @records[key] = { identity: name, expires: nil, bound: true }
        end
      end

      def reconcile(deadline:)
        remaining = deadline - clock
        return false unless remaining.positive?
        Async::Task.current.with_timeout(remaining) { reconcile_servers(deadline: deadline) }
      rescue Async::TimeoutError
        false
      end

      def reconcile_retired(deadline:)
        current = current_names
        targets = @records.reject { |_key, row| current.include?(row.fetch(:identity)) }
        @servers.keys.each do |server|
          scoped = targets.select { |(owner, _cid), _row| owner == server }
          next if scoped.empty?
          scoped.each_key do |key|
            @connection.publish("$SYS.REQ.SERVER.#{server}.KICK", JSON.generate(cid: key[1]), reply: @kick_reply)
          end
          remaining = deadline - clock
          return false unless remaining.positive?
          Async::Task.current.with_timeout(remaining) { confirm_missing(server, {}, deadline: deadline, tracked: scoped) }
        rescue Async::TimeoutError
          return false
        end
        # Unknown legacy clients still require the exhaustive startup scan.
        # Ordinary mutations prioritize their exact authorized attempts, so a full
        # healthy inventory cannot impose one remote RTT per healthy page.
        pending.zero?
      end

      def reconcile_servers(deadline:)
        current = current_names
        complete = @discovery_complete && @records.all? { |(server, _cid), row| @servers.key?(server) || current.include?(row.fetch(:identity)) }
        @servers.keys.each do |server|
          break if clock >= deadline
          next if @inventory_seen.key?(server)
          begin
            rows, stable = connections(server, deadline: deadline)
            rows.each_value { |row| observe(server, row) }
            if stable
              confirm_missing(server, rows, deadline: deadline)
              @inventory_seen[server] = true
            end
            @connection.flush
          rescue Async::TimeoutError
            # A partition leaves physical sockets pending; no stale generation is accepted.
            complete = false
          end
        end
        finished = @discovery_complete && @servers.keys.all? { |server| @inventory_seen.key?(server) }
        if finished
          @inventory_ready = true if complete && pending.zero?
          @inventory_seen.clear
        end
        complete && finished
      end
    end
  end
end
