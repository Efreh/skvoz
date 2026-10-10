# frozen_string_literal: true
require_relative 'network_configuration'
require_relative 'process'
require 'io/nonblock'

module Skvoz
  module Server
    module Bootstrap
      APP_UID = 10001
      APP_GID = 10001
      HELPER_STATE = '/var/lib/skvoz-network/state'

      def self.unprivileged!
        raise Error, 'Server UID must be 10001' unless Process.uid == APP_UID && Process.euid == APP_UID && Process.gid == APP_GID && Process.egid == APP_GID
        status = File.read('/proc/self/status')
        %w[CapInh CapPrm CapEff CapBnd CapAmb].each do |key|
          raise Error, 'Server capabilities must be empty' unless status.match?(/^#{key}:\s+0+$/)
        end
        raise Error, 'No-new-privileges required' unless status.match?(/^NoNewPrivs:\s+1$/)
      end

      def self.start(config)
        raise Error, 'Root bootstrap required' unless Process.euid.zero?
        policy = NetworkConfiguration.new(config).inventory!(config)
        value = Marshal.load(Marshal.dump(config.value))
        if config.egress?
          value['network']['server_addresses'] = policy.server['server_addresses']
          value['network']['management_endpoints'] = policy.server['management_endpoints']
        end
        # No CHOWN capability: the application creates its own private files.
        pid = fork do
          Process.groups = []
          Process::GID.change_privilege(APP_GID)
          Process::UID.change_privilege(APP_UID)
          PrivateFiles.create_directory(config['state_dir'])
          PrivateFiles.write(File.join(config['state_dir'], 'bootstrap.json'), JSON.generate(value))
          exit! 0
        end
        _, status = Process.waitpid2(pid)
        raise Error, 'Application state initialization failed' unless status.success?
        helper_pid = nil
        owner = nil
        if policy.ip?
          PrivateFiles.create_directory(File.dirname(HELPER_STATE))
          PrivateFiles.create_directory(HELPER_STATE)
          helper_config = File.join(File.dirname(HELPER_STATE), 'helper.json')
          bytes = JSON.generate('v' => 1, 'role' => 'server', 'state_dir' => HELPER_STATE,
            'policy' => { 'network' => policy.network, 'server' => policy.server })
          raise Error, 'Helper policy exceeds limit' if bytes.bytesize > 32768
          PrivateFiles.write(helper_config, bytes)
          owner, helper = UNIXSocket.pair
          owner.nonblock = helper.nonblock = true
          helper_pid = Process.spawn('setpriv', '--no-new-privs', '--bounding-set=-all,+net_admin',
            '--inh-caps=-all', '--ambient-caps=-all', config['helper_binary'], '--config', helper_config, '--control-fd', '3',
            3 => helper, close_others: true, pgroup: true, in: File::NULL)
          helper.close
        end
        environment = { 'SKVOZ_BOOTSTRAPPED' => '1', 'SKVOZ_HELPER_PID' => helper_pid&.to_s,
          'SKVOZ_HELPER_FD' => owner ? '3' : nil }
        maps = owner ? { 3 => owner } : {}
        exec(environment, 'setpriv', '--no-new-privs', '--reuid', APP_UID.to_s, '--regid', APP_GID.to_s,
          '--clear-groups', '--bounding-set=-all', '--inh-caps=-all', '--ambient-caps=-all',
          RbConfig.ruby, File.expand_path('../../../bin/skvoz-server', __dir__), 'serve',
          '--config', File.join(config['state_dir'], 'bootstrap.json'), **maps, close_others: true)
      ensure
        helper&.close unless helper&.closed?
        owner&.close unless owner&.closed?
      end
    end
  end
end
