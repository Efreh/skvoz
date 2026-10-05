# frozen_string_literal: true
require 'digest'
require_relative 'spec_helper'
require_relative 'support/system'

RSpec.describe 'Server container TCP resources', integration: true, capacity: true do
  include ServerSystem

  it 'accounts the actual Ruby, NATS and runtime cgroup separately from consumers' do
    shared = Pathname.new(ENV.fetch('SKVOZ_CAPACITY_DIRECTORY'))
    hold = Integer(ENV.fetch('SKVOZ_CAPACITY_HOLD', '1800'))
    raise ArgumentError, 'Capacity hold outside finite bounds' unless (0..1800).cover?(hold)
    report = { 'scope' => 'server container: RSpec driver, Ruby service, NATS, Rust runtime; consumers in a separate cgroup', 'runtime_sha256' => Digest::SHA256.file('/usr/local/bin/skvoz-network-runtime').hexdigest, 'samples' => [] }
    Dir.mktmpdir('server-capacity-', shared) do |temporary|
      directory = Pathname.new(temporary)
      certificates(directory)
      server = ServerSystem::Server.new(directory, overrides: { 'bind' => '0.0.0.0', 'port' => 4222 },
        allow: [{ 'cidr' => '127.0.0.0/8', 'protocols' => [6], 'ports' => [9000, 9001, 9002, 9003] }])
      profiles = 16.times.map do |index|
        bundle = server.command('add', "capacity#{index}")
        ca = directory.join("client-ca#{index}.pem")
        ca.write(bundle.fetch('ca_pem')); ca.chmod(0o600)
        limits = Skvoz::Server::NetworkConfiguration::LIMITS.merge('ip_sessions' => 1, 'core_streams' => 512,
          'lease_identities' => 1, 'core_receive_bytes' => 33554432, 'core_send_bytes' => 2097152,
          'runtime_buffer_bytes' => 100663296, 'runtime_buffer_records' => 16384)
        profile = directory.join("client#{index}.json")
        private_json(profile, { v: 1, role: 'client', server: nil,
          core: { url: "tls://127.0.0.1:#{bundle.fetch('port')}", tls_server_name: bundle.fetch('address'), trust: 'managed_ca',
            ca_file: ca.to_s, username: bundle.fetch('username'), password: bundle.fetch('password'), namespace: bundle.fetch('namespace'),
            peer_id: bundle.fetch('peer_id').to_s, membership: 'allowlist', allowed_peers: ['0'], initiate: ['0'] },
          network: { families: [4], max_mtu: 1400, channels: 1, limits: } })
        profile.to_s
      end
      started = monotonic
      sample = lambda do |phase|
        pids = [server.process] + File.read("/proc/#{server.process}/task/#{server.process}/children").split.map(&:to_i)
        processes = pids.filter_map do |pid|
          status = File.read("/proc/#{pid}/status")
          { 'pid' => pid, 'name' => status[/^Name:\s+(.*)$/, 1], 'rss_kib' => status[/^VmRSS:\s+(\d+)/, 1].to_i,
            'rss_peak_kib' => status[/^VmHWM:\s+(\d+)/, 1].to_i, 'fd' => Dir.children("/proc/#{pid}/fd").size,
            'nofile' => File.read("/proc/#{pid}/limits")[/^Max open files\s+(.*)$/, 1] }
        rescue Errno::ENOENT
          nil
        end
        memory = File.readlines('/sys/fs/cgroup/memory.stat').to_h { |line| key, value = line.split; [key, Integer(value)] }
        value = { 'phase' => phase, 'elapsed' => monotonic - started, 'processes' => processes,
          'memory_stat' => memory.slice('anon', 'file', 'sock', 'kernel', 'kernel_stack', 'slab'),
          'current' => Integer(File.read('/sys/fs/cgroup/memory.current')), 'peak' => Integer(File.read('/sys/fs/cgroup/memory.peak')) }
        report['samples'] << value
        private_json(shared.join('server-report.json'), report)
        value
      end
      before = sample.call('before')
      initial_ledger = server.command('health').fetch('connector').fetch('buffer_bytes')
      private_json(shared.join('ready.json'), { profiles:, hold: })
      deadline = monotonic + hold + 600
      until shared.join('consumer-done.json').file?
        raise Timeout::Error, 'External capacity consumer deadline' if monotonic >= deadline
        raise IOError, 'Real server process exited' if process_dead(server.process)
        consumer_phase = shared.join('consumer-report.json').file? ? JSON.parse(shared.join('consumer-report.json').read).dig('samples', -1, 'phase') : 'waiting'
        sample.call(consumer_phase)
        sleep 2
      end
      result = JSON.parse(shared.join('consumer-done.json').read)
      expect(result.fetch('status')).to eq('passed')
      final_health = wait_until(timeout: 30) do
        health = server.command('health')
        health if health.dig('connector', 'tcp_open') == 0
      end
      expect(final_health).to include('healthy' => true)
      report['after_cleanup'] = final_health.fetch('connector')
      final = sample.call('after-cleanup')
      final.fetch('processes').each do |process|
        old = before.fetch('processes').find { |entry| entry.fetch('pid') == process.fetch('pid') }
        expect(old).not_to be_nil
        allowance = process.fetch('name').start_with?('skvoz-network') ? 8 : process.fetch('name') == 'nats-server' ? 64 : 4
        expect(process.fetch('fd')).to be <= old.fetch('fd') + allowance
        expect(process.fetch('nofile').split.take(2)).to eq(%w[8192 8192])
      end
      expect(final_health.fetch('connector').fetch('buffer_bytes')).to be <= initial_ledger + 1048576
      expect(server.log.read).not_to match(/Network drive failure|Network readiness lost|Transport shard failure|Peer watermark timeout|Peer envelope sequence failure|Peer stream receive failure/)
      peak = Integer(File.read('/sys/fs/cgroup/memory.peak'))
      runtime_peak = report.fetch('samples').flat_map { |sample| sample.fetch('processes') }
        .select { |process| process.fetch('name').start_with?('skvoz-network') }.map { |process| process.fetch('rss_peak_kib') }.max
      expect(runtime_peak).not_to be_nil
      expect(runtime_peak).to be <= 512 * 1024
      expect(peak).to be <= 1024 * 1024 * 1024
      report.merge!('status' => 'passed', 'server_runtime_peak_kib' => runtime_peak, 'whole_server_container_peak_bytes' => peak)
    rescue StandardError => error
      report.merge!('status' => 'failed', 'error' => "#{error.class}: #{error.message}")
      raise
    ensure
      server&.close
      private_json(shared.join('server-report.json'), report)
    end
  end
end
