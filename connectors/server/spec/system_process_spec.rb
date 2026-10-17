# frozen_string_literal: true
require_relative 'spec_helper'
require_relative 'support/system'

RSpec.describe 'System process checks' do
  def reap_child(pid)
    Process.kill('KILL', pid) rescue Errno::ESRCH
    Process.waitpid(pid) rescue Errno::ECHILD
  end

  it 'reports a live process as alive' do
    expect(ServerSystem.process_dead(Process.pid)).to be(false)
  end

  it 'reports both an unreaped zombie and a reaped child as dead' do
    pid = fork { exit! 0 }
    ServerSystem.wait_until(timeout: 3) { File.read("/proc/#{pid}/stat").split(') ', 2).last.split.first == 'Z' }
    expect(ServerSystem.process_dead(pid)).to be(true)
    Process.waitpid(pid)
    expect(ServerSystem.process_dead(pid)).to be(true)
  ensure
    reap_child(pid) if pid
  end

  it 'treats ESRCH from reading an already opened proc stat as dead' do
    pid = fork { sleep 30 }
    path = "/proc/#{pid}/stat"
    read_side_esrch = false
    allow(File).to receive(:read).and_call_original
    allow(File).to receive(:read).with(path) do
      File.open(path) do |stat|
        Process.kill('KILL', pid)
        Process.waitpid(pid)
        begin
          stat.read
        rescue Errno::ESRCH
          read_side_esrch = true
          raise
        end
      end
    end
    expect(ServerSystem.process_dead(pid)).to be(true)
    expect(read_side_esrch).to be(true)
  ensure
    reap_child(pid) if pid
  end

  it 'propagates unrelated proc stat read failures' do
    path = "/proc/#{Process.pid}/stat"
    allow(File).to receive(:read).and_call_original
    allow(File).to receive(:read).with(path).and_raise(Errno::EACCES)
    expect { ServerSystem.process_dead(Process.pid) }.to raise_error(Errno::EACCES)
  end
end
