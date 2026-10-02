# frozen_string_literal: true
require_relative 'spec_helper'

RSpec.describe Skvoz::Server::Enrollment do
  it 'finishes an in-flight durable mutation and discards queued work when only control retires' do
    Dir.mktmpdir('skvoz-retirement-') do |directory|
      Async do |task|
        entered = false
        applied = []
        enrollment = described_class.new({}, nil) do |login, device|
          entered = true
          task.sleep(0.05)
          File.write(File.join(directory, 'committed.json'), JSON.generate(login:, device:))
          applied << device
          { v: 1, peer_id: 2 }
        end
        replies = []
        enrollment.define_singleton_method(:respond) { |*reply| replies << reply }
        enrollment.instance_variable_set(:@ready, true)
        enrollment.instance_variable_get(:@queue).concat([
          ['shared', 'reply', JSON.generate(v: 1, device: 'a' * 32)],
          ['shared', 'reply', JSON.generate(v: 1, device: 'b' * 32)]
        ])
        worker = task.async { enrollment.send(:worker) }
        enrollment.instance_variable_set(:@worker, worker)
        enrollment.instance_variable_get(:@tasks) << worker
        task.sleep(0.001) until entered
        enrollment.stop(graceful: true)
        expect(JSON.parse(File.read(File.join(directory, 'committed.json')))).to include('device' => 'a' * 32)
        expect(applied).to eq(['a' * 32])
        expect(replies).to be_empty
        expect(enrollment.ready?).to be(false)
        expect(worker.alive?).not_to be_truthy
      end.wait
    end
  end
end
