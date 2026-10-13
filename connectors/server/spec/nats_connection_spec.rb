# frozen_string_literal: true
require_relative 'spec_helper'
require_relative '../lib/skvoz/server/nats_connection'

RSpec.describe Skvoz::Server::NatsConnection do
  it 'rejects an invalid INFO identity and closes the unadmitted socket without masking the cause' do
    server_key = Skvoz::Server::NatsAuth::Key.new(:server).public_key
    invalid = [nil, Skvoz::Server::NatsAuth::Key.new(:user).public_key,
               server_key[0...-1] + (server_key[-1] == 'A' ? 'B' : 'A')]
    invalid.each do |identity|
      input, output = UNIXSocket.pair
      connection = described_class.new({ 'port' => 4222, 'address' => 'localhost' },
                                     Struct.new(:value).new({ 'tls' => { 'ca' => nil } }), 'sys')
      allow(TCPSocket).to receive(:new).with('127.0.0.1', 4222).and_return(input)
      allow(Skvoz::Server::BrokerTLS).to receive(:connect).and_wrap_original do |_method, _raw, **_options, &block|
        block.call('server_id' => identity)
      end
      Async do |task|
        expect { connection.start(task) }.to raise_error(Skvoz::Server::Error, 'Native service server identity invalid')
        expect(connection.ready?).to be(false)
        expect(input).to be_closed
        expect(output.read).to eq('')
      end.wait
    ensure
      connection&.stop
      input&.close unless input&.closed?
      output&.close unless output&.closed?
    end
  end

  it 'collects a late remote reply within the complete native discovery window' do
    input, output = UNIXSocket.pair
    connection = described_class.new(nil, nil, 'sys')
    connection.instance_variable_set(:@socket, input)
    connection.instance_variable_set(:@ready, true)
    Async do |task|
      reader = task.async { connection.send(:reader) }
      peer = task.async do
        subject = sid = nil
        loop do
          line = output.gets("\r\n")
          break unless line
          fields = line.split
          case fields.first
          when 'SUB' then subject, sid = fields.values_at(1, 2)
          when 'PING' then output.write("PONG\r\n")
          when 'PUB'
            output.read(Integer(fields.last) + 2)
            body = JSON.generate('name' => 'one')
            output.write("MSG #{subject} #{sid} #{body.bytesize}\r\n#{body}\r\n")
            task.sleep(0.22)
            body = JSON.generate('name' => 'two')
            output.write("MSG #{subject} #{sid} #{body.bytesize}\r\n#{body}\r\n")
          end
        end
      end
      result = task.with_timeout(1) { connection.query('$SYS.REQ.SERVER.PING.IDZ', {}, timeout: 0.3, collect: true) }
      expect(result).to eq([{ 'name' => 'one' }, { 'name' => 'two' }])
    ensure
      connection.stop
      peer&.stop; reader&.stop
    end.wait
  ensure
    input&.close unless input&.closed?
    output&.close unless output&.closed?
  end

  it 'bounds truncated body and terminator reads without dispatching an incomplete message' do
    ["MSG fixture 1 4\r\nab", "MSG fixture 1 4\r\nabcd\r", "MSG fixture 1 4\r\nabcdxx"].each do |frame|
      input, output = UNIXSocket.pair
      connection = described_class.new(nil, nil, 'auth')
      connection.instance_variable_set(:@socket, input)
      connection.instance_variable_set(:@ready, true)
      dispatched = false
      connection.instance_variable_set(:@handlers, { '1' => ->(_message) { dispatched = true } })
      Async do |task|
        output.write(frame)
        started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        task.with_timeout(3) { task.async { connection.send(:reader) }.wait }
        expect(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).to be <= 2.5
        expect(connection.ready?).to be(false)
        expect(connection.failure).to eq('native_service_unavailable')
        expect(dispatched).to be(false)
      end.wait
    ensure
      connection&.stop
      input&.close unless input&.closed?
      output&.close unless output&.closed?
    end
  end
end
