# frozen_string_literal: true
require_relative 'spec_helper'
require 'stringio'

RSpec.describe Skvoz::Server::Diagnostics do
  it 'drops a log event when the stderr pipe reader has disappeared' do
    reader, writer = IO.pipe
    reader.close
    original = $stderr
    begin
      $stderr = writer
      expect(described_class.emit('runtime_ready', revision: 1)).to be(false)
    ensure
      $stderr = original
      writer.close
    end
  end

  it 'still closes the socket and releases stream reservations when stderr is closed' do
    connector = Skvoz::Server::TCPConnector.new(path: '/unused', policy: nil)
    limits = { stream_frames: 128, stream_bytes: 2_097_152 }
    stream = Skvoz::Server::TCPStream.new(connector, 1, 2, ''.b, limits)
    reader, writer = UNIXSocket.pair
    stream.instance_variable_set(:@socket, reader)
    stream.instance_variable_set(:@opened, true)
    connector.streams[1] = stream
    frame = Skvoz::Server::Protocol::Frame.new(Skvoz::Server::Protocol::DATA, 0, 1, [0].pack('Q>') + 'data')
    stream.event(frame)
    expect(connector.statistics).to include('event_frames' => 1)
    output = StringIO.new
    output.close
    original = $stderr
    begin
      $stderr = output
      expect { stream.abort }.not_to raise_error
      expect(reader).to be_closed
      expect(connector.statistics).to include('streams' => 0, 'event_frames' => 0, 'event_bytes' => 0)
      expect(connector.statistics['outcomes']).to include('cancelled' => 1)
    ensure
      $stderr = original
      reader.close unless reader.closed?
      writer.close
      stream.instance_variable_get(:@queue).close
    end
  end

  it 'logs finite error classifications without messages, payloads or full paths' do
    marker = 'private-secret-payload-marker'
    error = begin
      raise Errno::ECONNRESET, marker
    rescue Errno::ECONNRESET => failure
      failure
    end
    output = StringIO.new
    original = $stderr
    begin
      $stderr = output
      described_class.emit('stream_failed', **described_class.error_fields(error))
    ensure
      $stderr = original
    end
    value = JSON.parse(output.string)
    expect(value).to include('event' => 'stream_failed', 'error_code' => 'econnreset', 'errno' => Errno::ECONNRESET::Errno)
    expect(value['time']).to end_with('Z')
    expect(output.string).not_to include(marker, __dir__)
    expect(described_class.error_fields(IOError.new('closed stream'))).to include(error_code: 'io_closed')
    expect(described_class.error_fields(IOError.new(marker))).to include(error_code: 'io_error')
    expect(described_class.error_fields(Skvoz::Server::DestinationError.new('refused', errno: 101))).to include(errno: 101)
  end

  it 'caps stream log bursts while retaining outcome counts and the latest failure' do
    connector = Skvoz::Server::TCPConnector.new(path: '/unused', policy: nil)
    allow(Process).to receive(:clock_gettime).and_return(123.0)
    output = StringIO.new
    original = $stderr
    begin
      $stderr = output
      100.times { |index| connector.outcome('failed', handle: index.to_s, error_code: 'queue_overflow') }
    ensure
      $stderr = original
    end
    expect(output.string.lines.length).to eq(20)
    expect(connector.statistics).to include('suppressed_events' => 80)
    expect(connector.statistics['outcomes']).to include('failed' => 100)
    expect(connector.statistics['last_failure']).to include(handle: '99')
  end

  it 'does not report a blocked socket reader awakened by local cancellation as a failure' do
    connector = Skvoz::Server::TCPConnector.new(path: '/unused', policy: nil)
    session = instance_double(Skvoz::Server::IPCSession, request: nil, check: nil)
    connector.instance_variable_set(:@session, session)
    limits = { stream_frames: 128, stream_bytes: 2_097_152 }
    metadata = JSON.generate(v: 1, type: 'tcp', host: '127.0.0.1', port: 12345)
    stream = Skvoz::Server::TCPStream.new(connector, 1, 2, metadata, limits)
    reader, writer = UNIXSocket.pair
    allow(connector).to receive(:connect).and_return(reader)
    output = StringIO.new
    original = $stderr
    begin
      $stderr = output
      Async do |task|
        stream.start(task)
        actor = stream.instance_variable_get(:@tasks).first
        task.sleep(0.01)
        stream.abort
        actor.wait
      end.wait
    ensure
      $stderr = original
      reader.close unless reader.closed?
      writer.close
    end
    expect(output.string).not_to include('SKVOZ stream failed:', '"event":"stream_failed"')
    expect(connector.statistics['outcomes']).to include('cancelled' => 1, 'failed' => 0)
  end

  it 'distinguishes cancellation, rejection and transport/protocol/deadline failures from CLOSED reasons' do
    protocol = Skvoz::Server::Protocol
    limits = { stream_frames: 128, stream_bytes: 2_097_152 }
    output = StringIO.new
    original = $stderr
    begin
      $stderr = output
      { 2 => 'rejected', 3 => 'cancelled', 4 => 'failed', 5 => 'failed', 6 => 'failed' }.each do |reason, outcome|
        connector = Skvoz::Server::TCPConnector.new(path: '/unused', policy: nil)
        stream = Skvoz::Server::TCPStream.new(connector, 1, 2, ''.b, limits)
        frame = protocol::Frame.new(protocol::CLOSED, 0, 1, [reason].pack('n'))
        stream.event(frame)
        stream.event(frame)
        expect(connector.statistics['outcomes'][outcome]).to eq(1)
        expect(connector.statistics['outcomes'].values.sum).to eq(1)
      end
    ensure
      $stderr = original
    end
    expect(output.string.lines.length).to eq(5)
  end
end
