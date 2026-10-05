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
  end

end
