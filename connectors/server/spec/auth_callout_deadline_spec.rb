# frozen_string_literal: true
require_relative 'spec_helper'
require_relative '../lib/skvoz/server/auth_callout'

RSpec.describe Skvoz::Server::AuthCallout do
  it 'rejects late password completions even while the native request remains valid' do
    # Exercise BCrypt overrun and delayed result drain without sleeping.
    [[102.1, 102.1, false], [101.95, 102.1, false], [101.95, 101.95, true]].each do |finished, drained, granted|
      clock = 100.0
      allow(Process).to receive(:clock_gettime).with(Process::CLOCK_MONOTONIC) { clock }
      allow(Time).to receive(:now).and_return(Time.at(1_000))
      allow(Skvoz::Server::State).to receive(:login?).and_return(true)
      allow(Skvoz::Server::State).to receive(:password?).and_return(true)
      request = { 'exp' => 1_003, 'nats' => { 'connect_opts' => { 'user' => 'alice', 'pass' => 'fixture-password' } } }
      allow(Skvoz::Server::NatsAuth).to receive(:request).and_return(request)
      state = double('state', value: { 'users' => { 'alice' => { 'hash' => 'fixture-hash', 'access_revision' => 1 } } },
                     access_ready?: true, identity: 'fixture-alice', permissions: { 'publish' => ['fixture'] })
      revocation = double('revocation', reserve: true)
      responder = described_class.allocate
      { state: state, revocation: revocation, issuer: double('issuer', public_key: 'fixture-issuer'),
        curve: Object.new, jobs: SizedQueue.new(16), results: SizedQueue.new(16), paused: false }.each do |name, value|
        responder.instance_variable_set("@#{name}", value)
      end
      replies = []
      allow(responder).to receive(:respond) { |_reply, _request, **arguments| replies << arguments }
      password = double('password')
      allow(BCrypt::Password).to receive(:new).with('fixture-hash').and_return(password)
      allow(password).to receive(:==).with('fixture-password') { clock = finished; true }

      responder.send(:receive, reply: '_INBOX.fixture', data: 'fixture', headers: { 'Nats-Server-Xkey' => 'fixture' })
      clock = 101.9
      responder.instance_variable_get(:@jobs).push(nil)
      responder.send(:password_work)
      completion = responder.instance_variable_get(:@results).pop(true)
      expect(completion.fetch(:deadline)).to eq(102.0)
      expect(completion.fetch(:result)).to be(true)
      clock = drained
      completion.fetch(:complete).call(completion)

      if granted
        expect(replies).to eq([{ identity: 'fixture-alice', permissions: { 'publish' => ['fixture'] } }])
        expect(revocation).to have_received(:reserve).once
      else
        expect(replies).to eq([{ error: 'Authentication rejected' }])
        expect(revocation).not_to have_received(:reserve)
      end
    end
  end
end
