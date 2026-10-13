#!/usr/bin/env ruby
# frozen_string_literal: true
require 'bundler/setup'
require_relative '../../lib/skvoz/server/service'

abort 'Expected serve --config FILE' unless ARGV.size == 3 && ARGV[0, 2] == ['serve', '--config']
config = Skvoz::Server::Configuration.new(JSON.parse(Skvoz::Server::PrivateFiles.read(ARGV.last), allow_duplicate_key: false))
server = Skvoz::Server::Service.new(config)
%w[INT TERM].each { |signal| Signal.trap(signal) { server.stop } }
Async { |task| server.run(task) }.wait
