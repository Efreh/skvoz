#!/usr/bin/env ruby
# frozen_string_literal: true
require 'json'

File.open(ENV.fetch('UPDATE_TEST_CALLS'), 'a') { |file| file.puts(JSON.generate(ARGV)) }
warn 'Ordinary Docker progress'
if ARGV.include?('ps')
  stopped = ENV['UPDATE_TEST_STOPPED'] == '1' ||
            (ENV['UPDATE_TEST_STOP_AFTER_PULL'] == '1' && File.exist?(ENV.fetch('UPDATE_TEST_PULLED')))
  puts 'running-server' unless stopped
elsif ARGV.include?('pull')
  if ENV['UPDATE_TEST_PULL_FAIL'] == '1'
    warn 'Registry unavailable'
    exit 8
  end
  File.write(ENV.fetch('UPDATE_TEST_PULLED'), '')
end
