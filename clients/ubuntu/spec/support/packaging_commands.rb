#!/usr/bin/env ruby
# frozen_string_literal: true

# The live manager is modeled; unit-file queries and disable use real systemctl.
require 'json'
state_path = ENV.fetch('SKVOZ_PACKAGE_STATE')
state = JSON.parse(File.read(state_path))
program = File.basename($PROGRAM_NAME)
calls_path = ENV.fetch('SKVOZ_PACKAGE_CALLS')
File.open(calls_path, 'a') { |file| file.puts(JSON.generate([program, *ARGV])) }
if program == 'skvoz-network-helper'
  checks = File.readlines(calls_path).count { |line| JSON.parse(line).first == program }
  abort('Retained network state is not removable') if checks == state['fail_check']
  exit
end

case ARGV.first
when 'list-unit-files', 'disable'
  exec('/usr/bin/systemctl', "--root=#{ENV.fetch('SKVOZ_PACKAGE_ROOT')}", *ARGV)
when 'list-units'
  if ARGV.last.end_with?('.service')
    puts 'skvoz-network-helper@1000.service loaded active running' if state['active_helper']
  else
    state.fetch('sockets').each do |unit, active|
      next if ARGV.include?('--state=active') && !active
      puts "#{unit} loaded #{active ? 'active listening' : 'inactive dead'}"
    end
  end
when 'stop', 'start'
  unit = ARGV.fetch(1)
  abort('Expected a socket instance') unless state.fetch('sockets').key?(unit)
  state['sockets'][unit] = ARGV.first == 'start'
  File.write(state_path, JSON.generate(state))
else
  abort("Unexpected package command: #{ARGV.inspect}")
end
