# frozen_string_literal: true
require 'bundler/setup'
require 'rspec'
require 'tmpdir'
require 'async'
require_relative '../lib/skvoz/server/state'
require_relative '../lib/skvoz/server/runtime_control'
require_relative '../lib/skvoz/server/enrollment'

RSpec.configure do |config|
  config.order = :random
  config.disable_monkey_patching!
  config.expect_with(:rspec) { |expectations| expectations.syntax = :expect }
  config.filter_run_excluding integration: true unless ENV['SKVOZ_INTEGRATION'] == '1'
  config.filter_run_excluding capacity: true unless ENV['SKVOZ_CAPACITY_DIRECTORY']
  config.filter_run_excluding compose: true unless ENV['SKVOZ_COMPOSE'] == '1'
  config.fail_if_no_examples = true
end
