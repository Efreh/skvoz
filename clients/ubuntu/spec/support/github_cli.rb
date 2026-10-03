#!/usr/bin/env ruby
# frozen_string_literal: true

# Disposable release API substitute; the workflow still runs real Bash, jq and dpkg-deb.
require 'json'
state_path = ENV.fetch('SKVOZ_RELEASE_STATE')
state = JSON.parse(File.read(state_path))
File.open(ENV.fetch('SKVOZ_RELEASE_CALLS'), 'a') { |file| file.puts(JSON.generate(ARGV)) }
operation = ARGV.first == 'api' ? 'api' : ARGV[1]
abort("Injected #{operation} failure") if ENV['SKVOZ_RELEASE_FAIL'] == operation
tag = ARGV[2]
release = state.find { |entry| entry.fetch('tag_name') == tag }

case ARGV[0..1]
when ['release', 'view']
  abort('Release not found') unless release
  puts release.fetch('draft')
when ['release', 'create']
  abort('Release already exists') if release
  abort('Expected a draft release') unless ARGV.include?('--draft')
  state << { 'id' => 10_000, 'tag_name' => tag, 'draft' => true, 'published_at' => nil, 'assets' => [] }
when ['release', 'upload']
  abort('Only draft assets can be changed') unless release&.fetch('draft')
  assets = ARGV.drop(3).reject { |argument| argument.start_with?('--') }
  abort('Missing assets') unless assets.size == 2 && assets.all? { |path| File.file?(path) }
  release['assets'] = assets.map { |path| File.basename(path) }
when ['release', 'edit']
  abort('Incomplete release') unless release&.fetch('assets', [])&.size == 2
  abort('Expected publication') unless ARGV.include?('--draft=false')
  release['draft'] = false
  release['published_at'] = '2026-10-03T12:00:00Z'
when ['release', 'delete']
  abort('Unsafe deletion') unless release && !release.fetch('draft') && ARGV.include?('--yes') && !ARGV.include?('--cleanup-tag')
  state.delete(release)
else
  abort("Unexpected command: #{ARGV.inspect}") unless ARGV.first == 'api'
  abort('Expected all release pages') unless ARGV.include?('--paginate') && ARGV.include?('--slurp')
  puts JSON.generate(state.each_slice(100).to_a)
end
File.write(state_path, JSON.generate(state))
