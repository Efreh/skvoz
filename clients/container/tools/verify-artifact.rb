# frozen_string_literal: true
require 'json'
require 'digest'
root = ARGV.fetch(0)
source = ARGV.fetch(1)
manifest = JSON.parse(File.read(File.join(root, 'native-source.json')), allow_duplicate_key: false)
raise 'Unsupported artifact manifest' unless manifest['v'] == 1
manifest.fetch('source_files').each do |name, digest|
  valid = name.match?(/\A(?:Cargo\.(?:toml|lock)|(?:core|network|vendor\/async-nats)\/[^\0]+)\z/) && !name.split('/').include?('..')
  raise 'Invalid source manifest path' unless valid
  raise 'Native source mismatch' unless Digest::SHA256.file(File.join(source, name)).hexdigest == digest
end
raise 'Runtime digest mismatch' unless Digest::SHA256.file(File.join(root, 'skvoz-network-runtime')).hexdigest == manifest.fetch('runtime_sha256')
raise 'Gemfile mismatch' unless Digest::SHA256.file('Gemfile').hexdigest == manifest.fetch('gemfile_sha256')
raise 'Gem lock mismatch' unless Digest::SHA256.file('Gemfile.lock').hexdigest == manifest.fetch('gemlock_sha256')
manifest.fetch('bundle_files').each do |name, digest|
  raise 'Invalid bundle path' unless name.start_with?('bundle/') && !name.split('/').include?('..') && !name.include?("\0")
  raise 'Bundle digest mismatch' unless Digest::SHA256.file(File.join(root, name)).hexdigest == digest
end
version = IO.popen([File.join(root, 'skvoz-network-runtime'), '--version'], &:read).strip
raise 'Runtime version mismatch' unless $?.success? && version == 'skvoz-network-runtime 0.7.0 network=6 api=1 core=5.0.0'
puts JSON.generate(artifact: 'verified', runtime_sha256: manifest.fetch('runtime_sha256'), source_files: manifest.fetch('source_files').size, bundle_files: manifest.fetch('bundle_files').size)
