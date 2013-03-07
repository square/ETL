#!/usr/bin/env rake
require "bundler/gem_tasks"
begin
  require 'rspec/core/rake_task'

  RSpec::Core::RakeTask.new(:spec) do |t|
    t.rspec_opts = '-b'
  end

  task default: :spec
rescue LoadError
  $stderr.puts "rspec not available, spec task not provided"
end

begin
  require 'cane/rake_task'

  desc "Run cane to check quality metrics"
  Cane::RakeTask.new(:quality) do |cane|
    cane.abc_max    = 10
    cane.style_glob = "lib/**/*.rb"
    cane.no_doc     = true
  end

  task :default => :quality
rescue LoadError
  warn "cane not available, quality task not provided."
end
