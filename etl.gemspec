# -*- encoding: utf-8 -*-
require File.expand_path('../lib/etl/version', __FILE__)

Gem::Specification.new do |gem|
  gem.authors       = ["Jeff Iacono"]
  gem.email         = ["iacono@squareup.com"]
  gem.description   = %q{Extract, Transform, and Load (ETL) ruby wrapper}
  gem.summary       = %q{Extract, Transform, and Load (ETL) ruby wrapper. Supports basic and iterative ETL operations.}
  gem.homepage      = "https://github.com/square/ETL"

  gem.files         = `git ls-files`.split($\)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.name          = "ETL"
  gem.require_paths = ["lib"]
  gem.version       = ETL::VERSION

  gem.add_runtime_dependency "activesupport", [">= 3.2.3"]

  gem.add_development_dependency "rake"
  gem.add_development_dependency "cane"
  gem.add_development_dependency "mysql2"
  gem.add_development_dependency "rspec", [">= 2"]
end
