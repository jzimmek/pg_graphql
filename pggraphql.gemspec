# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'pggraphql/version'

Gem::Specification.new do |spec|
  spec.name          = "pggraphql"
  spec.version       = Pggraphql::VERSION
  spec.authors       = ["Jan Zimmek"]
  spec.email         = ["jan.zimmek@web.de"]
  spec.summary       = %q{PG GraphQL}
  spec.description   = %q{A data fetching library build on top of PostgreSQL inspired by Facebook's GraphQL}
  spec.homepage      = "https://github.com/jzimmek/pg_graphql"
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_dependency "json"
  spec.add_dependency "activesupport"

  spec.add_development_dependency "test-unit"

  spec.add_development_dependency "bundler", "~> 1.7"
  spec.add_development_dependency "rake", "~> 10.0"
end
