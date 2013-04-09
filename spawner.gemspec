$:.push File.expand_path("../lib", __FILE__)

require "spawner/version"

Gem::Specification.new do |s|
  s.name        = "spawner"
  s.version     = Spawner::VERSION
  s.platform    = Gem::Platform::RUBY
  s.authors     = ["Xavier NOELLE"]
  s.email       = ["xavier[dot]noelle[at]gmail[dot]com"]
  s.homepage    = "https://github.com/gray-matter/spawner"
  s.summary     = %q{Simple loop job runner service.}
  s.description = %q{The spawner lets you parallelize anything, using either threads or processes.}
  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- test/*-functional-*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]

  s.add_dependency "sourcify", ">= 0.6.0.rc3"
end
