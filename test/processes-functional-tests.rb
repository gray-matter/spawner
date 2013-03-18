require 'test/unit'
require 'generic-functional-tests'

class ProcessesFunctionalTests < Test::Unit::TestCase
  public
  def self.parallelism_model()
    return 'process'
  end

  include GenericFunctionalTestsMixin
end
