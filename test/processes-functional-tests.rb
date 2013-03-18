$LOAD_PATH.unshift("#{File.dirname(__FILE__)}")

require 'test/unit'
require 'generic-functional-tests'

class ProcessesFunctionalTests < Test::Unit::TestCase
  public
  def self.parallelism_model()
    return 'process'
  end

  include GenericFunctionalTestsMixin
end
