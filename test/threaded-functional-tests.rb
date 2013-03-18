$LOAD_PATH.unshift("#{File.dirname(__FILE__)}")

require 'test/unit'
require 'generic-functional-tests'

class ThreadedFunctionalTests < Test::Unit::TestCase
  public
  def self.parallelism_model()
    return 'thread'
  end

  include GenericFunctionalTestsMixin
end
