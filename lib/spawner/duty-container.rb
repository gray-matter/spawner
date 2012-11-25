require 'duty'

module Spawner
  # The sole purpose of this class is to (thread-safely) wrap a duty or none, to
  # handle workers persistency. Otherwise, we would not be able to hotswap a
  # DRb-exposed duty with another.
  class DutyContainer
    attr_reader :duty

    def initialize()
      @duty = nil
      @duty_mutex = Mutex.new()
    end

    # Get the duty and discard it
    def get_duty()
      duty = nil

      @duty_mutex.synchronize() do
        duty = @duty
        @duty = nil
      end

      return duty
    end

    def duty=(duty)
      @duty_mutex.synchronize() do
        @duty = duty
      end
    end
  end
end
