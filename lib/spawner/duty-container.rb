require 'duty'

module Spawner
  # The sole purpose of this class is to (thread-safely) wrap a duty or none, to
  # handle workers persistency. Otherwise, we would not be able to hotswap a
  # DRb-exposed duty with another.
  class DutyContainer
    def initialize()
      @duty = nil
      @duty_mutex = Mutex.new()
    end

    def duty=(duty)
      @duty_mutex.synchronize() do
        @duty = duty
      end
    end

    def get_duty()
      duty = @duty
      @duty = nil
      return duty
    end
  end
end
