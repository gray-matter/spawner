module Spawner
  # The sole purpose of this class is to (thread-safely) wrap a duty or none, to
  # handle workers persistency. Otherwise, we would not be able to hotswap a
  # DRb-exposed duty with another.
  class DutyContainer
    # Construct a DutyContainer object.
    def initialize()
      @duty = nil
      @duty_mutex = Mutex.new()
    end

    # Get the duty and discard it.
    def get_duty()
      next_duty = nil

      @duty_mutex.synchronize() do
        next_duty = @duty
        @duty = nil
      end

      return next_duty
    end

    # Tells whether this container holds a job or not.
    def has_duty?()
      @duty_mutex.synchronize() do
        return !@duty.nil?()
      end
    end

    # Thread-safely set +the_duty+ to be performed.
    def duty=(the_duty)
      @duty_mutex.synchronize() do
        @duty = the_duty
      end
    end
  end
end
