module Spawner
  # The AdeptRunner and its subclasses each implement a way to start, stop or
  # poll the status of an adept, provide it the duties and ways for it to
  # communicate about failures and successes.
  class AdeptRunner
    public

    # Whether the runner is busy or not, i.e. if it currently has a duty
    # available (this doesn't mean that it is currently executing it).
    def busy?()
      @mutex.synchronize() do
        return @busy
      end
    end

    # Construct an AdeptRunner object.
    def initialize()
      @busy = false
      @job_mutex = Mutex.new()
      @duty_container = DutyContainer.new()
      @duty_completion_callback = Proc.new() {}
      @duty_failure_callback = Proc.new() {}
    end

    # Stop the runner.
    def stop()
      @job_mutex.synchronize() do
        @busy = false
      end
    end

    # Give the +duty+ to this runner to be dispatched to a worker, which can be
    # persistent or not, depending on the value of +persistent_worker+.
    # If the persistency changes between two calls to this method, the runner
    # will take care of stopping the old worker and starting a new one the right
    # persistency.
    def give_duty(duty, persistent_worker)
      if !persistent_worker
        stop()
      end

      duty.register_completion_callback(method(:report_duty_completion))
      duty.register_failure_callback(method(:report_duty_failure))

      @job_mutex.synchronize() do
        raise "Unable to give a duty to a busy runner" if @busy

        @duty_container.duty = duty
      end

      if persistent_worker && alive?()
        wake_up()
      else
        start(persistent_worker)
      end
    end

    # Register the given +callback+ as the one to call when the duty is
    # completed.
    def register_completion_callback(callback)
      @duty_completion_callback = callback
    end

    # Register the given +callback+ as the one to call when the duty failed to
    # be completed.
    def register_failure_callback(callback)
      @duty_failure_callback = callback
    end

    protected
    # Throw a NotImplementedError exception. This is just a way to explicitly
    # provide the list of method which shall be implemented by subclasses.
    def not_implemented()
      raise NotImplementedError.new()
    end

    # Return true if the underlying runner is still alive, false otherwise.
    def alive?()
      not_implemented()
    end

    # Report the completion of the duty with the given +id+, which returned
    # +returned_value+ while it was expected to return +expected_value+.
    def report_duty_completion(id, returned_value, expected_value)
      @job_mutex.synchronize() do
        @busy = false
      end

      @duty_completion_callback.call(id, returned_value, expected_value)
    end


    # Report the failure of the duty with the given +id+, with the given
    # exception.
    def report_duty_failure(id, exception)
      @duty_failure_callback.call(id, exception)
    end

    # Wake the underlying runner up, when applicable, usually because it has
    # work to do.
    def wake_up()
      # Do nothing by default
    end
  end
end
