module Spawner
  class AdeptRunner
    public

    def busy?()
      @mutex.synchronize() do
        return @busy
      end
    end

    def initialize()
      @busy = false
      @job_mutex = Mutex.new()
      @duty_container = DutyContainer.new()
      @duty_completion_callback = Proc.new() {}
      @duty_failure_callback = Proc.new() {}
    end

    def stop()
      @job_mutex.synchronize() do
        @busy = false
      end
    end

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

    def register_completion_callback(callback)
      @duty_completion_callback = callback
    end

    def register_failure_callback(callback)
      @duty_failure_callback = callback
    end

    private
    def not_implemented()
      raise NotImplementedError.new()
    end

    def alive?()
      not_implemented()
    end

    def report_duty_completion(id, returned_value, expected_value)
      @job_mutex.synchronize() do
        @busy = false
      end

      @duty_completion_callback.call(id, returned_value, expected_value)
    end

    def report_duty_failure(id, exception)
      @duty_failure_callback.call(id, exception)
    end

    def wake_up()
      # Do nothing by default
    end
  end
end
