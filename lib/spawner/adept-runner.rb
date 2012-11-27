module Spawner
  class AdeptRunner
    public
    attr_reader :busy

    alias busy? busy

    def initialize()
      @busy = false
      @duty_container = DutyContainer.new()
      @persistent_worker = nil
      @adept = Adept.new()
    end

    def stop()
      not_implemented()
    end

    def try_stop()
      if !busy?()
        stop()
        return true
      end

      return false
    end

    def give_duty(duty, persistent_worker)
      # If the worker was persistent before and should not be anymore, then kill
      # it and spawn a new one
      if !persistent_worker && (!@persistent_worker.nil?() && @persistent_worker)
        stop()
      end

      if busy?()
        raise "Unable to give a duty to a busy runner"
      end

      duty.register_completion_callback(method(:report_duty_completion), true)
      duty.register_start_callback(method(:report_duty_start), true)

      @persistent_worker = persistent_worker
      @duty_container.duty = duty

      if !alive?()
        start(persistent_worker)
      end

      if persistent_worker
        # FIXME: handle dead process
        wake_up()
      end
    end

    private
    def not_implemented()
      raise NotImplementedError.new()
    end

    def alive?()
      not_implemented()
    end

    def report_duty_start()
      not_implemented()
    end

    def report_duty_completion()
      not_implemented()
    end

    def report_duty_start(id)
      @busy = true
    end

    def report_duty_completion(id, return_value)
      @busy = false
    end

    def wake_up()
      # Do nothing by default
    end
  end
end
