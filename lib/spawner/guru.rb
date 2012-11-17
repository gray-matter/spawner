require 'duty'

module Spawner
  # A Guru holds the instructions and knows which adept is in charge of which duty
  class Guru
    public

    def initialize(duty_completion_callback)
      @duties_mutex = Mutex.new()
      @duties = Hash.new()
      @duties_start_time = Hash.new()
      @duties_end_time = Hash.new()
      @duty_completion_callback = duty_completion_callback

      @current_duty_id = 1
    end

    # Return an unique duty id whose status may be polled later using the
    # duty_completed? method
    def add_duty(instructions, expected_value)
      duty_id = nil

      @duties_mutex.synchronize() do
        duty_id = @current_duty_id
        @current_duty_id += 1
      end

      duty = Duty.new(duty_id, instructions, method(:report_duty_start), method(:report_duty_completion))

      @duties_mutex.synchronize() do
        @duties[duty_id] = duty
      end

      return duty
    end

    def assigned_duty?(duty_id)
      @duties_mutex.synchronize() do
        return @duties_start_time.has_key?(duty_id)
      end
    end

    def completed_duty?(duty_id)
      @duties_mutex.synchronize() do
        return @duties_end_time.has_key?(duty_id)
      end
    end

    def duty_completion_time(duty_id)
      raise "Duty not started yet" if !assigned_duty(duty_id)
      raise "Duty not completed yet" if !completed_duty(duty_id)

      @duties_mutex.synchronize() do
        return @duties_end_time[duty_id] - @duties_start_time[duty_id]
      end
    end

    private

    def report_duty_start(duty_id)
      @duties_start_time[duty_id] = Time.now()
    end

    def report_duty_completion(duty_id, returned_value)
      @duties_end_time[duty_id] = Time.now()
      @duty_completion_callback.call(duty_id, returned_value)
    end

    # FIXME: add a method to inform of adept's death and give its job to another
  end
end
