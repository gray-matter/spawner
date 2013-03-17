require 'sourcify'

module Spawner
  # FIXME: prevent adepts from taking other adepts's duty
  class Duty
    public

    attr_reader :id, :start_time, :end_time

    def initialize(id, instructions, expected_value)
      @id = id
      @instructions = instructions
      @expected_value = expected_value
      @duty_completion_callback = Proc.new() {}
      @duty_failure_callback = Proc.new() {}
      @start_time = nil
      @end_time = nil
    end

    def get_instructions()
      @start_time = Time.now()
      return @instructions.to_source()
    end

    def register_completion_callback(callback)
      @duty_completion_callback = callback
    end

    def register_failure_callback(callback)
      @duty_failure_callback = callback
    end

    def report_completion(returned_value)
      @end_time = Time.now()

      Thread.new() do
        @duty_completion_callback.call(@id, returned_value, @expected_value)
      end
    end

    def report_failure(exception)
      @duty_failure_callback.call(@id, exception)
    end
  end
end
