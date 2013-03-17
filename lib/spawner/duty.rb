require 'sourcify'

module Spawner
  # FIXME: prevent adepts from taking other adepts's duty
  class Duty
    public

    attr_reader :id

    def initialize(id, instructions)
      @id = id
      @instructions = instructions
      @duty_start_callback = Proc.new() {}
      @duty_completion_callback = Proc.new() {}
      @duty_failure_callback = Proc.new() {}
    end

    def get_instructions()
      # FIXME: this shouldn't exist and the duty should know about its own start
      # and end time (remove this callback from Guru)
      @duty_start_callback.call(@id)

      return @instructions.to_source()
    end

    def register_completion_callback(callback)
      @duty_completion_callback = callback
    end

    def register_start_callback(callback)
      @duty_start_callback = callback
    end

    def register_failure_callback(callback)
      @duty_failure_callback = callback
    end

    def report_completion(returned_value)
      Thread.new() do
        @duty_completion_callback.call(@id, returned_value)
      end
    end

    def report_failure(exception)
      @duty_failure_callback.call(@id, exception)
    end

    def report_start()
      @duty_start_callback.call(@id)
    end
  end
end
