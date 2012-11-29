module Spawner
  # FIXME: prevent adepts from taking other adepts's duty
  class Duty
    public

    attr_reader :id

    def initialize(id, instructions)
      @duty_start_callbacks = Array.new()
      @duty_completion_callbacks = Array.new()
      @id = id
      @instructions = instructions
    end

    def register_completion_callback(callback, in_front = false)
      if in_front
        @duty_completion_callbacks.unshift(callback)
      else
        @duty_completion_callbacks << callback
      end
    end

    def register_start_callback(callback, in_front = false)
      if in_front
        @duty_start_callbacks.unshift(callback)
      else
        @duty_start_callbacks << callback
      end
    end

    def get_instructions()
      @duty_start_callbacks.each() do |cb|
        cb.call(@id)
      end

      return @instructions
    end

    def report_completion(returned_value)
      @duty_completion_callbacks.each() do |cb|
        cb.call(@id, returned_value)
      end
    end

    def report_failure(exception)
      $stderr.puts "Caught an exception in the duty: #{exception}"

      # FIXME: do something better
      report_completion(-1)
    end
  end
end
