require 'sourcify'

module Spawner
  # The duty object holds the job instructions as well as some extra metadata
  # about it.
  class Duty
    public

    # The duty id.
    attr_reader :id
    # The time at which this duty was started for the last time (nil if it was
    # never started).
    attr_reader :start_time
    # The time at which this duty ended for the last time (nil if it's not over
    # yet or if it failed).
    attr_reader :end_time

    # Construct a Duty object, which will be represented by +id+, hold the given
    # +instructions+ which will executed later and are expected to return the
    # +expected_value+.
    def initialize(id, instructions, expected_value)
      @id = id
      @instructions = instructions
      @expected_value = expected_value
      @duty_completion_callback = Proc.new() {}
      @duty_failure_callback = Proc.new() {}
      @start_time = nil
      @end_time = nil
    end

    # Return a pair composed of the instructions block together with a Binding
    # object representing the bindings of the instructions in the context in
    # which they were created.
    def get_instructions_and_binding()
      @start_time = Time.now()
      return @instructions.to_source(), @instructions.binding
    end

    # Register the given +callback+ as the one called when the duty is
    # completed without any error.
    # This callback shall expect three arguments: the duty id, the
    # returned_value and the expected value.
    def register_completion_callback(callback)
      @duty_completion_callback = callback
    end

    # Register the given +callback+ as the one called when the duty fails.
    # This callback shall expect two arguments: the duty id and the exception
    # object representing the error.
    def register_failure_callback(callback)
      @duty_failure_callback = callback
    end

    # Report the completion of this duty, with a given +returned_value+.
    def report_completion(returned_value)
      @end_time = Time.now()

      Thread.new() do
        @duty_completion_callback.call(@id, returned_value, @expected_value)
      end
    end

    # Report the failure of this duty, because of the given +expection+.
    def report_failure(exception)
      @end_time = nil

      Thread.new() do
        @duty_failure_callback.call(@id, exception)
      end
    end
  end
end
