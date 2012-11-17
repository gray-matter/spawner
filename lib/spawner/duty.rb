module Spawner
  # FIXME: prevent adepts from taking other adepts's duty
  class Duty
    public

    attr_reader :id

    def initialize(id, instructions, duty_start_callback, duty_completion_callback)
      @duty_start_callback = duty_start_callback
      @duty_completion_callback = duty_completion_callback
      @id = id
      @instructions = instructions
    end

    def get_instructions()
      @duty_start_callback.call(@id)

      return @instructions
    end

    def report_completion(returned_value)
      @duty_completion_callback.call(@id, returned_value)
    end
  end
end
