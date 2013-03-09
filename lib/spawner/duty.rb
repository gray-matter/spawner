require 'sourcify'

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
      @cb_mutex = Mutex.new()
    end

    def register_completion_callback(callback, in_front = false)
      register_to_callback(@duty_completion_callbacks, callback, in_front)
    end

    def register_start_callback(callback, in_front = false)
      register_to_callback(@duty_start_callbacks, callback, in_front)
    end

    def get_instructions()
      # FIXME: do this asynchronously ?
      @cb_mutex.synchronize() do
        @duty_start_callbacks.each() do |cb|
          cb.call(@id)
        end
      end

      return @instructions.to_source()
    end

    def report_completion(returned_value)
      # FIXME: do this asynchronously ?
      @cb_mutex.synchronize() do
        @duty_completion_callbacks.each() do |cb|
          cb.call(@id, returned_value)
        end
      end
    end

    def report_failure(exception)
      Spawner.jobs_logger.error("Caught an exception in the duty: '#{exception.message}'\n#{exception.backtrace().join("\n")}")

      # FIXME: do something better
      report_completion(-1)
    end

    private
    def register_callback(cb_list, cb, in_front)
      @cb_mutex.synchronize() do
        if in_front
          cb_list.unshift(cb)
        else
          cb_list << cb
        end
      end
    end
  end
end
