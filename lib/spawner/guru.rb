require 'duty'
require 'set'

module Spawner
  # A Guru holds the instructions and knows which adept is in charge of which
  # duty
  # FIXME: this is no longer true, cf. conductor
  class Guru
    public

    def initialize()
      @duties_mutex = Mutex.new()

      # Duties objects indexed by their id
      @duties = Hash.new()
      # List of unassigned duties' id
      @unassigned_duties_id = Array.new()

      @duties_start_time = Hash.new()
      @duties_end_time = Hash.new()

      @duties_expected_value = Hash.new()

      @current_duty_id = 1

      @duty_end_callback = Proc.new() {}
    end

    # Add instructions for a duty to to be performed by adepts.
    def add_duty(instructions, expected_value)
      duty_id = nil

      @duties_mutex.synchronize() do
        duty_id = @current_duty_id
        @current_duty_id += 1
      end

      duty = Duty.new(duty_id, instructions)

      @duties_mutex.synchronize() do
        @unassigned_duties_id << duty_id
        @duties[duty_id] = duty
        @duties_expected_value[duty_id] = expected_value
      end
    end

    # Assign as many as duties as possible to the given +runners+ and return the
    # duty id to runner mapping.
    def assign_duties(runners, persistent)
      nb_assigned_duties = 0
      duty_id_to_runner_mapping = Hash.new()

      @duties_mutex.synchronize() do
        while nb_assigned_duties < runners.size() && !@unassigned_duties_id.empty?()
          duty_id = @unassigned_duties_id.shift()
          duty = @duties[duty_id]
          runner = runners[nb_assigned_duties]

          runner.register_start_callback(method(:report_duty_start))
          runner.register_completion_callback(method(:report_duty_completion))
          runner.register_failure_callback(method(:report_duty_failure))

          runner.give_duty(duty, persistent)
          duty_id_to_runner_mapping[duty.id] = runner
          nb_assigned_duties += 1
        end
      end

      return duty_id_to_runner_mapping
    end

    def duty_completion_time(duty_id)
      @duties_mutex.synchronize() do
        raise "Duty not started yet" if !@duties_start_time.has_key?(duty_id)
        raise "Duty not completed yet" if !@duties_end_time.has_key?(duty_id)

        return @duties_end_time[duty_id] - @duties_start_time[duty_id]
      end
    end

    def unassigned_duties_count()
      @duties_mutex.synchronize() do
        return @duties.size() - @unassigned_duties_id.size()
      end
    end

    def assigned_duties_count()
      @duties_mutex.synchronize() do
        return @unassigned_duties_id.size()
      end
    end

    def duties_left()
      @duties_mutex.synchronize() do
        return @duties.size()
      end
    end

    # Return an array with the number of assigned duties and the number of
    # unassigned duties as elements.
    def duties_count_breakdown()
      @duties_mutex.synchronize() do
        return @duties.size() - @unassigned_duties_id.size(), @unassigned_duties_id.size()
      end
    end

    def register_duty_end_callback(callback)
      @duty_end_callback = callback
    end

    private

    def report_duty_start(duty_id)
      @duties_mutex.synchronize() do
        @duties_start_time[duty_id] = Time.now()
      end
    end

    def report_duty_completion(duty_id, returned_value)
      @duties_mutex.synchronize() do
        @duties_end_time[duty_id] = Time.now()
        @duties.delete(duty_id)

        if returned_value != @duties_expected_value[duty_id]
          Spawner.jobs_logger.info("The duty #{duty_id} returned " +
                                   "#{returned_value.inspect()} while it was expected " +
                                   "to return #{@duties_expected_value[duty_id].inspect()}")

          # FIXME: do something; try again or discard
        end

        @duties_expected_value.delete(duty_id)
      end

#      Thread.new() do
        @duty_end_callback.call(duty_id)
#      end
    end

    def report_duty_failure(duty_id, exception)
      @duties_mutex.synchronize() do
        @duties_start_time.delete(duty_id)
        @duties_end_time.delete(duty_id)
        @unassigned_duties_id << duty_id
        @duties_expected_value.delete(duty_id)
      end

#      Thread.new() do
        @duty_end_callback.call(duty_id)
#      end
    end
  end
end
