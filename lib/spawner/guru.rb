require 'set'

module Spawner
  # A Guru holds the instructions and knows which adept is in charge of which
  # duty
  class Guru
    public

    # Construct a Guru.
    def initialize()
      @duties_mutex = Mutex.new()

      # Duties objects indexed by their id
      @duties = Hash.new()
      # List of unassigned duties' id
      @unassigned_duties_id = Array.new()

      @remaining_retries = Hash.new()

      @current_duty_id = 1

      @duty_end_callback = Proc.new() {}
    end

    # Add +instructions+ for a duty to to be performed by adepts. If the
    # instructions return value is not the +expected_value+, it will be
    # considered as failed. The given job will be retried at most +max_retries+
    # times.
    def add_duty(instructions, expected_value, max_retries)
      duty_id = nil

      @duties_mutex.synchronize() do
        duty_id = @current_duty_id
        @current_duty_id += 1
      end

      duty = Duty.new(duty_id, instructions, expected_value)

      @duties_mutex.synchronize() do
        @unassigned_duties_id << duty_id
        @duties[duty_id] = duty
        @remaining_retries[duty.id] = max_retries
      end
    end

    # Assign as many as duties as possible to the given +runners+ and return the
    # duty id to runner mapping. They will be configured to be +persistent+ or
    # not, and will be allowed to fail at most +max_retries+ each.
    def assign_duties(runners, persistent)
      nb_assigned_duties = 0
      duty_id_to_runner_mapping = Hash.new()

      @duties_mutex.synchronize() do
        while nb_assigned_duties < runners.size() && !@unassigned_duties_id.empty?()
          duty_id = @unassigned_duties_id.shift()
          duty = @duties[duty_id]
          runner = runners[nb_assigned_duties]

          runner.register_completion_callback(method(:report_duty_completion))
          runner.register_failure_callback(method(:report_duty_failure))

          runner.give_duty(duty, persistent)
          duty_id_to_runner_mapping[duty.id] = runner
          nb_assigned_duties += 1
        end
      end

      return duty_id_to_runner_mapping
    end

    # Get the completion time of the duty referenced by +duty_id+.
    def duty_completion_time(duty_id)
      @duties_mutex.synchronize() do
        duty = @duties[duty_id]

        return nil if duty.start_time.nil?() || duty.end_time.nil?()

        return duty.end_time.to_i() - duty.start_time.to_i()
      end
    end

    # Get the count of unassigned duties, i.e. duties run by no adept yet.
    def unassigned_duties_count()
      @duties_mutex.synchronize() do
        return @duties.size() - @unassigned_duties_id.size()
      end
    end

    # Get the count of assigned duties, i.e. duties currently run by an adept.
    def assigned_duties_count()
      @duties_mutex.synchronize() do
        return @unassigned_duties_id.size()
      end
    end

    # Get the count of duties (both assigned and unassigned) left.
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

    # Register the given +callback+ as the one called whenever a duty ends.
    # This callback takes a single argument, which is the id of the finished
    # duty.
    def register_duty_end_callback(callback)
      @duty_end_callback = callback
    end

    private

    # Handle the completion of the duty referenced by +duty_id+, with the given
    # +returned_value+, while +expected_value+ was expected.
    def report_duty_completion(duty_id, returned_value, expected_value)
      @duties_mutex.synchronize() do
        if !expected_value.nil?() && returned_value != expected_value
          Spawner.jobs_logger.info("The duty #{duty_id} returned " +
                                   "#{returned_value.inspect()} while it was expected " +
                                   "to return #{expected_value.inspect()}\n")

          handle_duty_failure(duty_id)
        else
          @duties.delete(duty_id)
          @remaining_retries.delete(duty_id)
        end
      end

      @duty_end_callback.call(duty_id)
    end

    # Handle the failure of the duty referenced by +duty_id+, +exc+ being the
    # exception thrown by the adept when it ran the duty.
    def report_duty_failure(duty_id, exc)
      Spawner.jobs_logger.error(" The job #{duty_id} failed with the following exception: #{exc} (#{exc.backtrace().join("\n")})\n")

      @duties_mutex.synchronize() do
        handle_duty_failure(duty_id)
      end

      @duty_end_callback.call(duty_id)
    end

    # /!\ This method MUST BE called within a mutex lock block, since it is not
    # thread-safe.
    # Handle what's going on after the duty referenced by +duty_id+ has failed
    # (retry it or not, mainly).
    def handle_duty_failure(duty_id)
      if @remaining_retries[duty_id] > 0
        Spawner.jobs_logger.info("Retrying the duty #{duty_id} (#{@remaining_retries[duty_id]} retries remaining)")
        @remaining_retries[duty_id] -= 1
        @unassigned_duties_id << duty_id
      else
        @duties.delete(duty_id)
        @remaining_retries.delete(duty_id)
      end
    end
  end
end
