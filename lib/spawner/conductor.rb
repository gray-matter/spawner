# FIXME
$LOAD_PATH.unshift("#{File.dirname(__FILE__)}")

require 'guru'
require 'adept'
require 'configuration'
require 'adept-thread-runner'
require 'adept-process-runner'

module Spawner
  class Conductor
    public
    def initialize(config_file_name)
      Thread.abort_on_exception = true

      @stopping = false

      @guru = Guru.new(method(:report_duty_completion))

      # List the idle adept runners
      @idle_runners = Array.new()
      # Maps a duty id to the runners that performs this task
      @busy_runners = Hash.new()

      @runners_mutex = Mutex.new()

      # The lists of assigned and unassigned duties
      @assigned_duties = Hash.new()
      @unassigned_duties = Hash.new()

      @duties_mutex = Mutex.new()

      @config = Configuration.new(config_file_name)
    end

    # Add a duty to be performed given a callable +instructions+ block and
    # expecting it to return an +expected_value+. Perform the task immediately
    # or not, depending on the value of +perform_now+.
    def add_duty(instructions, expected_value, description = '', perform_now = true)
      if @stopping
        puts "Server stopping...discarding this job"
      end

      duty = @guru.add_duty(instructions, expected_value)

      @duties_mutex.synchronize() do
        @unassigned_duties[duty.id] = duty
      end

      allocate_duties() if perform_now
    end

    def reload_config()
      @config.reload()

      # FIXME: if the parallelism model changes, terminate every runner which
      # runs with the old model
      # Adjust the number of runners if the max changes.
    end

    def join()
      while true
        nb_assigned_jobs = nil
        nb_unassigned_jobs = nil

        @duties_mutex.synchronize() do
          nb_assigned_jobs = @assigned_duties.size()
          nb_unassigned_jobs = @unassigned_duties.size()
        end

        break if nb_assigned_jobs + nb_unassigned_jobs == 0

        if block_given?()
          yield nb_assigned_jobs, nb_unassigned_jobs
        end

        sleep @config['join_lookup_period_seconds'].to_i()
      end
    end

    def allocate_duties()
      while true
        shall_break = false

        @duties_mutex.synchronize() do
          shall_break = @unassigned_duties.empty?()
        end

        break if shall_break

        create_needed_runners()

        while true
          next_duty = nil
          next_duty_id = nil
          runner = nil

          @duties_mutex.synchronize() do
            if !@unassigned_duties.empty?()
              next_duty_id, next_duty = @unassigned_duties.shift()
              @assigned_duties[next_duty_id] = next_duty
            end
          end

          break if next_duty.nil?()

          @runners_mutex.synchronize() do
            if !next_duty.nil?() && !@idle_runners.empty?()
              runner = @idle_runners.pop()
              @busy_runners[next_duty_id] = runner
              runner.give_duty(next_duty, @config['persistent_worker'])
            end
          end

          # There is no runner available
          if runner.nil?()
            # Put the job back into the unassigned duties
            @duties_mutex.synchronize() do
              @assigned_duties.delete(next_duty_id)
              @unassigned_duties[next_duty_id] = next_duty
            end

            shall_break = true
            break
          end
        end

        break if shall_break
      end
    end

    # Create as many runners as possible, capped by the max_concurrents_duties
    # configuration value.
    # Return the number of created runners.
    def create_needed_runners()
      @runners_mutex.synchronize() do
        nb_runners = @idle_runners.size() + @busy_runners.size()
#        puts "#{nb_runners} running (#{@idle_runners.size()} idle, #{@busy_runners.size()} busy)"
        nb_runners_to_create = @config['max_concurrents_duties'].to_i() - nb_runners

        nb_runners_to_create.times do
          @idle_runners << spawn_adept_runner()
        end

        return nb_runners_to_create
      end
    end

    def stop()
      @stopping = true

      puts "Now stopping..."

      @duties_mutex.synchronize() do
        puts "Notice: discarding #{@unassigned_duties.size()} unassigned jobs" unless @unassigned_duties.empty?()
      end

      @runners_mutex.synchronize() do
        @busy_runners.each() do |runner|
          runner.stop()
        end
      end
    end

    def try_stop()
      if !busy?()
        stop()
        return true
      end

      return false
    end

    def jobs_left()
      @duties_mutex.synchronize() do
        return @unassigned_duties.size() + @assigned_duties.size()
      end
    end

    # Return true if at least one runner is busy
    def busy?()
      @runners_mutex.synchronize() do |runner|
        return true if runner.busy?()
      end

      return false
    end

    private
    PARALLELISM_MODEL_THREADS = 'thread'
    PARALLELISM_MODEL_PROCESSES = 'process'

    def spawn_adept_runner()
      adept_runner = nil
      parallelism_model = @config['parallelism_model']

      # TODO : allow an arbitrary runner model to be added (eg. over SSH)
      case parallelism_model
        when PARALLELISM_MODEL_THREADS
        adept_runner = AdeptThreadRunner.new()
        when PARALLELISM_MODEL_PROCESSES
        adept_runner = AdeptProcessRunner.new()
        else
        raise "Unknown parallelism model '#{parallelism_model}'"
      end

      return adept_runner
    end

    def report_duty_completion(duty_id, returned_value)
      adept_runner = nil

      @duties_mutex.synchronize() do
        runner = @busy_runners.delete(duty_id)
        @idle_runners << runner

        @assigned_duties.delete(duty_id)
      end

      # FIXME : handle other configuration changes, eg. running model
      harvest_supernumerary_runners()
      allocate_duties()
    end

    # Handle the possible change of the max concurrent duties configuration property.
    def harvest_supernumerary_runners()
      @runners_mutex.synchronize() do
        available_runners = @idle_runners.size() + @busy_runners.size()
        @idle_runners = @idle_runners[0..[0, @config['max_concurrents_duties'].to_i() - available_runners].max()]
      end
    end
  end
end
