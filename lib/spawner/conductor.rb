# FIXME
$LOAD_PATH.unshift("#{File.dirname(__FILE__)}/..")
$LOAD_PATH.unshift("#{File.dirname(__FILE__)}")

require 'spawner'
require 'guru'
require 'adept'
require 'configuration'
require 'adept-thread-runner'
require 'adept-process-runner'
require 'set'
require 'logger'

module Spawner
  class Conductor
    public
    def initialize(config_file_name)
      Thread.abort_on_exception = true

      @config = Configuration.new()
      reload_config(config_file_name)

      @stopping = false

      @guru = Guru.new(method(:report_duty_completion))

      # List the idle adept runners
      @idle_runners = Array.new()
      # Maps a duty id to the runners that performs this task
      @busy_runners = Hash.new()
      # Keep track of every runner that is btw the idle and busy states, to
      # avoid creating unnecessary runners
      @living_dead_runners = Set.new()

      @runners_mutex = Mutex.new()

      # The lists of assigned and unassigned duties
      @assigned_duties = Hash.new()
      @unassigned_duties = Hash.new()

      @duties_mutex = Mutex.new()

      # These fields are required for implementing the "join" method
      @joining_thread = nil
      @joining_thread_stopping = false
      @joining_thread_mutex = Mutex.new()

      # FIXME : add a pseudo-global logger, since maintaining a local one would
      # be painful (need to pass its changes to other classes using it)
      @logger = nil
      @log_file_name = nil
      @logger_mutex = Mutex.new()
    end

    # Add a duty to be performed given a callable +instructions+ block and
    # expecting it to return an +expected_value+. Perform the task immediately
    # or not, depending on the value of +perform_now+.
    def add_duty(instructions, expected_value, description = '', perform_now = true)
      if @stopping
        Spawner. "Server stopping...discarding this job"
      end

      duty = @guru.add_duty(instructions, expected_value)

      @duties_mutex.synchronize() do
        @unassigned_duties[duty.id] = duty
      end

      allocate_duties() if perform_now
    end

    def reload_config(config_file_name = nil)
      @config.reload(config_file_name)

      Spawner.set_internal_log_file(@config['spawner_log_file_name'])
      Spawner.set_jobs_log_file(@config['jobs_log_file_name'])

      # FIXME: if the parallelism model changes, terminate every runner which
      # runs with the old model
    end

    def join()
      @joining_thread = Thread.new() do
        while true
          nb_assigned_jobs = nil
          nb_unassigned_jobs = nil
          shall_break = false

          @joining_thread_mutex.synchronize() do
            @duties_mutex.synchronize() do
              nb_assigned_jobs = @assigned_duties.size()
              nb_unassigned_jobs = @unassigned_duties.size()
            end

            shall_break = nb_assigned_jobs + nb_unassigned_jobs == 0

            if !shall_break && block_given?()
              yield nb_assigned_jobs, nb_unassigned_jobs
            end

            @joining_thread_stopping = shall_break
          end

          break if shall_break

          # Wait for someone to wake us up to notify that a duty may have been
          # completed
          Thread.stop()

          @joining_thread_stopping = false
        end

        # This should be useless
        @runners_mutex.synchronize() do
          @busy_runners.each() do |unused, runner|
            runner.stop()
          end
        end

        @runners_mutex.synchronize() do
          @idle_runners.each() do |runner|
            runner.stop()
          end
        end
      end

      # Hack: if we don't specify a timeout, join will throw a "deadlock detected"
      # exception when the worker thread hits the "Thread.stop()" part, thinking that
      # we're waiting for something that will never happen, even though a "CONT"
      # signal might (and should) wake it up.
      @joining_thread.join(HUGE_TIMEOUT_TO_AVOID_DEADLOCK)
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

          @runners_mutex.synchronize() do
            if !@idle_runners.empty?()
              runner = @idle_runners.pop()
              @living_dead_runners << runner
            end
          end

          # There is no runner available
          if runner.nil?()
            shall_break = true
            break
          end

          @duties_mutex.synchronize() do
            if !@unassigned_duties.empty?()
              next_duty_id, next_duty = @unassigned_duties.shift()
              @assigned_duties[next_duty_id] = next_duty
            end
          end

          if next_duty.nil?()
            # Put the runner back into the idle runners
            @runners_mutex.synchronize() do
              @idle_runners << runner
              @living_dead_runners.delete(runner)
            end

            shall_break = true
            break
          else
            @runners_mutex.synchronize() do
              @busy_runners[next_duty_id] = runner
              @living_dead_runners.delete(runner)
              runner.give_duty(next_duty, @config['persistent_workers'])
            end
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
        nb_runners = @idle_runners.size() + @busy_runners.size() + @living_dead_runners.size()
        nb_runners_to_create = @config['max_concurrents_duties'].to_i() - nb_runners

        nb_runners_to_create.times do
          @idle_runners << spawn_adept_runner()
        end

        return nb_runners_to_create
      end
    end

    def stop()
      @stopping = true

      Spawner.internal_logger.info("Now stopping...")

      @duties_mutex.synchronize() do
        if !@unassigned_duties.empty?()
          Spawner.internal_logger.info("Notice: discarding #{@unassigned_duties.size()} unassigned jobs")
        end
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
    HUGE_TIMEOUT_TO_AVOID_DEADLOCK = 42424242

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

      # The tests below are needed to avoid waking the thread for nothing just
      # before he stops
      if !@joining_thread.nil?()
        @joining_thread_mutex.lock()

        if @joining_thread_stopping
          @joining_thread_mutex.unlock()

          # Wait for the worker to stop for real...this shouldn't be too long
          while !@joining_thread.stop?()
            Thread.pass()
          end

          # If we are trying to join, let him wake up to test if he may return
        else
          @joining_thread_mutex.unlock()
        end

        @joining_thread.run()
      end
    end

    # Handle the possible change of the max concurrent duties configuration property.
    def harvest_supernumerary_runners()
      @runners_mutex.synchronize() do
        available_runners = @idle_runners.size() + @busy_runners.size() + @living_dead_runners.size()
        max_concurrents_duties = @config['max_concurrents_duties']

        if available_runners > max_concurrents_duties
          nb_removable_items = [@idle_runners.size(), available_runners - max_concurrents_duties].min()

          @idle_runners[0, nb_removable_items].each() do |runner_to_kill|
            runner_to_kill.stop()
          end

          @idle_runners = @idle_runners[nb_removable_items..-1]
        end
      end
    end
  end
end
