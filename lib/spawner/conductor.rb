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
require 'thread'

module Spawner
  # The Conductor is the emerged part of the spawner library.
  # It is in charge of creating (aka recruiting) adepts whenever needed.
  class Conductor
    public
    PARALLELISM_MODEL_THREADS = 'thread'
    PARALLELISM_MODEL_PROCESSES = 'process'

    def initialize()
      Thread.abort_on_exception = true

      @config = Configuration.new(EXPECTED_CONFIGURATION_KEYS)
      @config_mutex = Mutex.new()

      @stopping = false

      @guru = Guru.new()
      @guru.register_duty_end_callback(method(:report_duty_end))

      # List the idle adept runners
      @idle_runners = Array.new()
      # Maps a duty id to the runner that performs this task
      @busy_runners = Hash.new()
      @runners_mutex = Mutex.new()

      # These fields are required for implementing the "join" method
      @joining_thread = nil
      @no_more_duty_cond = ConditionVariable.new()
    end

    # Add a duty to be performed given a callable +instructions+ block and
    # expecting it to return an +expected_value+. Perform the task immediately
    # or not, depending on the value of +perform_now+.
    def add_duty(expected_value = nil, perform_now = true, &instructions)
      if @stopping
        Spawner.spawner_logger.info("Server stopping...discarding this job\n")
      end

      @runners_mutex.synchronize() do
        @guru.add_duty(instructions, expected_value)
      end

      allocate_duties() if perform_now
    end

    # Called without argument, this will reload the previous configuration
    # file, if any
    def load_config_from_file(config_file_name = nil)
      @config_mutex.synchronize() do
        old_configuration = @config.clone()

        begin
          @config.reload(config_file_name)
        rescue Exception => e
          handle_corrupted_config(e)
        end

        on_configuration_reloaded(old_configuration, @config)
      end
    end

    alias reload_config load_config_from_file

    def load_config_from_hash(config_hash)
      @config_mutex.synchronize() do
        old_configuration = @config.clone()

        begin
          @config.load_from_hash(config_hash)
        rescue Exception => e
          handle_corrupted_config(e)
        end

        on_configuration_reloaded(old_configuration, @config)
      end
    end

    def join()
      @joining_thread = Thread.new() do
        while true
          shall_break = false

          @runners_mutex.synchronize() do
            if !@busy_runners.empty?()
              if block_given?()
                yield @guru.duties_count_breakdown()
              end

              @no_more_duty_cond.wait(@runners_mutex)
            else
              shall_break = true
              break
            end
          end

          break if shall_break
        end

        @runners_mutex.synchronize() do
          @busy_runners.each() do |unused, runner|
            runner.stop()
          end

          @idle_runners.each() do |runner|
            runner.stop()
          end
        end
      end

      @joining_thread.join(HUGE_TIMEOUT_TO_AVOID_DEADLOCK)
    end

    def start()
      create_needed_runners()

      @runners_mutex.synchronize() do
        duty_id_to_runner_mapping = @guru.assign_duties(@idle_runners, @config[:persistent_workers])

        if !duty_id_to_runner_mapping.empty?()
          @idle_runners -= duty_id_to_runner_mapping.values()
          @busy_runners.merge!(duty_id_to_runner_mapping)
        end
      end
    end

    def stop()
      @stopping = true

      Spawner.spawner_logger.info("Now stopping...\n")

      @runners_mutex.synchronize() do
        (@busy_runners + @idle_runners).each() do |runner|
          runner.stop()
        end
      end
    end

    def jobs_left()
      return assign_duties_count()
    end

    private
    HUGE_TIMEOUT_TO_AVOID_DEADLOCK = 42424242
    EXPECTED_CONFIGURATION_KEYS = [:max_concurrents_duties, :parallelism_model, :persistent_workers]

    alias allocate_duties start

    # Create as many runners as possible, capped by the max_concurrents_duties
    # configuration value.
    # Return the number of created runners.
    def create_needed_runners()
      @runners_mutex.synchronize() do
        nb_runners = @idle_runners.size() + @busy_runners.size()
        nb_runners_to_create = @config[:max_concurrents_duties].to_i() - nb_runners

        nb_runners_to_create.times do
          @idle_runners << spawn_adept_runner()
        end
      end
    end

    def spawn_adept_runner()
      adept_runner = nil
      parallelism_model = @config[:parallelism_model]

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

    def report_duty_end(duty_id)
      @runners_mutex.synchronize() do
        runner = @busy_runners.delete(duty_id)

        raise "The busy runners list is corrupted, please report this (duty_id = #{duty_id})" if runner.nil?()

        @idle_runners << runner
      end

      # FIXME : handle other configuration changes, eg. running model
      harvest_supernumerary_runners()
      allocate_duties()

      @runners_mutex.synchronize() do
        @no_more_duty_cond.signal()
      end
    end

    # Handle the possible change of the max concurrent duties configuration property.
    def harvest_supernumerary_runners()
      @runners_mutex.synchronize() do
        available_runners = @idle_runners.size() + @busy_runners.size()
        max_concurrents_duties = @config[:max_concurrents_duties]

        if available_runners > max_concurrents_duties
          nb_removable_items = [@idle_runners.size(), available_runners - max_concurrents_duties].min()

          @idle_runners[0, nb_removable_items].each() do |runner_to_kill|
            runner_to_kill.stop()
          end

          @idle_runners = @idle_runners[nb_removable_items..-1]
        end
      end
    end

    def handle_corrupted_config(exc)
      Spawner.spawner_logger.error("Corrupted configuration: #{exc}\n") unless Spawner.spawner_logger.nil?()

      # If there's no configuration at all, crash; otherwise, use the last
      # known configuration
      raise exc unless @config.valid?()
    end

    def on_configuration_reloaded(old_configuration, new_configuration)
      # There's no need to reload the logger if the path has not changed !
      [[:jobs_log_file_name, :set_jobs_log_file],
       [:spawner_log_file_name, :set_spawner_log_file]].each() do |vals|
        key, meth = vals

        if !old_configuration.valid?() || new_configuration[key] != old_configuration[key]
          Spawner.send(meth, new_configuration[key])
        end
      end

      # FIXME: if the parallelism model changes, terminate every runner which
      # runs with the old model
    end
  end
end
