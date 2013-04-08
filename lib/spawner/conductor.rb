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

      @config = Configuration.new(EXPECTED_CONFIGURATION_KEYS, DEFAULT_CONFIGURATION_VALUES)
      @config_mutex = Mutex.new()

      @stopping = false

      @guru = Guru.new()
      @guru.register_duty_end_callback(method(:report_duty_end))

      # List the idle adept runners
      @idle_runners = Array.new()
      # Maps a duty id to the runner that performs this task
      @busy_runners = Hash.new()
      @runners_mutex = Mutex.new()

      # This field is required for implementing the "join" method
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
        @guru.add_duty(instructions, expected_value, @config[:max_retries])
      end

      allocate_duties() if perform_now
    end

    # Load the configuration from a file with a given +file_path+.
    # Return true if the loading was successful, false otherwise.
    # Throw an exception if the loading failed and there was no fallback
    # configuration.
    #
    # Called with no argument, this will reload the previous configuration
    # file, if any.
    def load_config_from_file(config_file_path = nil)
      return load_config_from_source(@config.method(:reload), config_file_path)
    end

    alias reload_config load_config_from_file

    # Load the configuration from a given +config_hash+.
    # Return true if the loading was successful, false otherwise.
    # Throw an exception if the loading failed and there was no fallback
    # configuration.
    def load_config_from_hash(config_hash)
      return load_config_from_source(@config.method(:load_from_hash), config_hash)
    end

    # Wait for all duties to be performed, then return.
    def join()
      go_to_termination(true)
    end

    # Wait for the conductor to be stopped.
    def wait()
      go_to_termination(false)
    end

    def start()
      create_needed_runners()

      @runners_mutex.synchronize() do
        duty_id_to_runner_mapping = @guru.assign_duties(@idle_runners,
                                                        @config[:persistent_workers])

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
      return @guru.duties_left()
    end

    private
    HUGE_TIMEOUT_TO_AVOID_DEADLOCK = 42424242
    EXPECTED_CONFIGURATION_KEYS = [:max_concurrents_duties, :parallelism_model, :persistent_workers]
    DEFAULT_CONFIGURATION_VALUES = {:max_retries => 0}

    alias allocate_duties start

    # Load the configuration from a given +source+, using a given
    # +loading_method+.
    # Return true if the loading was successful, false otherwise.
    # Throw an exception if the loading failed and there was no fallback
    # configuration.
    def load_config_from_source(loading_method, source)
      @config_mutex.synchronize() do
        old_configuration = @config.clone()

        begin
          loading_method.call(source)
        rescue Exception => e
          handle_corrupted_config(e)
          return false
        end

        on_configuration_reloaded(old_configuration, @config)

        return true
      end
    end

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

    # Handle the case of a corrupted configuration.
    # If there was no configuration before, throw an exception; otherwise, do
    # nothing.
    def handle_corrupted_config(exc)
      Spawner.spawner_logger.error("Corrupted configuration: #{exc}\n") unless Spawner.spawner_logger.nil?()

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

    def go_to_termination(stop_when_done)
      @joining_thread = Thread.new() do
        while true
          shall_break = false

          @runners_mutex.synchronize() do
            if stop_when_done && @busy_runners.empty?()
              shall_break = true
              break
            else
              if block_given?()
                yield @guru.duties_count_breakdown()
              end

              @no_more_duty_cond.wait(@runners_mutex)
            end
          end

          break if shall_break
        end
      end

      @joining_thread.join(HUGE_TIMEOUT_TO_AVOID_DEADLOCK)
    end
  end
end
