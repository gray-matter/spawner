$LOAD_PATH.unshift("#{File.dirname(__FILE__)}")

require 'logger'

module Spawner
  public

  autoload :Conductor, 'spawner/conductor'
  autoload :Adept, 'spawner/adept'

  def self.set_spawner_log_file(file_name)
    @spawner_logger = logger_from_file_name(file_name, STDOUT)
  end

  def self.set_jobs_log_file(file_name)
    @jobs_logger = logger_from_file_name(file_name, STDOUT)
  end

  def self.set_spawner_log_file(file_name)
    @spawner_logger = logger_from_file_name(file_name, STDOUT)
  end

  def self.jobs_logger()
    @jobs_logger
  end

  def self.spawner_logger()
    @jobs_logger
  end

  private
  def self.logger_from_file_name(file_name, default_stream)
    # TODO: handle failures
    output_stream = nil

    if file_name.nil?()
      output_stream = default_stream
    else
      output_stream = File.new(file_name, "w")
    end

    @@logger_mutex ||= Mutex.new()

    @@logger_mutex.synchronize() do
      return Logger.new(output_stream)
    end
  end
end
