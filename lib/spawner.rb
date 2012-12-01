$LOAD_PATH.unshift("#{File.dirname(__FILE__)}")

require 'logger'

module Spawner
  autoload :Conductor, 'spawner/conductor'
  autoload :Adept, 'spawner/adept'

  public
  def self.internal_logger()
    return @@internal_logger
  end

  def self.jobs_logger()
    return @@jobs_logger
  end

  def self.set_internal_log_file(file_name)
    @@internal_logger = log_from_file_name(file_name, STDOUT)
  end

  def self.set_jobs_log_file(file_name)
    @@jobs_logger = log_from_file_name(file_name, STDOUT)
  end

  private
  def self.log_from_file_name(file_name, default_stream)
    # TODO: handle failures
    output_stream = nil

    if file_name.nil?()
      output_stream = default_stream
    else
      output_stream = File.new(file_name)
    end

    @@logger_mutex ||= Mutex.new()

    @@logger_mutex.synchronize() do
      return Logger.new(output_stream)
    end
  end
end
