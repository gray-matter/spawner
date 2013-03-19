require 'adept-runner'
require 'thread'
require 'string-io-redirector'

module Spawner
  class AdeptThreadRunner < AdeptRunner
    private
    # The sole purpose of this class is to allow to redirect writes to
    # std{out,err} to a method
    class StringIORedirector < StringIO
      def initialize(logger_method)
        @logger_method = logger_method
        super()
      end

      # FIXME: when writing without a line return, this will screw the log...
      def write(*args)
        @logger_method.call(*args)
      end
    end

    public
    def initialize()
      super()
      @adept_thread = nil
      @no_more_duty_cond = ConditionVariable.new()
      @adept = Adept.new()
    end

    def start(persistent_worker)
      @adept_thread = Thread.new() do
        Thread.abort_on_exception = true
        begin
          @job_mutex.synchronize() do
            begin
              duty = @duty_container.get_duty()
              previous_stderr = nil
              previous_stdout = nil

              raise "Getting the duty for #{object_id} returns nil, please report this" if duty.nil?()

              begin
                previous_stdout, $stdout = $stdout, StringIORedirector.new(Spawner.jobs_logger.method(:debug))
                previous_stderr, $stderr = $stderr, StringIORedirector.new(Spawner.jobs_logger.method(:error))
                @adept.give_duty(duty)
              rescue Exception => e
                duty.report_failure(e)
              ensure
                $stdout = previous_stdout unless previous_stdout.nil?()
                $stderr = previous_stderr unless previous_stderr.nil?()
              end

              @no_more_duty_cond.wait(@job_mutex) if persistent_worker
            end while persistent_worker
          end
        rescue Exception => e
          Spawner.spawner_logger.error("Exception raised in the thread runner: #{e}\n")
        end
      end
    end

    def stop()
      unless @adept_thread.nil?()
        @adept_thread.kill()
      end

      super()
    end

    def to_s()
      return "#<AdeptThreadRunner: thread id = #{@adept_thread.object_id}>"
    end

    def wake_up()
      Thread.new() do
        @job_mutex.synchronize() do
          @no_more_duty_cond.signal()
        end
      end
    end

    def alive?()
      return !@adept_thread.nil?() && @adept_thread.alive?()
    end
  end
end
