require 'adept-runner'

module Spawner
  class AdeptThreadRunner < AdeptRunner
    public
    def initialize()
      super()
      @adept_thread = nil
    end

    def start(persistent_worker)
      # Thread.stop when the job is done, modify the job and run
      # FIXME : handle suicide duties
      @adept_thread = Thread.new() do
        begin
          duty = @duty_container.get_duty()

          if duty.nil?()
            Thread.stop()
          else
            begin
              @adept.give_duty(duty)
            rescue Exception => e
              duty.report_failure(e)
            end
          end
        end while persistent_worker
      end
    end

    def stop()
      @adept_thread.kill() unless @adept_thread.nil?()
    end

    def wake_up()
      @adept_thread.run() unless @adept_thread.nil?()
    end

    def alive?()
      return !@adept_thread.nil?() && @adept_thread.alive?()
    end
  end
end
