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
            @adept.give_duty(duty)
          end
        end while persistent_worker
      end
    end

    def stop()
      @adept_thread.kill()
    end

    def wake_up()
      @adept_thread.run()
    end

    def alive?()
      return !@adept_thread.nil?() && @adept_thread.alive?()
    end
  end
end
