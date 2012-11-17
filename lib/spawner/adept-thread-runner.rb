require 'adept-runner'

module Spawner
  class AdeptThreadRunner < AdeptRunner
    public
    def initialize()
      @adept = Adept.new()
      @adept_thread = nil
      @persistent_worker = nil
    end

    def give_duty(duty, persistent_worker)
      # FIXME : handle the persistent_worker value
      # Thread.stop when the job is done, modify the job and run

      @adept_thread = Thread.new() do
        while @persistent_worker
          @adept.give_duty(@current_duty)
        end
      end
    end

    def stop()
      @adept_thread.kill()
    end

    def alive?()
      return @adept_thread.alive?()
    end
  end
end
