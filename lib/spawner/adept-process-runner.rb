require 'adept-runner'
require 'drb-utils'

module Spawner
  class AdeptProcessRunner < AdeptRunner
    public
    # FIXME : configurable path
    RUN_ADEPT_SCRIPT = "#{File.dirname(__FILE__)}/../../bin/run-adept"

    def initialize()
      @adept = Adept.new()
      @adept_process_id = nil
      @persistent_worker = nil
    end

    def give_duty(duty, persistent_worker)
      # FIXME : handle the persistent_worker value
      # If the worker was persistent before and should not be anymore, then kill
      # it and spawn a new one

      # FIXME for distributed stuff
      drb_uri = DRbUtils::bind_on_next_available_port("localhost", 4242, duty)

      if persistent_worker
        @adept_process_id = Process.spawn(RUN_ADEPT_SCRIPT, '--persistent', drb_uri)
      else
        @adept_process_id = Process.spawn(RUN_ADEPT_SCRIPT, drb_uri)
      end
    end

    def stop()
      @adept_thread.kill()
    end

    def alive?()
      begin
        # Kill 0 just checks if the process is still alive
        Process.kill(0, pid.to_i)
        true
      rescue Errno::ESRCH
        false
      end
    end
  end
end
