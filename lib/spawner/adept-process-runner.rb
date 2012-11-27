require 'adept-runner'
require 'drb-utils'
require 'duty-container'

module Spawner
  class AdeptProcessRunner < AdeptRunner
    public
    def initialize()
      super()
      @adept_process_id = nil
    end

    def start(persistent_worker)
      # FIXME for distributed stuff
      drb_uri = DRbUtils::bind_on_next_available_port("localhost", 4242, @duty_container)
      spawn_process(drb_uri, persistent_worker)
    end

    def stop()
      if !@adept_process_id.nil?()
        Process.kill("KILL", @adept_process_id) rescue Errno::ESRCH
        @adept_process_id = nil
      end
    end

    def alive?()
      return false if @adept_process_id.nil?()

      begin
        # Kill 0 just checks if the process is still alive
        Process.kill(0, @adept_process_id)
        true
      rescue Errno::ESRCH
        false
      end
    end

    private
    # FIXME : configurable path
    RUN_ADEPT_SCRIPT = "#{File.dirname(__FILE__)}/../../bin/run-adept"

    def spawn_process(drb_uri, persistent_worker)
      if persistent_worker
        @adept_process_id = Process.spawn(RUN_ADEPT_SCRIPT, '--persistent', drb_uri)
      else
        @adept_process_id = Process.spawn(RUN_ADEPT_SCRIPT, drb_uri)
      end

      Process.detach(@adept_process_id)
    end

    def wake_up()
      Process.kill('CONT', @adept_process_id)
    end
  end
end
