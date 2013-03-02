require 'adept-runner'
require 'duty-container'
require 'drb'

module Spawner
  class AdeptProcessRunner < AdeptRunner
    public
    def initialize()
      super()
      @adept_process_id = nil
      @reader_thread = nil
    end

    def start(persistent_worker)
      # FIXME for distributed stuff
      DRb.start_service(nil, @duty_container)
      drb_uri = DRb.uri()
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
      # FIXME : make this work under Window$
      my_out, its_out = IO.pipe()
      my_err, its_err = IO.pipe()

      its_out.sync = true
      its_err.sync = true

      if persistent_worker
        @adept_process_id = Process.spawn(RUN_ADEPT_SCRIPT, '--persistent',
                                          drb_uri,
                                          {:out => its_out, :err => its_err})
      else
        @adept_process_id = Process.spawn(RUN_ADEPT_SCRIPT, drb_uri,
                                          {:out => its_out, :err => its_err})
      end

      its_out.close()
      its_err.close()

      @reader_thread = Thread.new() do
        while !my_out.eof?() || !my_err.eof?()
          out_to_read = false
          err_to_read = false

          begin
            my_out.ungetbyte(my_out.read_nonblock(1))
            out_to_read = true
          rescue Errno::EAGAIN, EOFError
          end

          begin
            my_err.ungetbyte(my_err.read_nonblock(1))
            err_to_read = true
          rescue Errno::EAGAIN, EOFError
          end

          # FIXME: format the output to display the job id
          Spawner.jobs_logger.info(my_out.readline()) if out_to_read
          Spawner.jobs_logger.error(my_err.readline()) if err_to_read

          IO.select([my_out, my_err]) unless out_to_read || err_to_read
        end
      end

      Process.detach(@adept_process_id)
    end

    def wake_up()
      Process.kill('CONT', @adept_process_id)
    end
  end
end
