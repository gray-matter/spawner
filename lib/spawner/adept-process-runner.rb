require 'drb'

module Spawner
  # Processes version of the AdeptRunner.
  class AdeptProcessRunner < AdeptRunner
    public
    # Construct an AdeptProcessRunner.
    def initialize()
      super()
      @adept_process_id = nil
      @reader_thread = nil
    end

    # See AdeptRunner#start
    def start(persistent_worker)
      # FIXME for distributed stuff
      DRb.start_service(nil, @duty_container)
      drb_uri = DRb.uri()
      spawn_process(drb_uri, persistent_worker)
    end

    # See AdeptRunner#stop
    def stop()
      if !@adept_process_id.nil?()
        Process.kill("KILL", @adept_process_id) rescue Errno::ESRCH
        @adept_process_id = nil
      end

      super()
    end

    # See AdeptRunner#alive?
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

    # See Object#to_s
    def to_s()
      return "#<AdeptProcessRunner: pid = #@adept_process_id>"
    end

    private
    # FIXME : configurable path
    RUN_ADEPT_SCRIPT = "#{File.dirname(__FILE__)}/../../bin/run-adept"

    # Spawn a process which will be persistent or not depending on
    # +persistent_worker+ and will be given a DutyContainer on the given
    # +drb_uri+.
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

          prefix = "[PID ##{@adept_process_id.to_i()}] "
          Spawner.jobs_logger.debug(prefix + my_out.readline() + "\n") if out_to_read
          Spawner.jobs_logger.error(prefix + my_err.readline() + "\n") if err_to_read

          IO.select([my_out, my_err]) unless out_to_read || err_to_read
        end
      end

      Process.detach(@adept_process_id)
    end

    # See AdeptRunner#wake_up
    def wake_up()
      Process.kill('CONT', @adept_process_id)
    end
  end
end
