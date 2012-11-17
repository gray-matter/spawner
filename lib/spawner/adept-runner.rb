module Spawner
  class AdeptRunner
    public
    # FIXME: handle @config['persistant_workers']
    def stop()
      not_implemented()
    end

    def try_stop()
      if !busy?()
        stop()
        return true
      end

      return false
    end

    def busy?()
      not_implemented()
    end

    private
    def not_implemented()
      raise NotImplementedError.new()
    end
  end
end
