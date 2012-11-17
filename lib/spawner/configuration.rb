require 'yaml'

module Spawner
  class Configuration
    EXPECTED_KEYS = ['max_concurrents_duties', 'parallelism_model',
                     'persistent_workers', 'join_lookup_period_seconds']

    public
    def initialize(config_file_name)
      @config_file_name = config_file_name
      @config_mutex = Mutex.new()
      load()
    end

    def load()
      new_config = YAML.load_file(@config_file_name)
      missing_keys = EXPECTED_KEYS - new_config.keys()

      if !missing_keys.empty?()
        raise "Bad configuration file: missing the #{missing_keys.join(', ')} key(s)"
      else
        @config_mutex.synchronize() do
          @config = new_config
        end
      end
    end

    alias reload load

    def [](key)
      @config_mutex.synchronize() do
        return @config[key]
      end
    end
  end
end
