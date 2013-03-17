require 'yaml'

module Spawner
  class Configuration
    public
    def initialize(expected_keys)
      @config_mutex = Mutex.new()
      @config_file_name = nil
      @expected_keys = expected_keys
    end

    def load_from_hash(config_hash)
      new_config = symbolize_keys(config_hash)
      validate_config(config_hash)
      @config = config_hash
    end

    def load_from_file(config_file_name = nil)
      @config_mutex.synchronize() do
        config_file_name ||= @config_file_name

        begin
          new_config = symbolize_keys(YAML.load_file(config_file_name))
        rescue Exception => e
          raise "Bad configuration file given #{e.to_s()}"
        end

        validate_config(new_config)

        @config = new_config
        @config_file_name = nil
      end
    end

    alias reload load_from_file

    def [](key)
      @config_mutex.synchronize() do
        return @config[key]
      end
    end

    def valid?()
      @config_mutex.synchronize() do
        return !@config.nil?() && (@expected_keys - @config.keys).empty?()
      end
    end

    private
    def symbolize_keys(hash)
      return Hash[hash.map{ |k, v| [k.to_sym, v] }]
    end

    def validate_config(new_config)
      missing_keys = @expected_keys - new_config.keys()

      if !missing_keys.empty?()
        raise "Bad configuration file: missing the #{missing_keys.join(', ')} key(s)"
      end
    end
  end
end
