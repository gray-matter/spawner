require 'yaml'

module Spawner
  class Configuration
    public
    def initialize(expected_keys, default_values)
      @config_mutex = Mutex.new()
      @config_file_name = nil
      @expected_keys = expected_keys
      @default_values = default_values
      @config = nil
    end

    def load_from_hash(config_hash)
      new_config = fill_with_default_values(symbolize_keys(config_hash))
      validate_config(new_config)
      @config = new_config
    end

    def load_from_file(config_file_name = nil)
      @config_mutex.synchronize() do
        config_file_name ||= @config_file_name

        begin
          new_config = symbolize_keys(YAML.load_file(config_file_name))
        rescue Exception => e
          raise "Bad configuration file given: #{e.to_s()}"
        end

        new_config = fill_with_default_values(new_config)
        validate_config(new_config)

        @config = new_config
        @config_file_name = config_file_name
      end
    end

    alias reload load_from_file

    def [](key)
      @config_mutex.synchronize() do
        return @config[key]
      end
    end

    def each(&block)
      if block_given?()
        @config.each() do |k, v|
          yield k, v
        end
      else
        @config.each()
      end
    end

    def valid?()
      @config_mutex.synchronize() do
        return !@config.nil?() && missing_keys(@config).empty?()
      end
    end

    def clone()
      res = self.class().new(@expected_keys.clone(), @default_values.clone())
      res.config = self.config.clone() unless self.config.nil?()

      return res
    end

    protected
    attr_accessor :config

    def symbolize_keys(hash)
      return Hash[hash.map{ |k, v| [k.to_sym, v] }]
    end

    def missing_keys(config)
      return @expected_keys - config.keys
    end

    def validate_config(new_config)
      missing = missing_keys(new_config)

      if !missing.empty?()
        raise "Bad configuration file: missing the #{missing.join(', ')} key(s)"
      end
    end

    def fill_with_default_values(config)
      return @default_values.merge(config)
    end
  end
end
