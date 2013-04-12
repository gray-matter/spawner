require 'yaml'

module Spawner
  # Simple configuration class, mainly (but not only !) designed to handle
  # mandatory and optional keys constraints as well as thread-safety.
  class Configuration
    public
    # Construct a Configuration object which will expect the given
    # +expected_keys+ from any configuration source it is
    # given. +default_values+ is a hash table with default values to be used
    # whenever the value is not present in the configuration source.
    def initialize(expected_keys, default_values)
      @config_mutex = Mutex.new()
      @config_file_name = nil
      @expected_keys = expected_keys
      @default_values = default_values
      @config = nil
    end

    # Load the configuration from the given +config_hash+.
    def load_from_hash(config_hash)
      new_config = fill_with_default_values(symbolize_keys(config_hash))
      validate_config(new_config)
      @config = new_config
    end

    # Load the configuration from the given file with the +config_file_name+
    # path.
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

    # Reload the configuration from the last file from which the configuration
    # was loaded for the last time.
    alias reload load_from_file

    # Get the given +key+ from the loaded configuration.
    def [](key)
      @config_mutex.synchronize() do
        return @config[key]
      end
    end

    # Calls block once for each element in self, passing that element as a
    # parameter.
    # If no block is given, an enumerator is returned instead.
    def each(&block)
      if block_given?()
        @config.each() do |k, v|
          yield k, v
        end
      else
        @config.each()
      end
    end

    # Tells whether this configuration is valid or not, mainly whether it's
    # missing mandatory keys or not, or if it's nil.
    def valid?()
      @config_mutex.synchronize() do
        return !@config.nil?() && missing_keys(@config).empty?()
      end
    end

    # Clone this configuration.
    def clone()
      res = self.class().new(@expected_keys.clone(), @default_values.clone())
      res.config = self.config.clone() unless self.config.nil?()

      return res
    end

    protected

    # Get the internal config dictionary.
    attr_accessor :config

    private

    # Turn +hash+ keys into symbols.
    def symbolize_keys(hash)
      return Hash[hash.map{ |k, v| [k.to_sym, v] }]
    end

    # Return the keys missing in the given +config+.
    def missing_keys(config)
      return @expected_keys - config.keys
    end

    # Validate the configuration, throwing an exception if the configuration is
    # invalid.
    def validate_config(new_config)
      missing = missing_keys(new_config)

      if !missing.empty?()
        raise "Bad configuration file: missing the #{missing.join(', ')} key(s)"
      end
    end

    # Fill the given +config+ with the default values given in the constructor.
    def fill_with_default_values(config)
      return @default_values.merge(config)
    end
  end
end
