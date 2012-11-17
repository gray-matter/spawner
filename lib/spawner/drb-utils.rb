module Spawner
  module DRbUtils
    private
    MAX_PORT_NUMBER = 65535

    public
    def self.bind_on_next_available_port(host, start_port, obj)
      start_port.upto(MAX_PORT_NUMBER) do |port|
        begin
          drb_uri = "druby://#{host}:#{port}"
          DRb.start_service(drb_uri, obj)

          return drb_uri
        rescue Errno::EADDRINUSE
        rescue Errno::EACCES
        end
      end

      return nil
    end
  end
end
