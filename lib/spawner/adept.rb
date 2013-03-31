$LOAD_PATH.unshift(File.dirname(__FILE__))

require 'drb'
require 'dynamic-binding'

module Spawner
  # An Adept will follow blindly what its duty requires him to do
  class Adept
    public

    REQUIRED_DUTY_METHODS = ['get_instructions_and_binding', 'report_completion']

    def give_duty(duty)
      REQUIRED_DUTY_METHODS.each() do |method|
        raise "I will not perform my duty because it doesn't say how to '#{method}'" if !duty.respond_to?(method)
      end

      perform_duty(duty)
    end

    private

    def perform_duty(duty)
      # This will be a proc everytime, which can be a problem if it
      # contains a return statement => transform into a lambda
      instructions_str, binding = duty.get_instructions_and_binding()
      instructions = eval(instructions_str.sub('proc', 'lambda'))

      begin
        ret = instructions.call_with_binding(binding)
      rescue Exception => e
        duty.report_failure(e)
      end

      duty.report_completion(ret)
    end
  end
end
