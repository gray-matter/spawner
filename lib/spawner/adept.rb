require 'drb'

module Spawner
  # An Adept will follow blindly what its duty requires him to do
  class Adept
    public

    REQUIRED_DUTY_METHODS = ['get_instructions', 'report_completion']

    def give_duty(duty)
      REQUIRED_DUTY_METHODS.each() do |method|
        raise "I will not perform my duty because it doesn't say how to '#{method}'" if !duty.respond_to?(method)
      end

      perform_duty(duty)
    end

    private

    def perform_duty(duty)
      instructions = duty.get_instructions()
      ret = instructions.call()
      duty.report_completion(ret)
    end
  end
end