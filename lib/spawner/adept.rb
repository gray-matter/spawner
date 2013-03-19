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
      # This will be a proc everytime, which can be a problem if it
      # contains a return statement => transform into a lambda
      instructions_str = duty.get_instructions().sub('proc', 'lambda')
      instructions = eval(instructions_str)

      begin
        ret = instructions.call()
      rescue Exception => e
        duty.report_failure(e)
      end

      duty.report_completion(ret)
    end
  end
end
