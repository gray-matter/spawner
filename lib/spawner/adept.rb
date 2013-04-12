require 'drb'

module Spawner
  # Emulate a lookup stack based on the given bindings.
  # As seen on https://github.com/niklasb/ruby-dynamic-binding
  class LookupStack
    def initialize(bindings = [])
      @bindings = bindings
    end

    def method_missing(m, *args)
      @bindings.reverse_each do |bind|
        begin
          value = eval(m.to_s, bind)
          return value
        rescue NameError
        end
      end
      raise NoMethodError, "No such variable: %s" % m
    end

    def run_proc(p)
      instance_exec(&p)
    end
  end
end

class Proc
  # Call the proc, providing it the needed +bindings+.
  def call_with_binding(bindings)
    Spawner::LookupStack.new([bindings]).run_proc(self)
  end
end

module Spawner
  # An Adept will follow blindly what its duty requires him to do
  class Adept
    public

    REQUIRED_DUTY_METHODS = ['get_instructions_and_binding', 'report_completion']

    # Give a +duty+ to this adept for him to perform it.
    def give_duty(duty)
      REQUIRED_DUTY_METHODS.each() do |method|
        raise "I will not perform my duty because it doesn't say how to '#{method}'" if !duty.respond_to?(method)
      end

      perform_duty(duty)
    end

    private

    # Perform the given +duty+.
    def perform_duty(duty)
      # This will be a proc everytime, which can be a problem if it
      # contains a return statement => transform into a lambda
      instructions_str, binding = duty.get_instructions_and_binding()
      instructions = eval(instructions_str.sub('proc', 'lambda'))

      begin
        ret = instructions.call_with_binding(binding)
      rescue Exception => e
        duty.report_failure(e)
      else
        duty.report_completion(ret)
      end
    end
  end
end
