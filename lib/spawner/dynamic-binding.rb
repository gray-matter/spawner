module Spawner
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
  def call_with_binding(bind)
    Spawner::LookupStack.new([bind]).run_proc(self)
  end
end
