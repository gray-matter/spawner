require 'spawner'
s = Spawner::Conductor.new('/path/to/config.yml')
1.upto(10) do |i|
  s.add_duty(Proc.new() { puts "Task #{i}" })
end
s.join()
