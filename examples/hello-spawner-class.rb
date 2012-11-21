$LOAD_PATH.unshift("#{File.dirname(__FILE__)}/../lib")

require 'spawner'

s = Spawner::Conductor.new("#{File.dirname(__FILE__)}/../etc/config.yml")
1.upto(20) do |i|
  s.add_duty(Proc.new() { puts "Task #{i}" }, 0)
end

s.join() {|ass, unass| puts "Still #{ass} jobs running, #{unass} jobs in the queue"}
