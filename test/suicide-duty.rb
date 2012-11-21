$LOAD_PATH.unshift("#{File.dirname(__FILE__)}/../lib")

require 'spawner'

s = Spawner::Conductor.new("#{File.dirname(__FILE__)}/../etc/config.yml")
1.upto(10) do |i|
  s.add_duty(Proc.new() { exit 1 }, 0)
end

s.join() {|ass, unass| puts "Still #{ass} jobs running, #{unass} jobs in the queue"}
