$LOAD_PATH.unshift("#{File.dirname(__FILE__)}/../lib")

require 'spawner'

$stdout.sync = true

s = Spawner::Conductor.new("#{File.dirname(__FILE__)}/../etc/config.yml")
1.upto(10) do |i|
  s.add_duty(0) do
    $stderr.puts "+++++++++++++++++++ Job done at #$$, thread #{Thread.current.object_id()}"
    0
  end
end

s.start()

s.join() {|ass, unass| puts "Still #{ass} jobs running, #{unass} jobs in the queue"}
