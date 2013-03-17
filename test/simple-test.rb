$LOAD_PATH.unshift("#{File.dirname(__FILE__)}/../lib")

require 'spawner'

$stdout.sync = true

s = Spawner::Conductor.new()
s.load_config_from_hash({
                          :max_concurrents_duties => 3,
                          :parallelism_model => 'thread',
                          :persistent_workers => true,
                          :spawner_log_file_name => '/tmp/jobs_log',
                          :internal_log_file_name => '/tmp/internal_log'
                        })


1.upto(10) do |i|
  s.add_duty(0) do
    $stderr.puts "+++++++++++++++++++ Job done at #$$, thread #{Thread.current.object_id()}"
    0
  end
end

s.start()

s.join() {|ass, unass| puts "Still #{ass} jobs running, #{unass} jobs in the queue"}
