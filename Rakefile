require 'rake/testtask'

desc "Test the spawner with threads"
Rake::TestTask.new(:test_threads) do |t|
  t.libs << "test"
  t.test_files = FileList['test/threaded-functional-tests.rb']
end

desc "Test the spawner with processes"
Rake::TestTask.new(:test_processes) do |t|
  t.libs << "test"
  t.test_files = FileList['test/processes-functional-tests.rb']
end

task :test => [:test_threads, :test_processes] do
  # Nothing
end
