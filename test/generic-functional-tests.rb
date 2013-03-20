$LOAD_PATH.unshift("#{File.dirname(__FILE__)}/../lib/")

require 'tempfile'
require 'test/unit'
require 'timeout'
require 'spawner'

module GenericFunctionalTestsMixin
  public
  def generate_spawner(max_concurrents_duties, persistent_workers,
                       jobs_log_file_name = "/dev/null",
                       spawner_log_file_name = "/dev/null")
    s = Spawner::Conductor.new()
    s.load_config_from_hash({:max_concurrents_duties => max_concurrents_duties,
                             :parallelism_model => self.class.parallelism_model,
                             :persistent_workers => persistent_workers,
                             :spawner_log_file_name => spawner_log_file_name,
                             :jobs_log_file_name => jobs_log_file_name
                            })

    return s
  end

  def generate_tasks_addition(spawner, nb_tasks, perform_now, timeout_seconds, &block)
    Timeout::timeout(timeout_seconds) do
      nb_tasks.times() do
        spawner.add_duty(nil, perform_now, &block)
      end

      spawner.join()
    end
  end

  def setup()
    @spawner_log_file = Tempfile.new('spawner_log')
    @jobs_log_file = Tempfile.new('jobs_log')
  end

  # Test that the spawner works as expected when given no task and asked to join
  def test_no_task()
    [0, 10].each() do |concurrent_duties|
      [true, false].each() do |persistent_workers|
        [true, false].each() do |perform_now|
          assert_nothing_thrown() do
            s = generate_spawner(concurrent_duties, persistent_workers,
                                 @jobs_log_file.path())
            generate_tasks_addition(s, 0, perform_now, 2) {}
          end

          assert_equal(0, @jobs_log_file.size(), "The jobs log is not empty, while it should")
        end
      end
    end
  end

  # Test that if the spawner works as expected when given empty tasks
  def test_empty_tasks()
    [3, 10].each() do |concurrent_duties|
      [true, false].each() do |persistent_workers|
        [true, false].each() do |perform_now|
          assert_nothing_thrown() do
            s = generate_spawner(concurrent_duties, persistent_workers,
                                 @jobs_log_file.path())
            generate_tasks_addition(s, 10, perform_now, 3) {}
          end

          assert_equal(0, @jobs_log_file.size(), "The jobs log is not empty, while it should")
        end
      end
    end
  end

  # Test that the spawner handles return statements in the instructions block
  def test_return_in_job()
    assert_nothing_thrown() do
      s = generate_spawner(1, false)
      generate_tasks_addition(s, 1, true, 2) {return 42}
    end
  end

  def test_failing_job()
    # TODO
  end
end
