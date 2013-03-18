$LOAD_PATH.unshift("#{File.dirname(__FILE__)}/../lib/")

require 'tempfile'
require 'test/unit'
require 'timeout'
require 'spawner'

module GenericFunctionalTestsMixin
  public
  def generate_spawner(max_concurrents_duties, persistent_workers,
                       spawner_log_file_name = "/dev/null",
                       jobs_log_file_name = "/dev/null")
    s = Spawner::Conductor.new()
    s.load_config_from_hash({:max_concurrents_duties => 3,
                             :parallelism_model => self.class.parallelism_model,
                             :persistent_workers => persistent_workers,
                             :spawner_log_file_name => spawner_log_file_name,
                             :internal_log_file_name => jobs_log_file_name
                            })

    return s
  end

  def generate_empty_tasks_test(nb_tasks, max_concurrents_duties,
                                persistent_workers, perform_now, timeout_seconds)
    Timeout::timeout(timeout_seconds) do
      s = generate_spawner(max_concurrents_duties, persistent_workers)

      nb_tasks.times() do
        s.add_duty(nil) do
        end
      end

      s.join()
    end
  end

  def test_no_task_finishes()
    [0, 10].each() do |concurrent_duties|
      [true, false].each() do |persistent_workers|
        [true, false].each() do |perform_now|
          assert_nothing_thrown do
            generate_empty_tasks_test(0, concurrent_duties, persistent_workers,
                                      perform_now, 2)
          end
        end
      end
    end
  end

  def test_empty_task_finishes()
    [3, 10].each() do |concurrent_duties|
      [true, false].each() do |persistent_workers|
        [true, false].each() do |perform_now|
          assert_nothing_thrown do
            generate_empty_tasks_test(10, concurrent_duties,
                                      persistent_workers, perform_now, 3)
          end
        end
      end
    end
  end
end
