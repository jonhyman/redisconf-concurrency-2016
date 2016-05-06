require_relative 'semaphore'

class SemaphoreTest
  SEMAPHORE_NAME = 'test_semaphore'
  NUM_RESOURCES = 3
  TIMEOUT = 30

  def self.run(stale_client_timeout = nil)
    threads = []
    5.times do |i|
      thread = Thread.new do
        semaphore = Semaphore.new(SEMAPHORE_NAME, NUM_RESOURCES, TIMEOUT, stale_client_timeout)
        success = false
        until success
          semaphore.lock do |r|
            success = true
            puts("In thread_#{i}, grabbed resource #{r}. Available resources #{$redis.lrange(semaphore.__send__(:available_list_key), 0, -1)}".green)
            sleep(0.25)
          end

          unless success
            puts("  thread_#{i} did not get resource, retrying in 0.1 seconds")
            sleep(0.1)
          end
        end
      end
      threads << thread
    end
    threads.each(&:join)
    nil
  end

  def self.stale_lock_demo
    run
    semaphore = Semaphore.new(SEMAPHORE_NAME, NUM_RESOURCES, TIMEOUT)
    # Monkey patch the release method of this object to do nothing. Simulates a process crashing while the lock
    # is held
    def semaphore.release(x)
    end
    # Lock -- this will not release an object
    semaphore.lock {|r| puts("* Holding #{r}".yellow)}
    sleep(2)
    run(1)
  end
end
