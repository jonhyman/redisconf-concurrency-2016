class Semaphore
  CHECK_STALE_EVERY_X_SECONDS = 10

  # Constructs a semaphore backed by Redis. There are two main keys: a list of available resources and a hash
  # which maps {resource => time_resource_was_taken} so that we can expire stale locks
  #
  # @param name String unique name of the semaphore
  # @param num_resources Integer of how many resources this semaphore has
  # @param expiration Integer in seconds how how the lock should exist for
  # @param stale_client_timeout Integer in seconds of when to expire stale locks
  def initialize(name, num_resources, expiration, stale_client_timeout = nil)
    @name = name
    @expiration = expiration
    @resource_count = num_resources
    @stale_client_timeout = stale_client_timeout
  end

  # @return Integer number of how many resources are available
  def available_count
    exists, length = $redis.pipelined do
      $redis.exists(created_at_key())
      $redis.llen(available_list_key())
    end
    if exists
      return length
    else
      return @resource_count
    end
  end

  # @param timeout Integer of how long to block waiting for the semaphore; if nil, will not block
  # @return false if the lock is not acquired, the return value of the block if the block was executed
  def lock(timeout = nil)
    create_if_not_exists()

    if @stale_client_timeout
      release_stale_locks()
    end

    # If we do not have a timeout, then do a non-blocking pop
    if timeout.nil?
      resource = $redis.rpop(available_list_key())
    else
      resource = $redis.brpop(available_list_key(), timeout)
      # brpop returns [key, popped_value] since you can brpop on multiple keys
      if resource
        resource = resource[1]
      end
    end

    if resource
      begin
        $redis.multi do
          # Store when we got the resource so it can be expired if it is stale
          $redis.hset(taken_hash_key(), resource, Time.now.to_f())
          # Set the expirations on all the keys we use
          $redis.expire(available_list_key(), @expiration)
          $redis.expire(taken_hash_key(), @expiration)
          $redis.expire(created_at_key(), @expiration)
        end

        return yield resource
      ensure
        release(resource)
      end
    end
    return false
  end

  def delete
    $redis.del(available_list_key(), taken_hash_key(), created_at_key())
  end

  private
  # Iterates through the hash of {resource => time_resource_was_taken} to determine if any are stale; if so, then
  # the stale locks are released. This will only release stale locks every CHECK_STALE_EVERY_X_SECONDS seconds
  def release_stale_locks
    created_at = $redis.get(created_at_key()).to_f()

    # Optimization: ensure that we've had enough time pass to even check for staleness in the first place
    if created_at + @stale_client_timeout <= Time.now.to_f()
      mutex_key = "redis:semaphore:#{@name}:stale_mutex"

      # We need to only have one process doing this at a time to avoid a race of multiple processes releasing the
      # same stale lock, so use a setnx
      did_set = $redis.set(mutex_key, true, :nx => true, :ex => CHECK_STALE_EVERY_X_SECONDS)

      if did_set
        taken_pairs = $redis.hgetall(taken_hash_key())
        current_time = Time.now.to_f()
        taken_pairs.each do |token, locked_at|
          timed_out_at = locked_at.to_f() + @stale_client_timeout
          if timed_out_at < current_time
            puts("* Unlocking #{token} because it is stale".red)
            release(token)
          end
        end
      end
    end
  end

  def release(resource)
    $redis.multi do
      $redis.hdel(taken_hash_key(), resource)
      $redis.lpush(available_list_key(), resource)
    end
  end

  def available_list_key
    return @list_key ||= "redis:semaphore:#{@name}:list"
  end

  def taken_hash_key
    return @taken_hash_key ||= "redis:semaphore:#{@name}:taken"
  end

  def created_at_key
    return @created_at_key ||= "redis:semaphore:#{@name}:created"
  end

  # Atomically creates the lock if it does not already exist
  def create_if_not_exists
    args = [
      [available_list_key(), taken_hash_key(), created_at_key()],
      [@resource_count, @expiration, Time.now.to_f()]
    ]
    $redis.eval(create_script(), *args)
  end

  def create_script
    # In order to only create the lock if it doesn't exist, we either need a mutex or need to be atomic, so we can use
    # LUA scripting here
    return @create_script ||= """
      if redis.call('EXISTS', KEYS[3]) == 0 then
        redis.call('DEL', KEYS[1])
        redis.call('DEL', KEYS[2])
        for i=1,ARGV[1] do
          redis.call('LPUSH', KEYS[1], i)
        end
        redis.call('SET', KEYS[3], ARGV[3])
        redis.call('EXPIRE', KEYS[1], ARGV[2])
        redis.call('EXPIRE', KEYS[3], ARGV[2])
      end
      """.strip
  end
end
