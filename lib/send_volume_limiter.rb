class SendVolumeLimiter
  def initialize(identifier, limit)
    @identifier = identifier
    @limit = limit
  end

  def limited_users(users)
    key = "volume:#{@identifier}"
    number_to_dispatch = users.length
    counter = $redis.incrby(key, number_to_dispatch)

    # If the counter is already greater than @limit, we've hit the rate limit
    # so we just need to determine how many (if any) were left for this dispatch attempt
    if counter > @limit
      number_to_dispatch = [@limit - (counter - number_to_dispatch), 0].max
    end

    if number_to_dispatch > 0
      return users[0...number_to_dispatch]
    end

    # Rate limit maxed out, do not dispatch to anyone
    return []
  end

  def reset
    key = "volume:#{@identifier}"
    $redis.del(key)
  end
end
