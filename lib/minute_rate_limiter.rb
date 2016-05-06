class MinuteRateLimiter
  def initialize(api_key, per_minute_rate_limit = 5)
    @api_key = api_key
    @per_minute_rate_limit = per_minute_rate_limit
  end

  def should_rate_limit?
    count, _ = $redis.multi do
      $redis.incr(key)
      $redis.expire(key, 60)
    end
    count >= @per_minute_rate_limit
  end

  def remaining
    [@per_minute_rate_limit - $redis.get(key).to_i, 0].max
  end

  private
  def key
    now = Time.now.utc
    time_pattern = now.strftime("%Y-%m-%d-%H-%M")
    "rate_limit:#{@api_key}:#{time_pattern}"
  end
end
