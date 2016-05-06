class SendSpeedLimiter
  SECONDS_PER_PERIOD = 60
  SECONDS_PER_DAY = 86400


  # KEYS = 1-indexed [next_timestamp_key, number_sent_key]
  # ARGV = 1-index [requested_timestamp, number_to_send, per_minute_rate_limit]
  TIME_RATE_LIMIT_LUA_SCRIPT = <<EOF
local next_time_string = redis.call('get', KEYS[1])
local requested_time_to_send = tonumber(ARGV[1])
local next_time_to_send = 0
local number_to_send = tonumber(ARGV[2])
local rate_limit = tonumber(ARGV[3])
if next_time_string then
  next_time_to_send = tonumber(next_time_string)
end
if next_time_to_send < requested_time_to_send then
  redis.call('set', KEYS[1], requested_time_to_send)
  redis.call('set', KEYS[2], number_to_send)
  redis.call('expire', KEYS[1], #{SECONDS_PER_DAY})
  redis.call('expire', KEYS[2], #{SECONDS_PER_DAY})
  return requested_time_to_send
end
local total_sent = redis.call('incrby', KEYS[2], number_to_send)
if total_sent > rate_limit then
  next_time_to_send = next_time_to_send + #{SECONDS_PER_PERIOD}
  redis.call('set', KEYS[1], next_time_to_send)
  redis.call('set', KEYS[2], number_to_send)
end
redis.call('expire', KEYS[1], #{SECONDS_PER_DAY})
redis.call('expire', KEYS[2], #{SECONDS_PER_DAY})
return next_time_to_send
EOF

  def initialize(identifier, rate_limit)
    @identifier = identifier
    @rate_limit = rate_limit
  end

  def time_to_send(num_recipients)
    current_minute = Time.now.utc.change(:sec => 0).to_i
    result = $redis.eval(TIME_RATE_LIMIT_LUA_SCRIPT,
                         [redis_timestamp_key(), redis_sent_count_key()],
                         [current_minute, num_recipients, @rate_limit])
    return Time.at(result.to_i)
  end

  def reset
    $redis.del(redis_timestamp_key)
    $redis.del(redis_sent_count_key)
  end

  private
  def redis_timestamp_key
    return "time-rate-limit:timestamp:#{@identifier}"
  end

  def redis_sent_count_key
    return "time-rate-limit:sent-count:#{@identifier}"
  end
end
