class FrequencyLock
  def initialize(name, duration)
    @name = name
    @duration = duration
  end

  def set_lock
    return $redis.set("#{@name}-set_lock", true, :nx => true, :ex => @duration)
  end

  def getset_lock
    key = "#{@name}-getset_lock"
    value = $redis.get(key)
    acquired = false
    if value.nil? || value.to_i() < Time.now.to_i() - @duration
      value = $redis.getset(key, Time.now.to_i())
      if value.nil? || value.to_i() < Time.now.to_i() - @duration
        acquired = true
      end
    end
    return acquired
  end
end
