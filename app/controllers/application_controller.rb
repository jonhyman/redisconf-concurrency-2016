class ApplicationController < ActionController::Base
  def index
    render :json => {}
  end

  def rate_limited
    limiter = MinuteRateLimiter.new(params[:api_key], 5)
    if limiter.should_rate_limit?
      render :text => "Over rate limit", :status => 429
    else
      render :json => {:requests_remaining => limiter.remaining}
    end
  end
end
