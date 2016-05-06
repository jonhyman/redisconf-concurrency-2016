Rails.application.routes.draw do
  root 'application#index'

  match '/rate_limited', :to => 'application#rate_limited', :via => :get
end
