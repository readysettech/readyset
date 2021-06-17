FactoryBot.define do
  factory :user do
    name { "Michael Example" }
    email { "michael@example.com" }
    password_digest { User.digest('password') }
    admin { true }
    activated { true }
    activated_at { Time.zone.now }
  end
end