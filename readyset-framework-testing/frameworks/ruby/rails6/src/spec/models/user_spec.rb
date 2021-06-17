require "rails_helper"

RSpec.describe User do
  before do
    @user = build(:user, name: "Example User", email: "user@example.com", admin: false)
    @michael = build(:user)
    @archer = build(:user, name: "Sterling Archer", email: "duchess@example.gov", admin: false)
    @lana = build(:user, name: "Lana Kane", email: "hands@example.gov", admin: false)
  end

  it "should be valid" do
    expect(@user.valid?).to eq(true)
  end

  it "name should be present" do
    @user.name = "     "
    expect(@user.valid?).to eq(false)
  end

  it "email should be present" do
    @user.email = "     "
    expect(@user.valid?).to eq(false)
  end

  it("name should not be too long") do
    @user.name = ("a" * 51)
    expect(@user.valid?).to eq(false)
  end

  it("email should not be too long") do
    @user.email = (("a" * 244) + "@example.com")
    expect(@user.valid?).to eq(false)
  end

  it("email validation should accept valid addresses") do
    valid_addresses = ["user@example.com", "USER@foo.COM", "A_US-ER@foo.bar.org", "first.last@foo.jp", "alice+bob@baz.cn"]
    valid_addresses.each do |valid_address|
      @user.email = valid_address
      expect(@user.valid?).to eq(true)
    end
  end

  it("email validation should reject invalid addresses") do
    invalid_addresses = ["user@example,com", "user_at_foo.org", "user.name@example.", "foo@bar_baz.com", "foo@bar+baz.com"]
    invalid_addresses.each do |invalid_address|
      @user.email = invalid_address
      expect(@user.valid?).to be false
    end
  end

  it("email addresses should be unique") do
    duplicate_user = @user.dup
    @user.save
    expect(duplicate_user.valid?).to eq(false)
  end

  it("password should be present (nonblank)") do
    @user.password = @user.password_confirmation = (" " * 6)
    expect(@user.valid?).to eq(false)
  end

  it("password should have a minimum length") do
    @user.password = @user.password_confirmation = ("a" * 5)
    expect(@user.valid?).to eq(false)
  end

  it("authenticated? should return false for a user with nil digest") do
    expect(@user.authenticated?(:remember, "")).to eq(false)
  end

  it("associated microposts should be destroyed") do
    @user.save
    @user.microposts.create!(:content => "Lorem ipsum")
    expect { @user.destroy }.to(change { Micropost.count }.by(-1))
  end

  it("should follow and unfollow a user") do
    @michael.follow(@archer)
    expect(@michael.following?(@archer)).to eq(true)
    # expect(archer.followers.include?(michael)).to eq(true)
    @michael.unfollow(@archer)
    expect(@michael.following?(@archer)).to eq(false)
  end

  it("feed should have the right posts") do
    @lana.microposts.each do |post_following|
      expect(@michael.feed.include?(post_following)).to eq(true)
    end
    @michael.microposts.each do |post_self|
      expect(@michael.feed.include?(post_self)).to eq(true)
    end
    @archer.microposts.each do |post_unfollowed|
      assert_not(@michael.feed.include?(post_unfollowed))
    end
  end
end
