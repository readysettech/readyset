namespace :guide_validation do
  desc "1 Validations Overview"
  task :step_1 do
    raise "validation failed" unless Person.create(name: "John Doe").valid?
    raise "validation should have failed" if Person.create(name: nil).valid?
  end

  task :step_1_1 do
  end

  task :step_1_2 do
    p = Person.new(name: "John Doe")
    raise "not new" unless p.new_record?
    raise "save failed" unless p.save
    raise "new" if p.new_record?
  end

  task :step_1_3 do
    # There are no code snippets; should we write some ourselves that use the functions mentioned?
  end

  task :step_1_4 do
    p = Person.new
    raise "errors" unless p.errors.size == 0
    raise "should be invalid" if p.valid?
    raise "wrong error" if p.errors.objects.first.full_message != "Name can't be blank"

    p = Person.create
    raise "wrong error" if p.errors.objects.first.full_message != "Name can't be blank"
    raise "save succeeded" if p.save
    begin
      p.save!
    rescue ActiveRecord::RecordInvalid
    end
    begin
      Person.create!
    rescue ActiveRecord::RecordInvalid
    end
  end

  task :step_1_5 do
    raise "errors before validation" if Person.new.errors[:name].any?
    raise "no errors after validation" unless Person.create.errors[:name].any?
  end

  task :step_2_1 do
    p = Person2.new(terms_of_service: "something")
    raise "should be invalid 1" if p.valid?
    p = Person2.new(terms_of_service: "yes")
    raise "should be valid 1" unless p.valid?
    p = Person2.new(terms_of_service: "yes", eula: "something")
    raise "should be invalid 2" if p.valid?
    p = Person2.new(terms_of_service: "yes", eula: "TRUE")
    raise "should be valid 2" unless p.valid?
  end

  task :step_2_2 do
    lib = Library.create(name: "Library")
    raise "should be valid 1" unless lib.valid?
    b = lib.books.create(title: "Good Book")
    raise "should be valid 2" unless b.valid?
    b = lib.books.create(title: "Bad Book")
    raise "should be valid 3" unless b.valid?
    b = Book.create(title: "Fake Book")
    raise "should not be valid" if b.valid?
  end

  task :step_2_3 do
    p = Person3.new(email: "te@s.t")
    raise "should be invalid 1" if p.valid?
    p = Person3.new(email: "te@s.t", email_confirmation: "tte@s.t")
    raise "should be invalid 2" if p.valid?
    p = Person3.new(email: "te@s.t", email_confirmation: "te@s.t")
    raise "should be valid" unless p.valid?
  end

  task :step_2_4 do
    p = Person3.new(email: "inva@l.id", email_confirmation: "inva@l.id")
    raise "should be invalid" if p.valid?
    p = Person3.new(email: "va@l.id", email_confirmation: "va@l.id")
    raise "should be valid" unless p.valid?
  end

  task :step_2_5 do
    p = Person3.new(email: "invalid", email_confirmation: "invalid")
    raise "should be invalid" if p.valid?
    p = Person3.new(email: "valid@ema.il", email_confirmation: "valid@ema.il")
    raise "should be valid" unless p.valid?
  end

  task :step_2_6 do
    p = Person4.new(opt_in: "something")
    raise "should be invalid" if p.valid?
    p = Person4.new(opt_in: "yes")
    raise "should be valid 1" unless p.valid?
    p = Person4.new(opt_in: "no")
    raise "should be valid 2" unless p.valid?
  end

  task :step_2_7 do
    p = Person3.new(email: "a@b.c", email_confirmation: "a@b.c")
    raise "should be too short" if p.valid?
    p = Person3.new(email: "waaaay@too.long", email_confirmation: "waaaay@too.long")
    raise "should be too long" if p.valid?
    p = Person3.new(email: "gold@i.locks", email_confirmation: "gold@i.locks")
    raise "should be just right" unless p.valid?
  end

  task :step_2_8 do
    n = Numbers.new(integer: "a", float: "b")
    raise "should be invalid 1" if n.valid?
    n = Numbers.new(integer: "1", float: "b")
    raise "should be invalid 2" if n.valid?
    n = Numbers.new(integer: "a", float: "1.1")
    raise "should be invalid 3" if n.valid?
    n = Numbers.new(integer: "1.1", float: "1.1")
    raise "should be invalid 4" if n.valid?
    n = Numbers.new(integer: "1", float: "1.1")
    raise "should be valid" unless n.valid?
  end

  task :step_2_9 do
    p = Person3.new(email: "miss@i.ng")
    raise "should be invalid" if p.valid?
    p = Person3.new(email: "pres@en.t", email_confirmation: "pres@en.t")
    raise "should be valid" unless p.valid?
  end

  task :step_2_10 do
    n = Numbers.new(integer: "2", float: "2.2", string: "something")
    raise "should be invalid" if n.valid?
    n = Numbers.new(integer: "2", float: "2.2")
    raise "should be valid" unless n.valid?
  end

  task :step_2_11 do
    p = Person3.new(email: "per@s.on", email_confirmation: "per@s.on")
    raise "should be valid" unless p.valid?
    p.save!
    p = Person3.new(email: "per@s.on", email_confirmation: "per@s.on")
    raise "should be invalid" if p.valid?
  end

  task :step_2_12 do
    p = Person3.new(email: "evil@per.son", email_confirmation: "evil@per.son")
    raise "should be invalid" if p.valid?
    p = Person3.new(email: "good@per.son", email_confirmation: "good@per.son")
    raise "should be valid" unless p.valid?
  end

  task :step_2_13 do
    p = Person3.new(email: "Bad@ema.il", email_confirmation: "Bad@ema.il")
    raise "should be invalid" if p.valid?
    p = Person3.new(email: "good@ema.il", email_confirmation: "good@ema.il")
    raise "should be valid" unless p.valid?
  end


  task :step_3_1 do
    c = Coffee.new(size: "something")
    raise "should be invalid" if c.valid?
    c = Coffee.new(size: "small")
    raise "should be valid 1" unless c.valid?
    c = Coffee.new()
    raise "should be valid 2" unless c.valid?
  end

  task :step_3_2 do
    t = Topic.new(title: "something")
    raise "should be invalid" if t.valid?
    t = Topic.new(title: "thing")
    raise "should be valid 1" unless t.valid?
    t = Topic.new(title: "")
    raise "should be valid 2" unless t.valid?
    t = Topic.new(title: nil)
    raise "should be valid 3" unless t.valid?
    t = Topic.new()
    raise "should be valid 4" unless t.valid?
  end

  task :step_3_3 do
    p = Person5.create(name: "Somebody", age: 30, username: "somebody")
    raise "should be valid" unless p.valid?
    p = Person5.new(age: 30, username: "nobody")
    raise "should be invalid 1" if p.valid?
    p = Person5.new(name: "Nobody", age: "thirty", username: "nobody")
    raise "should ve invalid 2" if p.valid?
    p = Person5.new(name: "Nobody", age: 30, username: "somebody")
    raise "should be invalid 3" if p.valid?
  end

  task :step_3_4 do
    p = Person6.create(email: "test@ema.il", age: 30, name: "Somebody")
    raise "should be valid 1" unless p.valid?
    p = Person6.new(email: "test@ema.il", age: 30, name: "Somebody")
    raise "should be invalid 1" if p.valid?
    p = Person6.create(email: "test2@ema.il", age: 30, name: "Somebody")
    raise "should be valid 2" unless p.valid?
    p.email = "test@ema.il"
    p.save()
    raise "should be valid 3" unless p.valid?
    p = Person6.new(email: "test2@ema.il", age: "thirty", name: "Somebody")
    raise "should be valid 4" unless p.valid?
    p.age = 30
    p.save()
    raise "should be valid 5" unless p.valid?
    p.age = "thirty"
    raise "should be invalid 2" if p.valid?
    p = Person6.new(email: "test3@ema.il", age: 30)
    raise "should be invalid 3" if p.valid?
  end

  task :step_5_1 do
    o = Order.new(payment_type: "cash")
    raise "should be valid 1" unless o.valid?
    o = Order.new(payment_type: "card", card_number: 1234)
    raise "should be valid 2" unless o.valid?
    o = Order.new(payment_type: "card")
    raise "should be invalid 1" if o.valid?
  end

  task :step_5_2 do
    a = Account.new()
    raise "should be valid 1" unless a.valid?
    a = Account.new(password: "something", password_confirmation: "something")
    raise "should be valid 2" unless a.valid?
    # a = Account.new(password: "something")
    # raise "should be invalid 1" if a.valid?
    a = Account.new(password: "something", password_confirmation: "something else")
    raise "should be invalid 2" if a.valid?
  end

  task :step_5_3 do
    u = User4.new(is_admin: false)
    raise "should be valid 1" unless u.valid?
    u = User4.new(is_admin: false, password: "something long")
    raise "should be valid 2" unless u.valid?
    u = User4.new(is_admin: false, email: "te@s.t")
    raise "should be valid 3" unless u.valid?
    u = User4.new(is_admin: false, email: "te@s.t", password: "something long")
    raise "should be valid 4" unless u.valid?
    u = User4.new(is_admin: true, email: "te@s.t", password: "something long")
    raise "should be valid 5" unless u.valid?
    u = User4.new(is_admin: true)
    raise "should be invalid 1" if u.valid?
    u = User4.new(is_admin: true, password: "something long")
    raise "should be invalid 2" if u.valid?
    u = User4.new(is_admin: true, email: "te@s.t")
    raise "should be invalid 3" if u.valid?
  end

  task :step_5_4 do
    c = Computer.new(mouse: "3-button", market: Market.new(retail: true), trackpad: Trackpad.new(present: false))
    raise "should be valid 1" unless c.valid?
    c = Computer.new(mouse: "3-button", market: Market.new(retail: false), trackpad: Trackpad.new(present: true))
    raise "should be valid 2" unless c.valid?
    c = Computer.new(market: Market.new(retail: true), trackpad: Trackpad.new(present: false))
    raise "should be invalid 1" if c.valid?
    c = Computer.new(market: Market.new(retail: false), trackpad: Trackpad.new(present: true))
    raise "should be valid 3" unless c.valid?
    c = Computer.new(market: Market.new(retail: true), trackpad: Trackpad.new(present: true))
    raise "should be valid 4" unless c.valid?
  end

  task :step_6_1 do
    p = Person7.new(email: "test@te.st")
    raise "should be valid" unless p.valid?
    p = Person7.new(email: "test")
    raise "should be invalid 1" if p.valid?
    p = Person7.new()
    raise "should be invalid 2" if p.valid?
  end

  task :step_6_2 do
    i = Invoice.new(customer: Customer.new(name: "somebody", active: true), expiration_date: Date.today, discount: 1, total_value: 10)
    raise "should be valid" unless i.valid?
    i = Invoice.new(customer: Customer.new(name: "somebody", active: false), expiration_date: Date.today, discount: 1, total_value: 10)
    raise "should be invalid 1" if i.valid?
    i = Invoice.new(customer: Customer.new(name: "somebody", active: true), expiration_date: Date.ordinal(2001), discount: 1, total_value: 10)
    raise "should be invalid 2" if i.valid?
    i = Invoice.new(customer: Customer.new(name: "somebody", active: true), expiration_date: Date.today, discount: 10, total_value: 1)
    raise "should be invalid 3" if i.valid?
  end

  task :step_7_1 do
    p = Person8.new
    raise "should be invalid" if p.valid?
    raise "errors wrong" if p.errors.full_messages != ["Name can't be blank", "Name is too short (minimum is 3 characters)"]
    p = Person8.new(name: "John Doe")
    raise "should be valid" unless p.valid?
    raise "errors non-empty" if p.errors.full_messages != []
  end

  task :step_7_2 do
    p = Person8.new(name: "John Doe")
    raise "should be valid" unless p.valid?
    raise "errors non-empty" if p.errors[:name] != []
    p = Person8.new(name: "JD")
    raise "should be invalid 1" if p.valid?
    raise "errors wrong 1" if p.errors[:name] != ["is too short (minimum is 3 characters)"]
    p = Person8.new
    raise "should be invalid 2" if p.valid?
    raise "errors wrong 2" if p.errors[:name] != ["can't be blank", "is too short (minimum is 3 characters)"]
  end

  task :step_7_3 do
    p = Person8.new
    raise "should be invalid" if p.valid?
    raise "errors wrong 1" if (p.errors.where(:name).map do |e| e.message end) != ["can't be blank", "is too short (minimum is 3 characters)"]
    raise "errors wrong 2" if (p.errors.where(:name, :too_short).map do |e| e.message end) != ["is too short (minimum is 3 characters)"]
    e = p.errors.where(:name).last
    raise "attribute wrong" if e.attribute != :name
    raise "type wrong" if e.type != :too_short
    raise "count wrong" if e.options[:count] != 3
    raise "message wrong" if e.message != "is too short (minimum is 3 characters)"
    raise "full_mesage wrong" if e.full_message != "Name is too short (minimum is 3 characters)"
  end

  task :step_7_4 do
    p = Person9.create
    raise "should be invalid" if p.valid?
    raise "type wrong" if p.errors.where(:name).first.type != :too_plain
    raise "full_message wrong" if p.errors.where(:name).first.full_message != "Name is not cool enough"
  end

  task :step_7_5 do
    p = Person10.create
    raise "should be invalid" if p.valid?
    raise "full_message wrong" if p.errors.where(:base).first.full_message != "This person is invalid because ..."
  end

  task :step_7_6 do
    p = Person8.new
    raise "should be invalid" if p.valid?
    raise "should have errors 1" if p.errors.empty?
    p.errors.clear
    raise "should not have errors" unless p.errors.empty?
    raise "save should fail" if p.save
    raise "should have errors 2" if p.errors.empty?
  end

  task :step_7_7 do
    p = Person8.new
    raise "should be invalid" if p.valid?
    raise "should have 2 errors" if p.errors.size != 2
    p = Person8.new(name: "Andrea", email: "andrea@example.com")
    raise "should be valid" unless p.valid?
    raise "should have 0 errors" if p.errors.size != 0
  end
end

