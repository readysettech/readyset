require 'humanize'

namespace :guide_association do
  task :step_1 do
    @author = Author.create(name: "Somebody")
    @book = @author.book2s.create(title: "Epic Novel")
    @author.destroy
    # TODO:  Issue a bare SQL query to ensure that Somebody and Epic Novel are both gone from the database
  end

  task :step_2_1 do
    # Yes this is functionally the same as :step_1
    author = Author.create(name: "Somebody2")
    book = author.book2s.create(title: "Epic Novel 2")
    author.destroy
    # TODO:  Issue a bare SQL query to ensure that Somebody and Epic Novel are both gone from the database
  end

  task :step_2_2 do
    supplier = Supplier.create!(name: "Products Inc")
    account = Account2.create!(supplier: supplier, account_number: "1234")
    supplier.save!
  end

  task :step_2_3 do
    # Yes this is functionally the same as :step_1 and :step_2_1
    author = Author.create!(name: "Somebody3")
    book = author.book2s.create!(title: "Epic Novel 3")
    author.destroy
    # TODO:  Issue a bare SQL query to ensure that Somebody and Epic Novel are both gone from the database
  end

  task :step_2_4 do
    doc = Physician.create!(name: "Dr Mario")
    patient = Patient.create!(name: "Me")
    appointment = Appointment.create!(physician: doc, patient: patient, appointment_date: Date.today)
    patient2 = Patient.create!(name: "You")
    appointment2 = Appointment.create!(physician: doc, patient: patient2, appointment_date: Date.today)
    raise "wrong patients" unless doc.patients == [patient, patient2]
  end

  task :step_2_5 do
    supplier = Supplier.create!(name: "Products Inc")
    account = Account2.create!(supplier: supplier, account_number: "1234")
    history = AccountHistory.create!(account2: account, credit_rating: 1000)
    supplier.save!
  end

  task :step_2_6 do
    p1 = Part.create!(part_number: "1")
    p2 = Part.create!(part_number: "2")
    p3 = Part.create!(part_number: "3")
    a1 = Assembly.create!(name: "a")
    a2 = Assembly.create!(name: "b")
    a1.parts = [p1, p2, p3]
    a1.save!
    p2.assemblies += [a2]
    p2.save!
    a2.parts += [p3]
    a2.save!
    raise "p1 assemblies wrong" unless p1.assemblies == [a1]
    raise "p2 assemblies wrong" unless p2.assemblies == [a1, a2]
    raise "p3 assemblies wrong" unless p3.assemblies == [a1, a2]
    raise "a1 parts wrong" unless a1.parts == [p1, p2, p3]
    raise "a2 parts wrong" unless a2.parts == [p2, p3]
  end

  task :step_2_7 do
    supplier = Supplier2.create!(name: "Widgetcorp")
    account = Account3.create!(supplier2: supplier)
  end

  task :step_2_8 do
    p1 = Part2.create!()
    p2 = Part2.create!()
    p3 = Part2.create!()
    a1 = Assembly2.create!()
    a2 = Assembly2.create!()
    a1.part2s = [p1, p2, p3]
    a1.save!
    p2.assembly2s += [a2]
    p2.save!
    a2.part2s += [p3]
    a2.save!
    raise "p1 assemblies wrong" unless p1.assembly2s == [a1]
    raise "p2 assemblies wrong" unless p2.assembly2s == [a1, a2]
    raise "p3 assemblies wrong" unless p3.assembly2s == [a1, a2]
    raise "a1 parts wrong" unless a1.part2s == [p1, p2, p3]
    raise "a2 parts wrong" unless a2.part2s == [p2, p3]

    p1 = Part3.create!()
    p2 = Part3.create!()
    p3 = Part3.create!()
    a1 = Assembly3.create!()
    a2 = Assembly3.create!()
    a1.part3s = [p1, p2, p3]
    a1.save!
    p2.assembly3s += [a2]
    p2.save!
    a2.part3s += [p3]
    a2.save!
    raise "p1 assemblies wrong" unless p1.assembly3s == [a1]
    raise "p2 assemblies wrong" unless p2.assembly3s == [a1, a2]
    raise "p3 assemblies wrong" unless p3.assembly3s == [a1, a2]
    raise "a1 parts wrong" unless a1.part3s == [p1, p2, p3]
    raise "a2 parts wrong" unless a2.part3s == [p2, p3]
  end

  task :step_2_9 do
    employee1 = Employee.create!(name: "Me")
    employee2 = Employee.create!(name: "You")
    product = Product.create!(name: "Thing")
    pic1 = Picture.create!(name: "My Photo", imageable: employee1)
    pic2 = Picture.create!(name: "Your Photo", imageable: employee2)
    pic3 = Picture.create!(name: "Product Photo", imageable: product)
  end

  task :step_2_10 do
    e1 = Employee2.create!(name: "CEO")
    e2 = Employee2.create!(name: "CTO", manager: e1)
    e3 = Employee2.create!(name: "VP Dev", manager: e2)
    e4 = Employee2.create!(name: "VP Product", manager: e2)
    e5 = Employee2.create!(name: "Manager Dev", manager: e4)
    e6 = Employee2.create!(name: "Dev 1", manager: e5)
    e7 = Employee2.create!(name: "Dev 2", manager: e5)
    raise "e2 subordinates wrong" unless e2.subordinates == [e3,e4]
    raise "e5 subordinates wrong" unless e5.subordinates == [e6,e7]
    raise "e4 subordinates wrong" unless e4.subordinates == [e5]
    raise "e4 manager wrong" unless e4.manager == e2
    raise "e7 manager wrong" unless e7.manager == e5

    # run the same tests as above, but this time the foreign key constraint is not enforced
    e1 = Employee3.create!(name: "CEO")
    e2 = Employee3.create!(name: "CTO", manager: e1)
    e3 = Employee3.create!(name: "VP Dev", manager: e2)
    e4 = Employee3.create!(name: "VP Product", manager: e2)
    e5 = Employee3.create!(name: "Manager Dev", manager: e4)
    e6 = Employee3.create!(name: "Dev 1", manager: e5)
    e7 = Employee3.create!(name: "Dev 2", manager: e5)
    # sort added because the order can be arbitrary in sharded keyspace
    raise "e2 subordinates wrong" unless e2.subordinates.sort == [e3,e4].sort
    raise "e5 subordinates wrong" unless e5.subordinates.sort == [e6,e7].sort
    raise "e4 subordinates wrong" unless e4.subordinates == [e5]
    raise "e4 manager wrong" unless e4.manager == e2
    raise "e7 manager wrong" unless e7.manager == e5
  end

  task :step_3_1 do
    author = Author.create!(name: "Great Writer")
    book1 = Book2.create!(author: author, title: "Series 1", published_at: Date.today)
    book2 = Book2.create!(author: author, title: "Series 2", published_at: Date.today)
    raise "wrong size" if author.book2s.size != 2
    st = ActiveRecord::Base.connection.raw_connection.prepare("DELETE FROM book2s WHERE author_id = ?")
    st.execute(author.id)
    st.close
    raise "not empty" unless author.book2s.reload.empty?
  end

  task :step_3_3 do
    # 3.3.1 is migration only and doesn't highlight any features that we haven't already tested ad infinitum
    # 3.3.2, however, shows create_join_table, so we want to show that working
    p1 = Part4.create!(part_number: "1")
    p2 = Part4.create!(part_number: "2")
    p3 = Part4.create!(part_number: "3")
    a1 = Assembly4.create!(name: "a")
    a2 = Assembly4.create!(name: "b")
    a1.parts = [p1, p2, p3]
    a1.save!
    p2.assemblies += [a2]
    p2.save!
    a2.parts += [p3]
    a2.save!
    raise "p1 assemblies wrong" unless p1.assemblies == [a1]
    raise "p2 assemblies wrong" unless p2.assemblies == [a1, a2]
    raise "p3 assemblies wrong" unless p3.assemblies == [a1, a2]
    raise "a1 parts wrong" unless a1.parts == [p1, p2, p3]
    raise "a2 parts wrong" unless a2.parts == [p2, p3]
  end

  task :step_3_5 do
    a = Author2.create!(first_name: "Great")
    b = Book3.create!(title: "Mystery", writer: a)
    b = a.book3s.first
    raise "name mismatch 1" unless a.first_name == b.writer.first_name
    a.first_name = "Awesome"
    raise "name mismatch 2" unless a.first_name == b.writer.first_name
  end

  task :step_4_1_1 do
    author1 = Author.create!(name: "Novelista")
    author2 = Author.create!(name: "Other Novelista")
    book = Book2.create!(author: author1, title: "Origin Story", published_at: Date.today)
    raise "author wrong 1" unless book.author.name == "Novelista"
    st = ActiveRecord::Base.connection.raw_connection.prepare("UPDATE authors SET name = ? WHERE id = ?")
    st.execute("Ghostwriter", author1.id)
    st.close
    raise "author wrong 2" unless book.author.name == "Novelista"
    book.reload_author
    raise "author wrong 3" unless book.author.name == "Ghostwriter"
    book.author = author2
    raise "author wrong 4" unless book.author.name == "Other Novelista"
    author3 = book.build_author(name: "Journalist")
    author3.save!
    raise "author wrong 5" unless book.author.name == "Journalist"
    author4 = book.create_author(name: "Online Journalist")
    raise "author wrong 6" unless book.author.name == "Online Journalist"
    book.create_author!
    raise "author wrong 7" unless book.author.name == nil
  end

  task :step_4_1_2 do
    author = Author3.create!(name: "Somebody")
    book1 = Book4.create!(author: author, title: "Something")
    raise "count wrong 1" unless author.book4s_count == 1
    raise "book wrong 1" unless author.book4s[0].title == "Something"
    book2 = Book4.create!(title: "Another Thing")
    book2.author = author
    book2.save!
    raise "count wrong 2" unless author.book4s_count == 2
    raise "book wrong 2" unless author.book4s[1].title == "Another Thing"
  end

  task :step_4_1_3 do
    author = Author4.create!(name: "G.A. Uthor")
    book = Book5.create!(author: author, title: "Book 1")
    chapter = Chapter.create!(book: book, number: 1, name: "Prologue")
    raise "author wrong" unless chapter.book.author.name == "G.A. Uthor"
    raise "all wrong" unless Book5.all == [book]
  end

  task :step_4_1_4 do
    book = Book4.create!(title: "Book 2")
    raise "author not nil" unless book.author.nil?
  end

  task :step_4_2_1 do
    supplier1 = Supplier3.create!(name: "SprocketCorp")
    supplier2 = Supplier3.create!(name: "SpringCorp")
    account = Account4.create!(supplier: supplier1, account_number: "1234")
    raise "supplier wrong 1" unless account.supplier.name == "SprocketCorp"
    st = ActiveRecord::Base.connection.raw_connection.prepare("UPDATE supplier3s SET name = ? WHERE guid = ?")
    st.execute("GearCorp", supplier1.id)
    st.close
    raise "supplier wrong 2" unless account.supplier.name == "SprocketCorp"
    account.reload_supplier
    raise "supplier wrong 3" unless account.supplier.name == "GearCorp"
    account.supplier = supplier2
    raise "supplier wrong 4" unless account.supplier.name == "SpringCorp"
    supplier3 = account.build_supplier(name: "PulleyCorp")
    supplier3.save!
    raise "supplier wrong 5" unless account.supplier.name == "PulleyCorp"
    supplier4 = account.create_supplier(name: "BearingCorp")
    raise "supplier wrong 6" unless account.supplier.name == "BearingCorp"
    account.create_supplier!
    raise "supplier wrong 7" unless account.supplier.name == nil
  end

  task :step_4_2_2 do
    supplier = Supplier3.create!(name: "Water LLC")
    account = Account4.create!(supplier: supplier, account_number: "5678")
    supplier.destroy!
    account.reload
    raise "supplier should be null" unless account.supplier == nil
    account.create_supplier!(name: "Distilled Water LLC")
    raise "supplier wrong" unless account.supplier.name == "Distilled Water LLC"
  end

  task :step_4_2_3 do
    supplier = Supplier4.create!(name: "Energy GmbH")
    account = Account5.create!(supplier: supplier, account_number: "67890")
    tx = Transaction.create!(account: account, value: 10, description: "1000 kWh")
    raise "supplier wrong" unless tx.account.supplier.name == "Energy GmbH"
    raise "all wrong" unless Account5.all == [account]
  end

  task :step_4_2_4 do
    account = Account4.create!(account_number: "0")
    raise "supplier not nil" unless account.supplier.nil?
  end

  task :step_4_3_1 do
    author = Author4.create!(name: "Wordsmith")
    raise "books wrong 1" unless author.books == []
    book1 = Book5.create!(author: author, title: "Words, Volume 1")
    raise "book count wrong" unless author.num_books == 1
    raise "books wrong 2" unless author.books == [book1]
    book2 = Book5.create!(title: "Words, Volume 2")
    author.books << book2
    raise "books wrong 3" unless author.books == [book1, book2]
    author.books.delete(book1)
    raise "books wrong 4" unless author.books == [book2]
    raise "author wrong 1" unless book1.author.name == author.name
    book1.reload
    raise "author wrong 2" unless book1.author.nil?
    author.books.destroy(book2)
    raise "books wrong 5" unless author.books == []
    author.books = [book1]
    author.save!
    raise "author wrong 3" unless book1.author.name == author.name
    author.reload
    raise "books wrong 6" unless author.books == [book1]
    raise "book ids wrong" unless author.book_ids == [book1.id]
    author.books.clear
    raise "books wrong 7" unless author.books.empty?
    book1.reload
    raise "author wrong 4" unless book1.author.nil?
    raise "book count wrong" unless author.books.size == 0
    book1.author = author
    raise "books wrong 8" unless author.books == [book1]
    raise "book find wrong" unless author.books.find(book1.id).title == book1.title
    author.save!
    book1.save!
    book3 = Book5.create!(title: "Words, Volume 1")
    raise "book where wrong" unless author.books.where(title: "Words, Volume 1").size == 1
    raise "book missing" unless author.books.exists?(title: "Words, Volume 1")
    raise "book shouldn't exist" if author.books.exists?(title: "Words, Volume 2")
    book3.title = "Words, Volume 3"
    book3.author = author
    book3.save!
    author.books.reload
    raise "books wrong 9" unless author.books == [book1, book3]
  end

  task :step_4_3_3 do
    author = Author4.create!(name: 'Cool Guy')
    for i in 1..10
      book = author.books.create!(title: "Chapters #{i}", confirmed: i % 2)
      for j in 1..12
        book.chapters.create!(number: j, name: j.humanize)
      end
    end
    raise "confirmed book count wrong" unless author.confirmed_books.size == 5
    raise "grouped chapter count wrong" unless author.grouped_chapters.size == 10
    raise "chapter count wrong" unless author.chapters.size == 100 # not 120 because we have a limit 100 in author4.rb
    raise "first chapter wrong" unless author.chapters.first.number == 2
    raise "missing chapter numbers" unless author.chapter_numbers.size == 120
  end

  task :step_4_4_1 do
    pup1 = Pupper.create!(name: "Sadie")
    pup2 = Pupper.create!(name: "Max")
    pup3 = Pupper.create!(name: "Spot")
    pup1.friends << pup2
    pup3.friends << pup2
    raise "friend count wrong 1" unless pup2.friends.size == 0 # self-referential joins aren't two-way, need to do a more complex setup for that
    pup4 = pup2.friends.create!(name: "Daisy")
    pup5 = pup2.friends.create!(name: "Fido")
    raise "friend count wrong 2" unless pup2.friends.size == 2
    pup2.friends.delete(pup4)
    pup4.reload
    pup2.friends.destroy(pup5)
    pup5.reload
    raise "friend count wrong 3" unless pup2.friends.size == 0
    pup3.friends = [pup1, pup4]
    pup3.reload
    raise "friends wrong 1" unless pup3.friends == [pup1, pup4]
    raise "friend ids wrong" unless pup3.friend_ids == [pup1.id, pup4.id]
    pup3.friends.clear
    raise "friends not empty" unless pup3.friends.empty?
    pup1.friends = [pup2, pup3, pup4, pup5]
    pup1.reload
    raise "friend count wrong 4" unless pup1.friends.size == 4
    raise "friend wrong 1" unless pup1.friends.find(pup3.id) == pup3
    raise "friend wrong 2" unless pup1.friends.where(name: "Max").first == pup2
    raise "friend should exist" unless pup1.friends.exists?(name: "Daisy")
    pup6 = pup2.friends.create!(name: "Rover")
    raise "friend should not exist" if pup1.friends.exists?(name: "Rover")
    pup7 = pup1.friends.build(name: "Bailey")
    raise "pupper should not exist" unless Pupper.where(name: "Bailey").to_a.size == 0
  end

  task :step_4_4_3 do
    m1 = Manufacturer.create!(name: "Sprocket Builder")
    sp1 = m1.parts.create!(kind: "sprocket", sku: "sprocket-50t")
    sp2 = m1.parts.create!(kind: "sprocket", sku: "sprocket-34t")
    m2 = Manufacturer.create!(name: "Chain Maker")
    c1 = m2.parts.create!(kind: "chain", sku: "chain-104l-titanium")
    c2 = m2.parts.create!(kind: "chain", sku: "chain-104l-dlc")
    m3 = Manufacturer.create!(name: "Jack of All Trades")
    sp3 = m3.parts.create!(kind: "sprocket", sku: "sprocket-25t")
    c3 = m3.parts.create!(kind: "chain", sku: "chain-104l-steel")
    a1 = Assembly5.create!(factory: "Seattle", name: "3:2 reduction, titanium", parts: [sp1, sp2, c1])
    a2 = Assembly5.create!(factory: "Seattle", name: "3:2 reduction, DLC", parts: [sp1, sp2, c2])
    a3 = Assembly5.create!(factory: "Seattle", name: "2:1 reduction, cheap", parts: [sp1, sp3, c3])
    a4 = Assembly5.create!(factory: "New York", name: "2:1 reduction, DLC", parts: [sp1, sp3, c2])
    raise "assembly manufacturer count wrong 1" unless a1.manufacturers.size == 2
    raise "assembly manufacturers wrong" unless a1.manufacturers == [m1, m2]
    for i in 1..10 do
      mfr = Manufacturer.create!(name: "Imitation Sprocket Builder #{i}")
      sp1.manufacturers << mfr
      sp2.manufacturers << mfr
      sp3.manufacturers << mfr
    end
    raise "limited manufacturer count wrong" unless sp1.manufacturers.size == 5
    sp1.manufacturers.reload
    raise "limited manufacturers wrong" unless sp1.manufacturers == [
      Manufacturer.find(11),
      Manufacturer.find(10),
      Manufacturer.find(9),
      Manufacturer.find(8),
      Manufacturer.find(7)
    ]
  end

  task :step_5 do
    c = Car.create!(color: 'Red', price: 10000)
    b = Bicycle.create!(color: 'Silver', price: 100)
    m = Motorcycle.create!(color: 'Black', price: 5000)
    raise "vehicles wrong" unless Vehicle.all == [c, b, m]
  end
end

