# Ruby on Rails Tutorial sample application

This is a fork of the [*Ruby on Rails Tutorial*](http://www.railstutorial.org/) application by Michael Hartl. 
We created this project to help you try RubyMine features.
To get started with the app, follow the [step-by-step tutorial](https://www.jetbrains.com/help/ruby/get-started.html) 
or check out our [YouTube channel](https://www.youtube.com/playlist?list=PLQ176FUIyIUanO72dRf6lOefKIznviKKJ).

## Getting started

To get started with the app, first clone the repo and `cd` into the directory:

```
$ git clone https://github.com/JetBrains/sample_rails_app.git 
$ cd sample_rails_app
```

Then install the needed gems (while skipping any gems needed only in production):

```
$ bundle install --without production
```

Install JavaScript dependencies:

```
$ yarn install
```

Next, migrate the database:

```
$ rails db:migrate
```

Finally, run the test suite to verify that everything is working correctly:

```
$ rails test
```

If the test suite passes, you'll be ready to run the app in a local server:

```
$ rails server
```

## Help page

For general help on the Rails Tutorial, see the [Rails Tutorial Help page](https://www.railstutorial.org/help).

## License

All source code in the [Ruby on Rails Tutorial](https://www.railstutorial.org/)
is available jointly under the MIT License and the Beerware License. See
[LICENSE.md](LICENSE.md) for details.
