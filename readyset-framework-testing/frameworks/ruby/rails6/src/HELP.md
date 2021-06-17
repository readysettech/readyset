# Rails Tutorial Help

This is the Help page for the [*Ruby on Rails Tutorial*](https://www.railstutorial.org/) (6th Edition) by [Michael Hartl](http://www.michaelhartl.com/).

## General suggestions

Web development is a tricky business, and despite the best efforts of the
tutorial it’s likely that you’ll run into trouble at some point. If you do, I suggest comparing your code to the [reference implementation of the sample app](https://github.com/mhartl/sample_app_6th_ed) to track down any discrepancies. You can also post your question at [Stack Overflow](https://stackoverflow.com/), but I suggest you do so after trying all the debugging tips listed below.

## Debugging tips

While it’s impossible to anticipate every potential problem, here are
some debugging tips that might help:

- Have you compared your code to the [reference implementation of the sample app](https://github.com/mhartl/sample_app_6th_ed)?
- Are you using the [exact gem versions](https://gemfiles-6th-ed.railstutorial.org) (including Rails) used in the
  tutorial?
- Did you try Googling the error message?
- Did you stop the Rails web server (with Ctrl-C) and restart?
- Did you try stopping Spring using `bin/spring stop`?
- Did you copy-and-paste from the book’s code? (Experience shows that typing in code, while a better learning technique in general, is error-prone, so when in doubt be sure to copy all code exactly.)
- Did you try Googling the error message?
- Did you re-run `bundle install`?
- Did you try running `bundle update`?
- Did you examine the Heroku logs using `heroku logs` or `heroku logs --tail`?
- Did you make sure the `sqlite3` gem is listed only in the `development` and `test` environments in the `Gemfile`?
- Did you look for the answer at Stack Overflow?
- Did you try Googling the error message?

If your problem is of a general nature, such as having issues installing
Rails or configuring your system, I suggest posting to [Stack
Overflow](http://stackoverflow.com/). This will allow other
people running into your issue (and not just those following the Rails
Tutorial) to benefit from the discussion. For issues
deploying to [Heroku](http://heroku.com/), please contact [Heroku
technical support](http://support.heroku.com/).

When asking your question on any mailing list or forum, be sure to
include as much relevant information as possible. To maximize your chances
of a helpful reply, I especially recommend the article [How To Ask Questions The Smart
Way](http://www.catb.org/esr/faqs/smart-questions.html) by [Eric
Raymond](http://www.catb.org/esr/).

## Error reports

Suspected errors, typos, and bugs can be emailed to <admin@railstutorial.org>. All such reports are gratefully received, but please double-check with the [online version of the tutorial](https://www.railstutorial.org/book) and the [reference implementation](https://github.com/mhartl/sample_app_6th_ed) before submitting.

