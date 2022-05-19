# Introduction

Welcome to ReadySet!

Here at `docs/` you can find:

* A guide for setting up your development environment and making your first code
  changes as a new engineer at ReadySet
* High-level documentation on our engineering process and workflow
* High-level documentation on how ReadySet works and the broad components of the
  system

As with all internal documentation, this page is a constant work in progress -
and if you are a new engineer at ReadySet, you are the best possible person for
evaluating what's written here - and making improvements! The source for this
documentation is located in the `docs/` directory in the root of the repository,
and is built by [mdbook][] from each commit to `main`. As you're going through
this documentation, you're strongly encouraged to make changes and submit CLs,
either to add extra information or to clarify and improve existing
documentation. <!-- TODO: link to "how to make a CL" docs here -->

The best place to start is, unsurprisingly, the [getting
started](./getting_started.md) page.

[mdbook]: https://rust-lang.github.io/mdBook/

## Other documentation

There are a couple of other places where documentation lives at ReadySet:

* [Rustdoc][rs-rustdoc], generated automatically from [doc-comments in the
  code][rustdoc] on every commit to main. These contain both API docs for
  structs, functions, etc., and also lower-level docs on individual modules in
  the system.
* [Our shared google drive][gdrive], which we use primarily because the
  live-collaboration features are unmatched everywhere else. This contains,
  among other things, design docs for larger changes we've made over the course
  of ReadySet's existence. Generally, the rule here is that because Google Drive
  is not versioned alongside our codebase, anything in there should be
  considered out of date by default, but can be useful to read if you're curious
  about the reasoning behind any previous decisions made.

In addition, a few other things on the internet are worth reading:

* [Jon Gjengset's PhD thesis][phd-thesis] has some details that're out of date,
  but is still by far the best conceptual overview of the concepts behind Noria,
  and the high-level structure of how all the pieces fit together.
* [The Rust Programming Language book][trpl] is a fantastic resource to learn
  Rust for anyone who's new to the language
* [Rust for Rustaceans][r4r] is great for "leveling up" rust knowledge for
  people who're familiar with the language

[rs-rustdoc]: http://docs/rustdoc/noria_server/index.html
[rustdoc]: https://doc.rust-lang.org/rustdoc/what-is-rustdoc.html
[gdrive]: https://drive.google.com/drive/u/0/folders/0APY-8O86YOpQUk9PVA
[phd-thesis]: https://jon.thesquareplanet.com/papers/phd-thesis.pdf
[trpl]: https://doc.rust-lang.org/book/
[r4r]: https://nostarch.com/rust-rustaceans
