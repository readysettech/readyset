# WordPress local testing

This is a modified copy of the [*WordPress 6.0
README*](https://core.trac.wordpress.org/browser/branches/6.0/README.md) with
additional instructions on inserting ReadySet between WordPress and a MySQL
database.

WordPress is available under the [*GPLv2
License*](https://wordpress.org/about/license/).

## Getting Started

WordPress is a PHP, MySQL, and JavaScript based project, and uses Node for its
JavaScript dependencies. A local development environment is available to
quickly get up and running.

You will need Node and npm installed on your computer. Node is a JavaScript
runtime used for developer tooling, and npm is the package manager included
with Node. If you have a package manager installed for your operating system,
setup can be as straightforward as:

* macOS: `brew install node`
* Windows: `choco install nodejs`
* Ubuntu: `apt install nodejs npm`

If you are not using a package manager, see the [Node.js download
page](https://nodejs.org/en/download/) for installers and binaries.

You will also need [Docker](https://www.docker.com/products/docker-desktop)
installed and running on your computer. Docker is the virtualization software
that powers the local development environment. Docker can be installed by
following the instructions in the official [Docker
docs](https://docs.docker.com/engine/install).

### Justfile

The commands described in this document can also be run by using
[`just`](https://github.com/casey/just) if desired. To install just if it is
not on your system and see which commands are available, run:

```
cargo install just
just -l
```

If building everything from scratch, `just` will build all the targets.


#### Clone the WordPress repository: `just clone`

```
git clone git://develop.git.wordpress.org/ wordpress_develop
```

#### Copy over modified files: `just modify`

The `docker-compose.yml` file in this directory will set up a container
configuration that will point WordPress to ReadySet. The `npm` commands below
will use the docker-compose file for determining what containers to launch or
modify.

```
cp ./docker-compose.wordpress.yml ./wordpress_develop/docker-compose.wordpress.yml
```

We make a couple of modifications to the install.js script to point WordPress to ReadySet and workaround an error with removing an old database with the same name. Copy it over as well.
```
cp ./install.js ./wordpress_develop/tools/local-env/scripts/install.js
```

### Development Environment Commands

Ensure [Docker](https://www.docker.com/products/docker-desktop) is running
before using these commands.

#### To start the development environment for the first time `just build`, `just start`

Clone the current repository using `git clone
https://github.com/WordPress/wordpress-develop.git`. Then in your terminal move
to the repository folder `cd wordpress-develop` and run the following commands:

```
npm install
npm run build:dev
npm run env:start
npm run env:install
```

Your WordPress site will accessible at http://localhost:8889. You can see or
change configurations in the `.env` file located at the root of the project
directory.

#### To run the tests `just test_php`, `just test_e2e`

These commands run the PHP and end-to-end test suites, respectively:

```
npm run test:php
npm run test:e2e
```

#### To restart the development environment

You may want to restart the environment if you've made changes to the
configuration in the `docker-compose.yml` or `.env` files. Restart the
environment with:

```
npm run env:restart
```

#### To stop the development environment

You can stop the environment when you're not using it to preserve your
computer's power and resources:

```
npm run env:stop
```

#### To start the development environment again

Starting the environment again is a single command:

```
npm run env:start
```

## Credentials

These are the default environment credentials:

* Database Name: `wordpress_develop`
* Username: `root`
* Password: `password`

To login to the site, navigate to http://localhost:8889/wp-admin.

* Username: `admin`
* Password: `password`

To generate a new password (recommended):

1. Go to the Dashboard
2. Click the Users menu on the left
3. Click the Edit link below the admin user
4. Scroll down and click 'Generate password'. Either use this password
   (recommended) or change it, then click 'Update User'. If you use the
   generated password be sure to save it somewhere (password manager, etc).

