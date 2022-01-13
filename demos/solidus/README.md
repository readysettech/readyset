# Solidus Demo App

This directory contains a demo [solidus][] application, along with a
`docker-compose` file to run the application against readyset

[solidus]: https://solidus.io/

## Running the App

After authenticating with ECR (TODO: link to how to do this), run the following
command (in this directory):

``` shellsession
$ docker-compose up -d solidus
```

Once the application starts, you can browse to http://localhost:3000 to open the
Solidus application.
