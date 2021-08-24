# Sequelize Local Testing

To test sequelize locally, we need to first build the following:

1. A base docker image. In the root directory of the mono-repo run `make
   build-basic`
2. A readyset-server docker image. In the root directory of the mono-repo run `make
   build-server`
3. A readyset-mysql docker image. In the root directory of the mono-repo run `make
   build-adapter`
4. The sequelize framework image. In the framework directory run `./run.sh
   build_image javascript/sequelize`

Now that we have all of that setup we can run the framework test by running the
following command from this directory:
```
docker-compose up
```

## Multiple runs

If you are running the test multiple times, you will want to stop and remove
running docker containers before trying to use `docker-compose up` again.

1. To stop and remove all running containers use the following command:

```sh
$ docker-compose down
```

## Logging

By default, `docker-compose up` will expose logs for all running containers. If
you would like to see only logs of a specific service, you will want to first
run `docker-compose` in a detached mode so no logs are displayed by default. You
can do this by using the command:

```sh
docker-compose up -d
```

Once you have docker-compose running in detached mode, you can inspect logs of a
specific service, and follow those logs with the following command:

```sh
docker-compose logs -f <service>
```

For instance, to see the logs for the adapter, which is named `db` in our
services list we would execute the following command:

```sh
docker-compose logs -f db
```
