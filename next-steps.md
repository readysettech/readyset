### Next steps

- Connect an existing database

  The tutorial above runs ReadySet against a sample database. To see the power of ReadySet against your own database, swap the `upstream-db-url` argument to point at an existing database for testing.

- Connect your application

  Once you have a ReadySet instance up and running, the next step is to connect your application by swapping out your database connection string to point to ReadySet instead. The specifics of how to do this vary by database client library, ORM, and programming language. See [Connect an App](https://docs.readyset.io/guides/connect/existing-app/) for examples.

  **Note:** By default, ReadySet will proxy all queries to the database, so changing your app to connect to ReadySet should not impact performance. You will explicitly tell ReadySet which queries to cache.

- Cache queries

  Once ReadySet is proxying queries, connect a database SQL shell to ReadySet and use the custom [`SHOW PROXIED QUERIES`](https://docs.readyset.io/guides/cache/cache-queries/#check-query-support) SQL command to view the queries that ReadySet has proxied to your upstream database and identify which queries are supported by ReadySet. Then use the custom [`CREATE CACHE`](https://docs.readyset.io/guides/cache/cache-queries/#cache-queries_1) SQL command to cache supported queries.

  **Note:** To successfully cache the results of a query, ReadySet must support the SQL features and syntax in the query. For more details, see [SQL Support](https://docs.readyset.io/reference/sql-support/).

- Tear down

  When you are done testing, stop and remove the Docker resources:

  ```bash
  docker rm -f readyset postgres mysql \
  && docker volume rm readyset
  ```

- Additional deployment options
  In addition to Docker, you can deploy ReadySet [using binaries](https://docs.readyset.io/releases/readyset-core/). For Kubernetes environments, check out how to [deploy ReadySet to Kubernetes](https://docs.readyset.io/guides/deploy/deploy-readyset-kubernetes/).

