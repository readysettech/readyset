# ReadySet Adapter Endpoints

The following HTTP endpoints are exposed by the ReadySet Adapter.

## Health Check

Get the health of the adapter. Return 200 code without a response body if the service is considered healthy or return no
response at all if the service is unhealthy.

"Healthy" _only_ indicates that the HTTP router is active but no further checks are performed.

* **URL**

  `/health`

* **Method:**

  `GET`

* **Success Response:**

    * **Code:** 200 <br />

* **Sample Call:**

  `curl -X GET <adapter>:<adapter-port>/health`

## Allow List

List of SQL queries that will be handled by ReadySet as opposed to being passed through to the underlying database.

* **URL**

  `/allow-list`

* **Method:**

  `GET`

* **Success Response:**

  Allow list as a JSON Object.

    * **Code:** 200 <br />
      **Content:** ```["SELECT `id` FROM `t1` WHERE (`id` = '<anonymized>')"]```

* **Error Response:**

    * **Code:** 500 Internal Server Error <br />
      **Content:** `"allow list failed to be converted into a json string"`

* **Sample Call:**

  `curl -X GET <adapter>:<adapter-port>/allow-list`

## Deny List

List of SQL queries that will _not_ be handled by ReadySet and instead passed through to the underlying database.

* **URL**

  `/deny-list`

* **Method:**

  `GET`

* **Success Response:**

  Allow list as a JSON Object.

    * **Code:** 200 <br />
      **Content:** ```["SELECT * FROM `t1`"]```

* **Error Response:**

    * **Code:** 500 Internal Server Error <br />
      **Content:** `"deny list failed to be converted into a json string"`

* **Sample Call:**

  `curl -X GET <adapter>:<adapter-port>/deny-list`

## Prometheus

Endpoint for Prometheus metric API calls.

* **URL**

  `/prometheus`

* **Method:**

  `GET`

* **Success Response:**

  * **Code:** 200 <br />
    **Content:**

    ```
    #TYPE noria_client_external_requests counter
    noria_client_external_requests{upstream_db_type="mysql",deployment="blej"} 1

    #TYPE startup_timestamp counter
    startup_timestamp{upstream_db_type="mysql",deployment="blej"} 1649290515172
    ```

* **Error Response:**

  Returns 404 if adapter is run without `--prometheus-metrics` or if the Prometheus exporter runs into any other type of
  error.

  * **Code:** 404 Not Found <br />
    **Content:** `"Prometheus metrics were not enabled. To fix this, run the adapter with --prometheus-metrics"`

  OR

  * **Code:** 404 Not Found <br />

* **Sample Call:**

  `curl -X GET <adapter>:<adapter-port>/prometheus`

* **Notes:**

  This endpoint is intended to be scraped by Prometheus. For almost all cases you want to query Prometheus directly to
  get metrics data.