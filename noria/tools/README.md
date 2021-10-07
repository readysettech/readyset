This directory contains several tools used to query noria deployments
and perform very simple tests.

`controller_request`: Issues a set of basic controller requests to the
current leader.

`metrics_dump`: Prints out a current dump of the leader instances metrics.

`query_installer`: Installs basic DDL, a table, and a SELECT * view.

`view_checker`: Issues a view query to a specific view.


Many of these tools take in an authority, authority-address, and deployment
as parameters. Below is an example of how to pass these parameters:
`./controller_request --authority consul --authority-address 127.0.0.1:8500 --deployment noria --endpoint /healthy_workers`
