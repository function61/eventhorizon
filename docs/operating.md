Operating
=========

Metrics
-------

Metrics are exposed via [Prometheus](https://prometheus.io/)-compatible api at `/metrics`.

You can debug those metrics from command line by running:

```
$ curl -s http://localhost:9092/metrics | egrep -v '(#|go_|process_)'
```

TODO: embed sample graphs here from Grafana & Prometheus.
