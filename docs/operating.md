Operating
=========

Metrics
-------

Metrics are exposed via [Prometheus](https://prometheus.io/)-compatible API at `/metrics`.

Full list of metrics at [writer/metrics.go](../writer/metrics.go)

You can debug those metrics from command line by running:

```
$ curl -s http://localhost:9094/metrics | egrep -v '(#|go_|process_)'
```

The metrics are best viewed in Grafana or Prometheus:

![](operating-graphs.png)
