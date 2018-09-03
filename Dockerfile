FROM alpine:3.5

RUN apk add --no-cache ca-certificates

VOLUME /horizon-data

CMD horizon writer

ENV METRICS_ENDPOINT=:9094/metrics
LABEL METRICS_ENDPOINT=:9094/metrics

COPY rel/horizon_linux-amd64 /usr/bin/horizon
