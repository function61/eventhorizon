FROM alpine:3.5

RUN apk add --no-cache ca-certificates

VOLUME /horizon-data

CMD horizon writer

ENV METRICS_ENDPOINT=:9094/metrics
LABEL METRICS_ENDPOINT=:9094/metrics

COPY cli/horizon/horizon /usr/bin/horizon
