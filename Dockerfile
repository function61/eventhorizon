FROM alpine:3.5

RUN apk add --no-cache ca-certificates

VOLUME /horizon-data

CMD horizon writer

COPY cli/horizon/horizon /usr/bin/horizon
