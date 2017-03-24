Network
=======

This document describes networking considerations per each component.

![](diagram.png)


Writer
------

Inbound connections:

- Writer API (HTTPS)

Outbound connections:

- scalablestore (HTTPS)
- Pub/sub server (TCP/TLS)


Pub/sub server
--------------

NOTE: currently pub/sub server is embedded with Writer server.

Inbound connections:

- Pub/sub server (TCP/TLS)


Pusher
------

Inbound connections:

- Writer proxy (loopback HTTP)

Outbound connections:

- Writer API (HTTPS)
- scalablestore (HTTPS)
- Endpoint (loopback HTTP)
