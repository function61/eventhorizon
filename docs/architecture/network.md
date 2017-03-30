Network
=======

This document describes networking considerations per each component.

![](diagram.png)


Writer
------

It is recommended that you run Writer on an Internet-routable IP (= public IP),
though advertising Writer on a LAN-only IP should work too.


Inbound (other components connect to):

- Writer API (:9092 HTTPS)

Outbound (opens connections to):

- scalablestore (:443 HTTPS)
- Pub/sub server (:9091 TCP/TLS)


Pub/sub server
--------------

NOTE: currently pub/sub server is embedded with Writer server.

Inbound (other components connect to):

- Pub/sub server (:9091 TCP/TLS)


Pusher
------

Pusher is usually ran alongside each application instance. There is no limit
on the number of Pusher instances - they are stateless.

Only inbound connection to Pusher is from loopback, therefore Pusher doesn't need
much on networking/firewall side.

Inbound (other components connect to):

- Writer proxy (:9093 loopback HTTP)

Outbound (opens connections to):

- Writer API (:9092 HTTPS)
- Pub/sub server (:9091 TCP/TLS)
- scalablestore (:443 HTTPS)
- Endpoint (loopback HTTP)
