
Summary
-------

Enables log-based architectures with realtime streaming, unlimited storage, compression and encryption.

| Key facts               |                                                                                          |
|-------------------------|------------------------------------------------------------------------------------------|
| Technologies            | Golang, Linux, Docker (not required for use), AWS S3.                                    |
| Production ready?       | No, not yet. But not too far away.                                                       |
| Use cases               | High availability software, datacenter failover and microservices.                       |
| Storage capacity        | Practically unlimited. You have to pay your bills though. :)                             |
| Data stored at          | AWS S3. Google Storage support planned.                                                  |
| Encryption at transport | TLS                                                                                      |
| Encryption at rest      | AES-CBC. Encryption keys are not trusted to AWS (=> not using S3 server-side encryption) |


Docs
----

- [Building & contributing](docs/building-and-contributing.md)


Architecture
------------

Writer manages writes to streams. Streams are divided into ~16 MB chunks which
are stored compressed & encrypted in AWS S3, except the last, "live", chunk that
we're writing into.

![](docs/architecture/diagram.png)

Quick glance:

- Somebody contacts Writer to append a line to the stream
- Writer notifies pub/sub that a stream has new lines
- All interested applications that want to read from any streams have a Pusher,
  which subscribes to pub/sub notifications for those streams.
- Once the Pusher learns of new events, it issues a read to either:
	- a Writer if it's live data OR
	- S3 if it's older data.
- Pusher then pushes this read to your application. The application transactionally
  verifies that the read offset is at the offset of last stored write in database.
  If not, error is returned along with the correct offset, and Pusher continues
  to read from the correct offset.


License & fair play
-------------------

Legal license: Apache 2.0 - free software.

Moral license: if you benefit commercially from the use of this project, any help
would be appreciated (though not legally required) if we helped you make money:

- Order support contract from us
- Contribute with new features, bug fixes and/or help with issues
- Become a sponsor
- Donate money
