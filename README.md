Event Horizon
=============

Summary
-------

Enables log-based architectures with realtime streaming, unlimited storage, compression and encryption.

| Key facts                  |                                                                                          |
|----------------------------|------------------------------------------------------------------------------------------|
| Use cases                  | Eventsourcing, high availability software/datacenter failover, microservices, big data.  |
| Is this a good fit for me? | For log-based architectures, yes. [Read more](docs/is-this-a-good-fit-for-me.md)         |
| Demo application           | [eventhorizon-exampleapp-go](https://github.com/function61/eventhorizon-exampleapp-go)             |
| Production ready?          | No, not yet. But not too far away.                                                       |
| Homepage & documentation   | Both project homepage and docs are currently on this README on GitHub                    |
| Technologies               | Golang, Linux, Docker (not required for use), AWS S3.                                    |
| Msg delivery semantics     | Exactly once, exact in-order delivery within a stream.                                   |
| Storage capacity           | Practically unlimited. You have to pay your bills though. :)                             |
| Data durability            | All writes transactionally backed by Write-Ahead-Log just like in databases.             |
| High availability          | Planned, going to use Hashicorp's Raft implementation.                                   |
| Data stored at             | AWS S3. Google Storage support planned.                                                  |
| Encryption at transport    | TLS (CA & server certs automatically managed)                                            |
| Encryption at rest         | AES256-CTR. Encryption keys are not trusted to AWS.                                      |
| Security                   | [Our security policy & information](https://function61.com/security/)                    |


Docs
----

How-to's & tutorials:

- [Quickstart](docs/quickstart.md) (for users)
- [Example app using Event Horizon](https://github.com/function61/eventhorizon-exampleapp-go)
	- Best way to grasp what Event Horizon is about, but you should also read quickstart.
	- Feature highlight: how to live-migrate a under-heavy-writes service with
	  zero downtime.
- [Building & contributing](docs/building-and-contributing.md) (for developers)
- [Operating](docs/operating.md)

Architecture:

- [Pusher wire protocol](docs/architecture/pusher-wire-protocol.md)
- [Raw storage format](docs/architecture/raw-storage-format.md)
- [Security](docs/architecture/security.md)
- [Network](docs/architecture/network.md)

Meta:

- [TODO & roadmap](docs/todo-roadmap.md)
- [Alternatives](docs/alternatives.md)


Architecture summary
--------------------

Writer manages writes to streams. Streams are divided into ~8 MB chunks which
are stored compressed & encrypted in AWS S3, except the last, "live", chunk that
we're writing into.

![](docs/architecture/diagram.png)

Data flow at a glance:

- Somebody contacts Writer to append a line to the stream
- Writer notifies pub/sub that a stream has new lines
- All interested applications that want to read from any streams have a Pusher,
  which subscribes to pub/sub notifications for those streams.
- Once the Pusher learns of new events, it issues a read to either:
	- a Writer if it's live data OR
	- S3 if it's older data.
- Pusher then pushes this read to your application. Your application transactionally
  verifies that the read offset is at the offset of last stored write in database.
  If not, error is returned along with the correct offset, and Pusher continues
  to read from the correct offset.


Programming language support
----------------------------

To use EventHorizon somewhere, that language has to be able to receive events over
HTTP + JSON, in just one HTTP path. There's even a helper library (pushlib - look
at the architecture diagram) for that, and it's only a small amount of code.

Existing implementations of pushlib:

- Go
	- [library](pusher/pushlib/) managed in this main repo
	- [example application](https://github.com/function61/eventhorizon-exampleapp-go)
- node.js (coming soon)
- PHP (coming soon)
- Your programming language not yet supported?
	- It's somewhat easy to implement - contributions appreciated :)


License & fair play
-------------------

Legal license: Apache 2.0 - free software.

Fair play: if you benefit commercially from the use of this project in any
significant capacity, any help would be appreciated (but not legally required):

- Order [support contract/consulting from us](https://function61.com/consulting/)
- Contribute with new features, bug fixes and/or help with issues
- Become a sponsor (get your company + link listed here in README as a sponsor)
- Donate money
- If you find EventHorizon useful, please spread the word! :)
