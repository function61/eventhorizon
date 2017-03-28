Is this a good fit for me?
==========================

TL;DR: Pyramid is good for EventSourcing, but it is up to you to weigh if
EventSourcing is a good architecture for you.

Like advertised, Pyramid is a good fit for log-based architectures or anything
else in log form (like big data).

In this document I'm going to focus on EventSourcing (a more concrete form of
log-based architectures) as a use case and if it's a good fit for you.

I'm not going to lie and tell you that EventSourcing is the right tool for you
or that Pyramid is good for everything else as well. Pyramid is bad as an
operational database, but good as a realtime source of truth or for computing
something (maybe reports) from large amounts of data over large spans of time.

If you don't yet know about log-based architectures, this is a good read:
https://www.confluent.io/blog/using-logs-to-build-a-solid-data-infrastructure-or-why-dual-writes-are-a-bad-idea/


Good things about EventSourcing
-------------------------------

- Guaranteed audit log of everything.
- EventSourcing has huge scalability if implemented well.
- EventSourcing is the only type of model that doesn't lose any data.
- Separation of read and write models. Your events are the only write model - your
  read model is how you project those events into your database. It is easier to
  model events based on what happens in the real world, because you don't have
  to think how that is modeled in the database.
- Easily separate your operational and analytical databases. They both can have
  optimized data models for their use cases.
- Enables downtime-free architecture (planned maintenance - unplanned downtime
  is always going to happen) even in face of database migrations or **even a total
  replacement of your tech stack**. Live migrate your system while your users are
  using it, and do the switchover when the replacement system has reached live state.
- High availability becomes easier because with EventSourcing you can use any
  database in a multi-master-like configuration without using the database's
  internal clustering tools. Each database is independent, but they still converge
  to the same state. **Pyramid is the transaction log**.
- Easier to write microservices or other types of distributed systems, because
  Pyramid works as an efficient distributed event bus.
- Integrations with partners can be implemented either as 100 % Pyramid stream
  output to the partner's system, or use an event as a trigger for custom code
  that builds the custom message to be sent to the partner's system.
- You can use polyglot persistence (= different databases for each optimized use
  case), if that benefits you.
- If your event database is backed up (Pyramid's use of S3 yields 99.999999999%
  durability), your **operational databases don't need backups**. **Event stream
  is the incremental backup**. However, if you only have a single operational
  database (= single point of failure => you shouldn't, if time to recovery is
  crucial) and you are concerned about time to recovery, you can always snapshot
  your database and restore efficiently by applying delta, which you don't have
  to write code for because Pyramid works by pushing deltas.


Bad things about EventSourcing
------------------------------

- You need to use a
  [task-based UI](https://image.slidesharecdn.com/psidi6-1209718520335063-8/95/patterns-for-distributed-systems-45-638.jpg),
  because having a huge form with semi-related fields that you can all modify
  and save as a whole is not going to work with EventSourcing.
	- Though from UX perspective task-based UI is probably a win.
- EventSourcing somewhat implies eventual consistency, which can be a pain to
  work with.
	- Though there is a feature in Pyramid that enables you to use optimistic
	locking to protect consistency by either letting commands sometimes fail or
	use a bit more code to handle re-tries.
- While events are hugely beneficial, they also are an additional layer that
  you might not have used before. It's a bit more code and stuff to think about.
- Saving all events from the beginning of time takes more space than just storing
  the current state. Pyramid helps you with that by using compression and cheap
  storage like AWS S3.
