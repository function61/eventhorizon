Enter Horizon CLI
=================

Event Horizon's CLI (Command Line Interface) is known as `horizon`, for brevity.

With Horizon CLI you can issue commands to the Writer servers, such as:

- Create new streams
- Manage subscriptions (subscribe/unsubscribe)
- Append event to a stream
- Batch-import events from a file to a stream
- Tap into the pub/sub subsystem
- Etc.

There are two options:

1. Run Horizon CLI from Docker image
2. Install Horizon binary locally

You can run the CLI from any (internet-connected) machine, as Horizon will
auto-discover the Writer servers via S3. The connection from clients to the
Writer is secured with TLS + an auth token.

Note: read the [Quickstart](quickstart.md) tutorial to know how to specify the STORE variable.


Run Horizon CLI from Docker image
---------------------------------

Launching the CLI container is simple:

```
$ export STORE=s3://..
$ docker run --rm -it -e "STORE=$STORE" fn61/eventhorizon sh
```

You should now be able to issue commands against the Writer cluster:

```
$ horizon reader-read /:0:0:? 10
```

To exit from the container, hit `Ctrl + d` and the temporary container will be deleted.


Install Horizon binary locally
------------------------------

This way you can connect to Writer servers without using Docker.

Download Event Horizon release from GitHub and put it into `/usr/bin/horizon`.

Now configure STORE variable by running:

```
$ export STORE=s3://...
```

Now you should be able to connect:

```
$ horizon reader-read /:0:0:? 10
```
