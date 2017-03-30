Enter Pyramid CLI
=================

With Pyramid CLI you can issue commands to the Writer servers, such as:

- Create new streams
- Manage subscriptions (subscribe/unsubscribe)
- Append event to a stream
- Batch-import events from a file to a stream
- Tap into the pub/sub subsystem
- Etc.

There are two options:

1. Run Pyramid CLI from Docker image
2. Install Pyramid binary locally

You can run the CLI from any (internet-connected) machine, as Pyramid will
auto-discover the Writer servers via S3. The connection from clients to the
Writer is secured with TLS + an auth token.

Note: read the [Quickstart](quickstart.md) tutorial to know how to specify the STORE variable.


Run Pyramid CLI from Docker image
---------------------------------

Launching the CLI container is simple:

```
$ docker run --rm -it -e STORE=s3://... fn61/pyramid sh
```

You should now be able to issue commands against the Writer cluster:

```
$ pyramid reader-read /:0:0:? 10
```

To exit from the container, hit `Ctrl + d` and the temporary container will be deleted.


Install Pyramid binary locally
------------------------------

This way you can connect to Pyramid Writer servers without using Docker.

Download Pyramid release from GitHub and put it into `/usr/bin/pyramid`.

Now configure STORE variable by running:

```
$ export STORE=s3://...
```

Now you should be able to connect to Pyramid:

```
$ pyramid reader-read /:0:0:? 10
```
