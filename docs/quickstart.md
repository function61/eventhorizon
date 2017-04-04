Quickstart
==========

Summary
-------

- Prepare your AWS account:
	- Create [S3 bucket](https://aws.amazon.com/s3/)
	- Create IAM credentials for S3
- Set up Writer ("the server")
- Run an example application feeding off of Event Horizon

Event Horizon runs just fine whether you have the Writer and application on the same server or not.


Create S3 bucket
----------------

I created an S3 bucket named `eventhorizon1.fn61.net` in region `EU (Frankfurt)`.

So our details are:

- Bucket name = `eventhorizon1.fn61.net` (it doesn't have to be a DNS name)
- Region code = `eu-central-1` ([S3 region codes](http://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region))

You'll need these later.


Create IAM credentials for S3
-----------------------------

[Create AWS IAM credentials](configuring/create-aws-iam-credentials.md)


Assembling your ENV variable
----------------------------

Event Horizon is configured via one ENV variable: `STORE`. It contains the S3 details
and all the rest is handled automatically.

The variable looks like this:

```
STORE=s3://AKIAIZQ7QCQOTMYODIAA:secretkey@eu-central-1/eventhorizon1.fn61.net
```

Where:

```
STORE=s3://APIKEY_ID:APIKEY_SECRET@S3_REGION/S3_BUCKET
```

NOTE: temporarily you have to replace `/` chars in secret key with `_`.

Define the ENV variable on your Writer server:

```
$ export STORE="s3://..."
```

(or pass it inline to Docker's `-e` argument)


Start Writer on the server
--------------------------

Now start the Writer on the server:

```
$ docker run --name eventhorizon -d --net=host -e "STORE=$STORE" fn61/eventhorizon

then check the logs:

$ docker logs eventhorizon
2017/03/31 11:08:59 configfactory: downloading discovery file
2017/03/31 11:09:00 main: failed to get discovery file - trying to bootstrap.
2017/03/31 11:09:00 bootstrap: starting bootstrap process
2017/03/31 11:09:00 bootstrap: resolving public IP from ipify.org
2017/03/31 11:09:00 bootstrap: public IP to advertise: 207.154.237.26
2017/03/31 11:09:00 bootstrap: generating certificate authority
2017/03/31 11:09:00 bootstrap: generating auth token
2017/03/31 11:09:00 bootstrap: generating encryption master key
2017/03/31 11:09:00 bootstrap: generating discovery file
2017/03/31 11:09:00 bootstrap: uploading discovery file to scalablestore
2017/03/31 11:09:00 bootstrap: discovery file uploaded to scalablestore
...
2017/03/31 11:09:00 PubSubServer: starting mainLogicLoop
2017/03/31 11:09:00 main: waiting for stop signal
2017/03/31 11:09:00 WriterHttp: binding to :9092
```

Now let's verify that all is working. Enter [Horizon CLI](enter-horizon-cli.md)
and read from the root stream (`/`):

```
$ horizon reader-read /:0:0:? 10
/Created {"subscription_ids":[],"ts":"2017-03-31T11:09:00.643Z"}
/ChildStreamCreated {"name":"/_sub","cursor":"/_sub:0:0:207.154.237.26","ts":"2017-03-31T11:09:00.647Z"}
```

Everything seems ok. You're done setting up!

Event Horizon is built on brutal simplicity. Under the hood lots of things were
done for you like discovering public IP address, provisioning certificate
authority, provisioning SSL cert, generating auth tokens and bootstrapping root
and subscription streams.

NOTE: if you don't have internet-routable IP address or want to explicitly use
this in a LAN-only setting, you can define ENV `WRITER_IP_TO_ADVERTISE=127.0.0.1`.


Now run your example application
--------------------------------

Go run [the example app](https://github.com/function61/eventhorizon-exampleapp-go),
either on the same server or a different server. This tutorial will get you up
to speed on the data model and concepts.
