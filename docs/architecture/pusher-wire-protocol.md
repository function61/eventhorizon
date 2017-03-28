Pusher wire protocol
====================

Pusher is entirely stateless - all it knows is the application's endpoint URL to
which to send the wire protocol requests. It learns the state on-the-fly from the
app and eventually converges the app's subscription to the realtime state of the
system.

The protocol is strictly request-response, and centers around just one HTTP path
with the same JSON stuff going in regardless of Pusher's state. If Pusher doesn't
know something, it sends a dummy value and receives the correct value back with
normal flow of validation that the app must do in every normal request anyway.


High-level summary
------------------

Direction arrows:

- -> Pusher request to App
- <- App response to Pusher

Bootstrapping Pusher state:

- -> Resolve subscription ID
- <- Subscription ID response
- -> (Subscription stream) offset resolve request
- <- (Subscription strean) offset resolve response

Repeat this as long as Pyramid has newer data than app:

- Read newer data from Pyramid
- -> Push that to app
- <- ACK

Pushing any streams, whether they are subscription streams or application data streams
follows the exact same semantics. From the app's perspective there is nothing
different except for the fact that subscription streams contain `SubscriptionActivity`
events, which it must react to by checking if they are ahead of the app's own cursors.


Resolve subscription ID
-----------------------

Pusher doesn't know what the application's subscription ID is - it has to learn it.

Pusher -> app:

```
POST /_pyramid_push?auth=8c382056 HTTP/1.1

{
	"SubscriptionId": "_query_",
	"Read": {
		"FromOffset": "/:-1:-1",
		"Lines": []
	}
}
```

App responds with subscription ID:

```
HTTP/1.1 200 OK

{
	"Code": "incorrect_subscription_id",
	"CorrectSubscriptionId": "/_sub/example-sub"
}
```


Resolve stream offset for subscription
--------------------------------------

Now we know that app's subscription ID is `/_sub/example-sub`, but we don't know
subscriber's offset for it. We'll post an empty push with dummy offset that is
guaranteed to fail and yield us a correct offset.

Pusher -> app:

```
POST /_pyramid_push?auth=8c382056 HTTP/1.1

{
	"SubscriptionId": "/_sub/example-sub",
	"Read": {
		"FromOffset": "/_sub/example-sub:-1:-1",
		"Lines": []
	}
}
```

App responds with correct stream offset:

```
HTTP/1.1 200 OK

{
	"Code": "incorrect_base_offset",
	"AcceptedOffset": "/_sub/example-sub:0:0:?",
}
```

NOTE: app began from empty database, and thus needs to read the subscription
from beginning. Normally app would have state and `AcceptedOffset` would be
further from the beginning.


Actual push
-----------

Now we know app's position in the subscription. Now Pusher does a read against
Pyramid and push those events to the app:

```
POST /_pyramid_push?auth=8c382056 HTTP/1.1

{
	"SubscriptionId": "/_sub/example-sub",
	"Read": {
		"FromOffset": "/_sub/example-sub:0:0:127.0.0.1",
		"Lines": [{
			"PtrAfter": "/_sub/example-sub:0:65:127.0.0.1",
			"Content": "{\"subscription_ids\":[],\"ts\":\"2017-03-27T16:53:51.655Z\"}",
			"MetaType": "Created",
			"MetaPayload": {
				"subscription_ids": [],
				"ts": "2017-03-27T16:53:51.655Z"
			}
		}, {
			"PtrAfter": "/_sub/example-sub:0:157:127.0.0.1",
			"Content": "{\"activity\":[\"/sampledata:0:141:127.0.0.1\"],\"ts\":\"2017-03-27T16:54:09.376Z\"}",
			"MetaType": "SubscriptionActivity",
			"MetaPayload": {
				"activity": ["/sampledata:0:141:127.0.0.1"],
				"ts": "2017-03-27T16:54:09.376Z"
			}
		}, {
			"PtrAfter": "/_sub/example-sub:0:249:127.0.0.1",
			"Content": "{\"activity\":[\"/sampledata:0:171:127.0.0.1\"],\"ts\":\"2017-03-27T16:55:04.493Z\"}",
			"MetaType": "SubscriptionActivity",
			"MetaPayload": {
				"activity": ["/sampledata:0:171:127.0.0.1"],
				"ts": "2017-03-27T16:55:04.493Z"
			}
		}, {
			"PtrAfter": "/_sub/example-sub:0:341:127.0.0.1",
			"Content": "{\"activity\":[\"/sampledata:0:201:127.0.0.1\"],\"ts\":\"2017-03-27T18:10:28.352Z\"}",
			"MetaType": "SubscriptionActivity",
			"MetaPayload": {
				"activity": ["/sampledata:0:201:127.0.0.1"],
				"ts": "2017-03-27T18:10:28.352Z"
			}
		}, {
			"PtrAfter": "/_sub/example-sub:0:433:127.0.0.1",
			"Content": "{\"activity\":[\"/sampledata:0:231:127.0.0.1\"],\"ts\":\"2017-03-27T18:23:10.007Z\"}",
			"MetaType": "SubscriptionActivity",
			"MetaPayload": {
				"activity": ["/sampledata:0:231:127.0.0.1"],
				"ts": "2017-03-27T18:23:10.007Z"
			}
		}]
	}
}
```

App response:

```
HTTP/1.1 200 OK

{
	"Code": "success",
	"AcceptedOffset": "/_sub/example-sub:0:65:127.0.0.1",
	"BehindCursors": ["/sampledata:0:0:?"]
}
```

Subscription streams (that we just pushed) are a special case of a regular stream:
they are the only streams that contain `SubscriptionActivity` meta events. They
are hints into other streams that the app is subscribed to, that have had new
activity.

The app acks the subscription stream forward as long as none of the mentioned
streams' pointers are ahead.

In this case, the acked offset was `/_sub/example-sub:0:65`, which tells us that
the app acked the `Created` meta event, but stopped at the first
`SubscriptionActivity` it saw. That means that the app has never touched the
`/sampledata` stream.

To help Pusher, the app fills in `BehindCursors` list, which is a list of all
the unique streams the app saw in all `SubscriptionActivity` that it processed,
that were behind. The app scans all those events it sees, even if the very first
event indicates a behind cursor.

Pusher now starts workers for each streams that are behind for the app, and starts
pushing those streams' updates.

Pusher periodically tries to re-push the subscription stream, and when the app
has moved forward with the other streams referred to in the subscription stream,
the app will ack forward accordingly.

Strict ordering withing streams is guaranteed, but by this design a crude
chronological ordering is preserved across streams when pushing.

Moving on: Pusher now starts pushing unsynced streams.


Push unsynced stream
--------------------

Previously, the app told us that it wants to see `/sampledata` from the beginning. Push:

```
POST /_pyramid_push?auth=8c382056 HTTP/1.1

{
	"SubscriptionId": "/_sub/example-sub",
	"Read": {
		"FromOffset": "/sampledata:0:0:127.0.0.1",
		"Lines": [{
			"PtrAfter": "/sampledata:0:65:127.0.0.1",
			"Content": "{\"subscription_ids\":[],\"ts\":\"2017-03-27T16:54:00.539Z\"}",
			"MetaType": "Created",
			"MetaPayload": {
				"subscription_ids": [],
				"ts": "2017-03-27T16:54:00.539Z"
			}
		}, {
			"PtrAfter": "/sampledata:0:141:127.0.0.1",
			"Content": "{\"subscription_id\":\"/_sub/example-sub\",\"ts\":\"2017-03-27T16:54:08.682Z\"}",
			"MetaType": "Subscribed",
			"MetaPayload": {
				"subscription_id": "/_sub/example-sub",
				"ts": "2017-03-27T16:54:08.682Z"
			}
		}, {
			"PtrAfter": "/sampledata:0:171:127.0.0.1",
			"Content": "Mon Mar 27 16:55:02 UTC 2017",
			"MetaType": "",
			"MetaPayload": null
		}, {
			"PtrAfter": "/sampledata:0:201:127.0.0.1",
			"Content": "Mon Mar 27 18:10:24 UTC 2017",
			"MetaType": "",
			"MetaPayload": null
		}, {
			"PtrAfter": "/sampledata:0:231:127.0.0.1",
			"Content": "Mon Mar 27 18:23:08 UTC 2017",
			"MetaType": "",
			"MetaPayload": null
		}]
	}
}
```

As you can see, stream `/sampledata` consisted of these events:

- Created event (the very first event of all streams)
- Subscription event (this records that the app subscribed to this stream)
- Three dummy application-level events with just timestamps in them.

App responds by ACKing the stream all the way to the top:

```
HTTP/1.1 200 OK

{
	"Code": "success",
	"AcceptedOffset": "/sampledata:0:231:127.0.0.1",
	"BehindCursors": []
}
```

When Pusher re-pushes the subscription stream, app now ACKs `/sampledata`'s
`SubscriptionActivity` events forward as long as there are no behind cursors in it.
