Setting up data storage
=======================

EventHorizon uses AWS DynamoDB as its data storage. We'll walk you through setting it up.

Contents:

- [Create DynamoDB table for EventHorizon](#create-dynamodb-table-for-eventhorizon)
- [Configure AWS permissions](#configure-aws-permissions)
- [Bootstrap EventHorizon](#bootstrap-eventhorizon)


Create DynamoDB table for EventHorizon
--------------------------------------

Create DynamoDB tables in `eu-central-1` (currently the region is hardcoded..)

### Events table

- Name = `prod_eh_events`
- Primary key = `s` (type: string)
- Add sort key, name = `v` (type: number)


### Snapshots table

- Name = `prod_eh_snapshots`
- Primary key = `s` (type: string)
- Add sort key, name = `c` (type: string)


### More info

We recommend starting with on-demand pricing so you don't have to think about scaling issues.
You can always change it later to start saving money if it gets pricy (it won't if your usage
is not massive) if you know your scaling patterns.

If you're wondering, why the key names are single-char names, you have to pay for each item's
attributes' key names per row also, so I decided to keep the most common needed attributes
short because I anticipate storing billions of events.


Configure AWS permissions
-------------------------

Permissions in AWS are configured in IAM.

We recommend creating two user groups:

- EventHorizon-read
- EventHorizon-readwrite

For read usergroup, define inline policy (recommended policy name `events-read`):

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
                "dynamodb:GetItem",
                "dynamodb:Query"
            ],
            "Resource": [
                "arn:aws:dynamodb:*:*:table/prod_eh_events",
                "arn:aws:dynamodb:*:*:table/prod_eh_snapshots"
            ]
        }
    ]
}
```

For readwrite usergroup, define inline policy (recommended policy name `events-readwrite`):

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
                "dynamodb:GetItem",
                "dynamodb:PutItem",
                "dynamodb:DeleteItem",
                "dynamodb:Query"
            ],
            "Resource": [
                "arn:aws:dynamodb:*:*:table/prod_eh_events",
                "arn:aws:dynamodb:*:*:table/prod_eh_snapshots"
            ]
        }
    ]
}
```


Bootstrap EventHorizon
----------------------

We need bootstrap EventHorizon's data structures. We either:

- Need a temporary user (with `EventHorizon-readwrite` group) to do it
- Or use other credentials with write access if you came here from setting up another
  project like CertBus
  * You can use credentials of the `CertBus-manager` user since it has write permission.

From this point on, you have to have these ENV vars defined for your AWS API keys:

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`

Run this to bootstrap EventHorizon:

```console
$ certbus eh-prod bootstrap
```

(Note: we're using `certbus` binary because v2 of EventHorizon doesn't have its own CLI binary yet.)

This created basic internal data structures for EventHorizon.
