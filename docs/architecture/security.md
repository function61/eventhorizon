Security
========

Levels of access
----------------

- Knowing writable STORE token => you can delete data
	- This is the highest level of access. Guard it well.
	- This token should not ever be used outside of Writer instances
- Knowing readable STORE token => you can read data, but cannot delete
- Knowing the auth token => you can append data, create streams, manage subscriptions etc.


Attack surface
--------------

Attack surface is anything that is invokable over network:

- Writer's HTTP endpoints:
	- TLS-encrypted
	- All endpoints except `/metrics` require authentication token: known only
	  by Writers/Pushers.
- Pub/sub server's TCP socket:
	- TLS-encrypted
	- Requires auth token: known only by Writers/Pushers.
	- No sensitive data goes through pub/sub => only updated cursors.
- Your application's HTTP endpoint for receiving pushes:
	- Not TLS-encrypted but since traffic is over loopback, if someone manages
	  to sniff that traffic no amount of encryption would protect you.
	- Protected by per-session generated auth token: known only by Pusher+your app.
- scalablestore (AWS S3)
	- TLS-encrypted
	- Requires AWS IAM authentication tokens: known only by Writers/Pushers.
	- All chunks are AES256-CTR encrypted before storing in scalablestore, so even
	  if AWS were malicious it couldn't decrypt the stream contents because it
	  never sees the encryption keys.


Notes
-----

- You cannot disable TLS communication. We default to secure, and don't want to
  complicate the project more by supporting downgrading security by configuration.
- In summary: all communication between all components are TLS-encrypted, with
  the exception of Pusher <=> your application, but that is a non-issue.


Recommended practices
---------------------

- Use two separate AWS access keys for Writer and Pusher instances
  (this is explained in Quickstart):
	- Writers need read/write access to S3 buckets.
	- Pushers need only read access to S3 buckets.
