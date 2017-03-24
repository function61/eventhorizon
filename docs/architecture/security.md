Security
========

Facts
-----

- All communications between Writers, Pub/sub servers&clients, Pushers and
  scalablestore are TLS-encrypted. This is done even if all your components are
  in a "trusted LAN" (because that's a myth).
- The only transport-layer communication that is not encrypted is between
  Pusher <=> your application, but that is ok because the Pusher runs on loopback
  (same server).
	- Rationale: if someone manages to intercept your loopback traffic, no amount
	  of encryption would save you. "The calls are coming from inside the house".
- All files shipped to S3 are encrypted with AES/CBC. Encryption provided by S3
  is not used, because that would require us trusting AWS with encryption keys.
  Our design ensures that encryption keys are only known to Writers and Pushers.
- All Writer HTTP endpoints (expect /metrics) are protected by auth token.
	- Currently the auth token gives you full access.


Recommended actions
-------------------

- Use two separate AWS access keys for Writer and Pusher instances
  (this is explained in Quickstart):
	- Writers need read/write access to S3 buckets.
	- Pushers need only read access to S3 buckets.
