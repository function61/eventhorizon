![Build status](https://github.com/function61/eventhorizon/workflows/Build/badge.svg)
[![Download](https://github.com/function61/eventhorizon/releases)](docs/assets/Download-install-green.svg)

Event Horizon
=============

**NOTE: major refactoring** underway. Event Horizon started as a promising R&D project that
wasn't production-ready due to its persistence layer not being a highly available datastore.

The v1 was solid as far as the (code-side) UX in the using application was concerned,
so I took what was good in v1 and started building v2 with a better, easier persistence layer
(AWS DynamoDB).

This document is work-in-progress and describes the progress of the EventHorizon v2.

Contents:

- [Installation](#installation)
- [How to identify old vs. new?](#how-to-identify-old-vs-new)
- [Sample application using EventHorizon](#sample-application-using-eventhorizon)
- [How does it look in my application?](#how-does-it-look-in-my-application)


Installation
------------

Currently there is no "server" component - the client library uses AWS DynamoDB directly.

Therefore, installation is: how to start using the client library.

Steps:

1. [Setting up data storage](docs/setting-up-data-storage/README.md)


How to identify old vs. new?
----------------------------

Code:

- New = everything under `pkg/` (but not including `legacy/` sub-package).
- Old = the `cmd/horizon` binary (and CLI) is using the legacy code (`pkg/legacy/..`)
  * Also the Docker image is the legacy binary

Documentation:

- New = everything linked from this README (except the below link)
- Old = [Legacy documentation is readable here](README-legacy.md)


Sample applications using EventHorizon
--------------------------------------

These apps use EventHorizon:

- [CertBus](https://github.com/function61/certbus)
- [Edgerouter](https://github.com/function61/edgerouter)
- [Lambda-alertmanager](https://github.com/function61/lambda-alertmanager)
- [Deployer](https://github.com/function61/deployer)
- [Varasto](https://github.com/function61/varasto) (soon)
- [pi-security-module](https://github.com/function61/pi-security-module) (soon)


Consistency model
-----------------

On use cases that require consistency, it is provided via **optimistic locking**. Optimistic
locking means that if you don't want to sometimes show error message for user to try again
(this would be bad UX), your application has to re-try the write:

- refresh its read model so you have the data that caused the conflict
- run the validations again
- append the event again

There's a helper for this:
[Reader.TransactWrite](https://godoc.org/github.com/function61/eventhorizon/pkg/ehreader#Reader.TransactWrite)
and [here's how using it looks in a real-world application]().

There's even a
[test that tests for conflicts on concurrent writes](https://github.com/function61/eventhorizon/blob/f89fe5d462ca6d7efd03a0b9b871bbec0ed513d9/pkg/ehreader/reader_test.go#L86).


How does it look in my application?
-----------------------------------

See [pkg/ehreader/reader_test.go](pkg/ehreader/reader_test.go) for example test case that
demoes how to use the reader to read data from EventHorizon.

I hope you can see from the test code that we also great testability support for
implementors of `EventsProcessor` interface where you don't have to do anything special for
your production code to be testable.

For receiving realtime data you would call
[Reader.Synchronizer](https://godoc.org/github.com/function61/eventhorizon/pkg/ehreader#Reader.Synchronizer).


Architecture
------------

Our events table and snapshots table mostly follow the design from page 41 onwards in
[CQRS Documents by Greg Young](https://cqrs.files.wordpress.com/2010/11/cqrs_documents.pdf).
