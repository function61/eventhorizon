[![Build Status](https://img.shields.io/travis/function61/eventhorizon.svg?style=for-the-badge)](https://travis-ci.org/function61/eventhorizon)
[![Download](https://img.shields.io/bintray/v/function61/dl/eventhorizon.svg?style=for-the-badge&label=Download)](https://bintray.com/function61/dl/eventhorizon/_latestVersion#files)
[![MicroBadger Size](https://img.shields.io/microbadger/image-size/fn61/eventhorizon.svg?style=for-the-badge&label=Docker+image)](https://hub.docker.com/r/fn61/eventhorizon/)

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


Sample application using EventHorizon
-------------------------------------

See [CertBus](https://github.com/function61/certbus).


How does it look in my application?
-----------------------------------

See [pkg/ehreader/reader_test.go](pkg/ehreader/reader_test.go) for example test case that
demoes how to use the reader to read data from EventHorizon.

I hope you can see from the test code that we also great testability support for
implementors of `EventsProcessor` interface where you don't have to do anything special for
your production code to be testable.

For receiving realtime data you would call
[Reader.Synchronizer](https://godoc.org/github.com/function61/eventhorizon/pkg/ehreader#Reader.Synchronizer).
