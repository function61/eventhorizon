Prerequisites
-------------

Read the [Quickstart](quickstart.md) guide first (you have to be learn to define
the `STORE` variable), maybe run the example app so you get comfortable with the
concepts first.

To contribute, follow these steps:

1. Clone this repo
2. Build the Docker dev image
3. Enter the container
4. Compile Pyramid
5. Make your change, compile & test
6. Commit to a feature branch, send a pull request in GitHub


Build the Docker image
----------------------

Development is done inside the Docker image produced by `Dockerfile-dev`. Build it:

```
$ docker build -t pyramid-dev -f Dockerfile-dev .
```

Enter the container:

```
$ docker run --name pyramid-dev -it -v "$(pwd):/app" -e STORE=... pyramid-dev
```

Building
--------

Install dependencies:

```
$ go get -d ./...
```

Build:

```
$ make
```

This produces the `pyramid` binary, which you can now run:

```
$ pyramid
```

(it is symlinked to `/usr/bin/pyramid` so you can run it from any directory)
