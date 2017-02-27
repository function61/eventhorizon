
Getting up and running
----------------------


Contributing
------------

1. Clone this repo.
2. Enter dev container: `$ docker run --rm -it -v "$(pwd):/app" eventhorizon-dev`
3. Define S3 credentials:
```
export AWS_ACCESS_KEY_ID="AKIAJVH7Q2W4T7USVWRQ"
export AWS_SECRET_ACCESS_KEY="..."
```
3. Install dependencies: `$ go get -d ./...`
4. Change a file (or test building before changing anything).
5. Run `$ go build`
6. Run `$ ./app server`
7. `$ go fmt ./...`
8. Commit to a feature branch & push & send a PR.


Building the dev Docker image
-----------------------------

```
$ docker build -t eventhorizon-dev -f Dockerfile-dev .
```
