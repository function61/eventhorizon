#!/bin/bash -eu

run() {
	fn="$1"

	echo "# $fn"

	"$fn"
}

downloadDependencies() {
	# dep ensure
	go get -d ./...
}

unitTests() {
	go test ./...
}

staticAnalysis() {
	go vet ./...
}

buildLinuxArm() {
	(cd cli/horizon && GOOS=linux GOARCH=arm go build -o ../../rel/horizon_linux-arm)
}

buildLinuxAmd64() {
	(cd cli/horizon && GOOS=linux GOARCH=amd64 go build -o ../../rel/horizon_linux-amd64)
}

uploadBuildArtefacts() {
	# the CLI breaks automation unless opt-out..
	export JFROG_CLI_OFFER_CONFIG=false

	jfrog-cli bt upload \
		"--user=joonas" \
		"--key=$BINTRAY_APIKEY" \
		--publish=true \
		'rel/*' \
		"function61/eventhorizon/main/$FRIENDLY_REV_ID" \
		"$FRIENDLY_REV_ID/"
}

rm -rf rel
mkdir rel

run downloadDependencies

# awaiting https://github.com/function61/eventhorizon/issues/5
# run staticAnalysis

run unitTests

run buildLinuxArm

run buildLinuxAmd64

if [ "${PUBLISH_ARTEFACTS:-''}" = "true" ]; then
	run uploadBuildArtefacts
fi

