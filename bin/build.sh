#!/bin/bash -eu

source /build-common.sh

COMPILE_IN_DIRECTORY="cmd/horizon"
BINARY_NAME="horizon"
BINTRAY_PROJECT="function61/eventhorizon"

standardBuildProcess
