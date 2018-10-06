#!/bin/bash -eu

source /build-common.sh

COMPILE_IN_DIRECTORY="cli/horizon"
BINARY_NAME="horizon"
BINTRAY_PROJECT="eventhorizon"

standardBuildProcess
