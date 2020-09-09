package main

import (
	"github.com/function61/eventhorizon/pkg/ehcli"
	"github.com/function61/eventhorizon/pkg/ehserver"
	"github.com/function61/gokit/app/aws/lambdautils"
	"github.com/function61/gokit/os/osutil"
)

func main() {
	if lambdautils.InLambda() {
		osutil.ExitIfError(ehserver.LambdaEntrypoint())
		return
	}

	osutil.ExitIfError(ehcli.Entrypoint().Execute())
}
