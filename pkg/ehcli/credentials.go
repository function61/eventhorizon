package ehcli

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/eventhorizon/pkg/ehevent"
	"github.com/function61/eventhorizon/pkg/ehreader"
	"github.com/function61/eventhorizon/pkg/policy"
	"github.com/function61/eventhorizon/pkg/system/ehcreddomain"
	"github.com/function61/eventhorizon/pkg/system/ehcredstate"
	"github.com/function61/gokit/cryptorandombytes"
	"github.com/function61/gokit/logex"
	"github.com/function61/gokit/osutil"
	"github.com/spf13/cobra"
)

func credentialsEntrypoint() *cobra.Command {
	parentCmd := &cobra.Command{
		Use:   "creds",
		Short: "Credentials management",
	}

	parentCmd.AddCommand(&cobra.Command{
		Use:   "ls",
		Short: "List credentials",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			rootLogger := logex.StandardLogger()

			osutil.ExitIfError(credentialsList(
				osutil.CancelOnInterruptOrTerminate(rootLogger),
				rootLogger))
		},
	})

	parentCmd.AddCommand(&cobra.Command{
		Use:   "rm [id] [reason]",
		Short: "Remove/revoke credentials",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			rootLogger := logex.StandardLogger()

			osutil.ExitIfError(credentialRemove(
				osutil.CancelOnInterruptOrTerminate(rootLogger),
				args[0],
				args[1],
				rootLogger))
		},
	})

	parentCmd.AddCommand(&cobra.Command{
		Use:   "mk [name]",
		Short: "Create credentials",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			rootLogger := logex.StandardLogger()

			osutil.ExitIfError(credentialsCreate(
				osutil.CancelOnInterruptOrTerminate(rootLogger),
				args[0],
				rootLogger))
		},
	})

	return parentCmd
}

func credentialsList(ctx context.Context, logger *log.Logger) error {
	client, err := ehreader.SystemClientFrom(ehreader.ConfigFromEnv)
	if err != nil {
		return err
	}

	sysState, err := ehcredstate.LoadUntilRealtime(ctx, client, logger)
	if err != nil {
		return err
	}

	for _, cred := range sysState.State.Credentials() {
		statementsHumanReadable := []string{}
		for _, statement := range cred.Policy.Statements {
			statementsHumanReadable = append(statementsHumanReadable, policy.HumanReadableStatement(statement))
		}

		fmt.Printf("Credential: %s (Id: %s)\n%s\n\n", cred.Name, cred.Id, strings.Join(statementsHumanReadable, "\n"))
	}

	return nil
}

func credentialsCreate(ctx context.Context, name string, logger *log.Logger) error {
	client, err := ehreader.SystemClientFrom(ehreader.ConfigFromEnv)
	if err != nil {
		return err
	}

	sysState, err := ehcredstate.LoadUntilRealtime(ctx, client, logger)
	if err != nil {
		return err
	}

	readWrite := []policy.Action{
		eh.ActionStreamCreate,
		eh.ActionStreamRead,
		eh.ActionStreamAppend,
		eh.ActionSnapshotRead,
		eh.ActionSnapshotWrite,
		eh.ActionSnapshotDelete,
	}

	policySerialized := policy.Serialize(policy.NewPolicy(policy.NewAllowStatement(
		readWrite,
		eh.RootName.Child("*").ResourceName(),
		eh.ResourceNameSnapshot.Child("*"),
	)))

	created := ehcreddomain.NewCredentialCreated(
		cryptorandombytes.Base64UrlWithoutLeadingDash(4),
		name,
		cryptorandombytes.Base64UrlWithoutLeadingDash(16),
		string(policySerialized),
		ehevent.MetaSystemUser(time.Now()))

	if _, err := sysState.Writer.AppendAfter(
		ctx,
		sysState.State.Version(),
		ehevent.Serialize(created),
	); err != nil {
		return err
	}

	fmt.Printf("API key: %s\n", created.ApiKey)

	return nil
}

func credentialRemove(ctx context.Context, id string, reason string, logger *log.Logger) error {
	client, err := ehreader.SystemClientFrom(ehreader.ConfigFromEnv)
	if err != nil {
		return err
	}

	sysState, err := ehcredstate.LoadUntilRealtime(ctx, client, logger)
	if err != nil {
		return err
	}

	return sysState.Reader.TransactWrite(ctx, func() error {
		if found := func() bool {
			for _, cred := range sysState.State.Credentials() {
				if cred.Id == id {
					return true
				}
			}

			return false
		}(); !found {
			return fmt.Errorf("Credential to remove not found with id: %s", id)
		}

		revoked := ehcreddomain.NewCredentialRevoked(
			id,
			reason,
			ehevent.MetaSystemUser(time.Now()))

		_, err = sysState.Writer.AppendAfter(
			ctx,
			sysState.State.Version(),
			ehevent.Serialize(revoked),
		)
		return err
	})
}
