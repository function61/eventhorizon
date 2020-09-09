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
	"github.com/function61/eventhorizon/pkg/ehreaderfactory"
	"github.com/function61/eventhorizon/pkg/policy"
	"github.com/function61/eventhorizon/pkg/system/ehcreddomain"
	"github.com/function61/eventhorizon/pkg/system/ehcredstate"
	"github.com/function61/gokit/crypto/cryptoutil"
	"github.com/function61/gokit/log/logex"
	"github.com/function61/gokit/os/osutil"
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
		Use:   "cat [id]",
		Short: "Print details of a credential",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			rootLogger := logex.StandardLogger()

			osutil.ExitIfError(credentialPrint(
				osutil.CancelOnInterruptOrTerminate(rootLogger),
				args[0],
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

func credentialPrint(ctx context.Context, id string, logger *log.Logger) error {
	client, err := ehreaderfactory.SystemClientFrom(ehreader.ConfigFromEnv)
	if err != nil {
		return err
	}

	credState, err := ehcredstate.LoadUntilRealtime(ctx, client, logger)
	if err != nil {
		return err
	}

	cred, err := credState.State.CredentialById(id)
	if err != nil {
		return err
	}

	printOneCredential(cred.Credential, cred.ApiKey)

	return nil
}

func credentialsList(ctx context.Context, logger *log.Logger) error {
	client, err := ehreaderfactory.SystemClientFrom(ehreader.ConfigFromEnv)
	if err != nil {
		return err
	}

	credState, err := ehcredstate.LoadUntilRealtime(ctx, client, logger)
	if err != nil {
		return err
	}

	for _, cred := range credState.State.Credentials() {
		printOneCredential(cred, "")
	}

	return nil
}

func printOneCredential(cred ehcredstate.Credential, apiKey string) {
	statementsHumanReadable := []string{}
	for _, statement := range cred.Policy.Statements {
		statementsHumanReadable = append(statementsHumanReadable, policy.HumanReadableStatement(statement))
	}

	maybeApiKey := func() string {
		if apiKey != "" {
			return fmt.Sprintf("API key: %s\n", apiKey)
		} else {
			return ""
		}
	}()

	fmt.Printf(
		"Credential: %s (Id: %s)\n%s%s\n\n",
		cred.Name,
		cred.Id,
		maybeApiKey,
		strings.Join(statementsHumanReadable, "\n"))
}

func credentialsCreate(ctx context.Context, name string, logger *log.Logger) error {
	client, err := ehreaderfactory.SystemClientFrom(ehreader.ConfigFromEnv)
	if err != nil {
		return err
	}

	credState, err := ehcredstate.LoadUntilRealtime(ctx, client, logger)
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
		cryptoutil.RandBase64UrlWithoutLeadingDash(4),
		name,
		cryptoutil.RandBase64UrlWithoutLeadingDash(16),
		string(policySerialized),
		ehevent.MetaSystemUser(time.Now()))

	if err := client.AppendAfter(
		ctx,
		credState.State.Version(),
		created,
	); err != nil {
		return err
	}

	fmt.Printf("API key: %s\n", created.ApiKey)

	return nil
}

func credentialRemove(ctx context.Context, id string, reason string, logger *log.Logger) error {
	client, err := ehreaderfactory.SystemClientFrom(ehreader.ConfigFromEnv)
	if err != nil {
		return err
	}

	credState, err := ehcredstate.LoadUntilRealtime(ctx, client, logger)
	if err != nil {
		return err
	}

	return credState.Reader.TransactWrite(ctx, func() error {
		if found := func() bool {
			for _, cred := range credState.State.Credentials() {
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

		return client.AppendAfter(
			ctx,
			credState.State.Version(),
			revoked)
	})
}
