package ehcli

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/function61/eventhorizon/pkg/ehevent"
	"github.com/function61/eventhorizon/pkg/policy"
	"github.com/function61/eventhorizon/pkg/randomid"
	"github.com/function61/eventhorizon/pkg/system/ehcred"
	"github.com/function61/eventhorizon/pkg/system/ehcreddomain"
	"github.com/function61/gokit/log/logex"
	"github.com/function61/gokit/os/osutil"
	"github.com/function61/gokit/time/timeutil"
	"github.com/scylladb/termtables"
	"github.com/spf13/cobra"
)

func credentialsEntrypoint() *cobra.Command {
	parentCmd := &cobra.Command{
		Use:   "cred",
		Short: "Credentials management (API keys)",
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

	withApiKey := false
	cmd := &cobra.Command{
		Use:   "cat [id]",
		Short: "Print details of a credential",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			rootLogger := logex.StandardLogger()

			osutil.ExitIfError(credentialPrint(
				osutil.CancelOnInterruptOrTerminate(rootLogger),
				args[0],
				withApiKey,
				rootLogger))
		},
	}
	cmd.Flags().BoolVarP(&withApiKey, "with-api-key", "", withApiKey, "Print the API key (secret)")
	parentCmd.AddCommand(cmd)

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

	policyNames := []string{}
	cmd = &cobra.Command{
		Use:   "mk [name]",
		Short: "Create credentials",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			rootLogger := logex.StandardLogger()

			osutil.ExitIfError(credentialCreate(
				osutil.CancelOnInterruptOrTerminate(rootLogger),
				args[0],
				policyNames,
				rootLogger))
		},
	}
	cmd.Flags().StringSliceVarP(&policyNames, "policy", "", policyNames, "Policies to attach")
	parentCmd.AddCommand(cmd)

	return parentCmd
}

func credentialPrint(
	ctx context.Context,
	id string,
	withApiKey bool,
	logger *log.Logger,
) error {
	creds, _, err := loadCreds(ctx, logger)
	if err != nil {
		return err
	}

	cred, err := creds.State.CredentialById(id)
	if err != nil {
		return err
	}

	printOneCredential(*cred, withApiKey)

	return nil
}

func credentialsList(ctx context.Context, logger *log.Logger) error {
	creds, _, err := loadCreds(ctx, logger)
	if err != nil {
		return err
	}

	policies := creds.State.Policies()

	findPolicy := func(id string) *ehcred.Policy {
		for _, pol := range policies {
			if pol.Id == id {
				return &pol
			}
		}

		return nil
	}

	view := termtables.CreateTable()
	// TODO: add "Has inline policy"
	view.AddHeaders("Id", "Name", "Created", "Policies")

	for _, cred := range creds.State.Credentials() {
		policyIds, err := creds.State.CredentialAttachedPolicyIds(cred.Id)
		if err != nil {
			return err
		}

		policyNames := []string{}
		for _, policyId := range policyIds {
			pol := findPolicy(policyId)
			if pol == nil { // shouldn't happen (referential integrity)
				return fmt.Errorf("cannot find policy by ID '%s'", policyId)
			}

			policyNames = append(policyNames, pol.Name)
		}

		view.AddRow(
			cred.Id,
			cred.Name,
			timeutil.HumanizeDuration(time.Since(cred.Created)),
			strings.Join(policyNames, ", "))
	}

	fmt.Println(view.Render())

	return nil
}

func printOneCredential(cred ehcred.Credential, printApiKey bool) {
	statementsHumanReadable := []string{}
	for _, statement := range cred.Policy.Statements {
		statementsHumanReadable = append(statementsHumanReadable, policy.HumanReadableStatement(statement))
	}

	maybeApiKey := func() string {
		if printApiKey {
			return fmt.Sprintf("API key: %s\n", cred.ApiKey)
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

func credentialCreate(
	ctx context.Context,
	name string,
	policyNames []string,
	logger *log.Logger,
) error {
	if len(policyNames) == 0 {
		return errors.New("doesn't make sense to create credential without a policy")
	}

	creds, client, err := loadCreds(ctx, logger)
	if err != nil {
		return err
	}

	meta := ehevent.MetaSystemUser(time.Now())

	credentialCreated := ehcreddomain.NewCredentialCreated(
		randomid.Short(),
		name,
		randomid.AlmostCryptoLong(),
		meta)

	events := []ehevent.Event{credentialCreated}

	policies := creds.State.Policies()

	for _, policyName := range policyNames {
		policy := func() *ehcred.Policy {
			for _, pol := range policies {
				if pol.Name == policyName {
					return &pol
				}
			}

			return nil
		}()
		if policy == nil {
			return fmt.Errorf("policy by name not found: %s", policyName)
		}

		events = append(events, ehcreddomain.NewCredentialPolicyAttached(
			credentialCreated.Id,
			policy.Id,
			meta))
	}

	if err := client.AppendAfter(
		ctx,
		creds.State.Version(),
		events...,
	); err != nil {
		return err
	}

	fmt.Printf("API key: %s\n", credentialCreated.ApiKey)

	return nil
}

func credentialRemove(ctx context.Context, id string, reason string, logger *log.Logger) error {
	creds, client, err := loadCreds(ctx, logger)
	if err != nil {
		return err
	}

	return creds.Reader.TransactWrite(ctx, func() error {
		if found := func() bool {
			for _, cred := range creds.State.Credentials() {
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
			creds.State.Version(),
			revoked)
	})
}
