package ehcli

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
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
		Short: "Credentials management (users, API keys)",
	}

	parentCmd.AddCommand(&cobra.Command{
		Use:   "ls",
		Short: "List users",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			rootLogger := logex.StandardLogger()

			osutil.ExitIfError(usersList(
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

			osutil.ExitIfError(userPrint(
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
		Short: "Remove/revoke access key",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			rootLogger := logex.StandardLogger()

			osutil.ExitIfError(accessKeyRemove(
				osutil.CancelOnInterruptOrTerminate(rootLogger),
				args[0],
				args[1],
				rootLogger))
		},
	})

	policyNames := []string{}
	cmd = &cobra.Command{
		Use:   "mk [name]",
		Short: "Create user",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			rootLogger := logex.StandardLogger()

			osutil.ExitIfError(userCreate(
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

func userPrint(
	ctx context.Context,
	userID string,
	withApiKey bool,
	logger *log.Logger,
) error {
	creds, _, err := loadCreds(ctx, logger)
	if err != nil {
		return err
	}

	user := creds.State.UserByID(userID)
	if user == nil {
		return fs.ErrNotExist
	}

	fmt.Printf("UserID=%s Created=%s\n", user.ID, timeutil.HumanizeDuration(time.Since(user.Created)))

	for _, accessKey := range user.AccessKeys {
		maybeApiKey := func() string {
			if withApiKey {
				return fmt.Sprintf("API key: %s", accessKey.CombinedToken())
			} else {
				return ""
			}
		}()

		fmt.Printf("AccessKey ID=%s Created=%s %s\n", accessKey.ID, timeutil.HumanizeDuration(time.Since(accessKey.Created)), maybeApiKey)
	}

	for _, policyID := range user.PolicyIDs {
		for _, statement := range creds.State.PolicyByID(policyID).Content.Statements {
			fmt.Println(policy.HumanReadableStatement(statement))
		}
	}

	return nil
}

func usersList(ctx context.Context, logger *log.Logger) error {
	creds, _, err := loadCreds(ctx, logger)
	if err != nil {
		return err
	}

	policies := creds.State.Policies()

	findPolicy := func(id string) *ehcred.Policy {
		for _, pol := range policies {
			if pol.ID == id {
				return &pol
			}
		}

		return nil
	}

	view := termtables.CreateTable()
	// TODO: add "Has inline policy"
	view.AddHeaders("UserID", "Name", "Created", "Policies")

	for _, user := range creds.State.Users() {
		policyNames := []string{}
		for _, policyId := range user.PolicyIDs {
			pol := findPolicy(policyId)
			if pol == nil { // shouldn't happen (referential integrity)
				return fmt.Errorf("cannot find policy by ID '%s'", policyId)
			}

			policyNames = append(policyNames, pol.Name)
		}

		view.AddRow(
			user.ID,
			user.Name,
			timeutil.HumanizeDuration(time.Since(user.Created)),
			strings.Join(policyNames, ", "))
	}

	fmt.Println(view.Render())

	return nil
}

func userCreate(
	ctx context.Context,
	name string,
	policyNames []string,
	logger *log.Logger,
) error {
	if len(policyNames) == 0 {
		return errors.New("doesn't make sense to create a user without a policy")
	}

	creds, client, err := loadCreds(ctx, logger)
	if err != nil {
		return err
	}

	meta := ehevent.MetaSystemUser(time.Now())

	userCreated := ehcreddomain.NewUserCreated(
		ehcreddomain.NewUserID(),
		name,
		meta)

	credentialCreated := ehcreddomain.NewUserAccessTokenCreated(
		userCreated.ID,
		ehcreddomain.NewAccessTokenID(),
		randomid.AlmostCryptoLong(),
		meta)

	events := []ehevent.Event{userCreated, credentialCreated}

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

		events = append(events, ehcreddomain.NewUserPolicyAttached(
			userCreated.ID,
			policy.ID,
			meta))
	}

	if err := client.AppendAfter(
		ctx,
		creds.State.Version(),
		events...,
	); err != nil {
		return err
	}

	return nil
}

func accessKeyRemove(ctx context.Context, id string, reason string, logger *log.Logger) error {
	creds, client, err := loadCreds(ctx, logger)
	if err != nil {
		return err
	}

	return creds.Reader.TransactWrite(ctx, func() error {
		cred := creds.State.CredentialByCombinedToken(id)
		if cred == nil {
			return fs.ErrNotExist
		}

		revoked := ehcreddomain.NewUserAccessTokenRevoked(
			cred.UserID,
			cred.AccessKeyID,
			reason,
			ehevent.MetaSystemUser(time.Now()))

		return client.AppendAfter(
			ctx,
			creds.State.Version(),
			revoked)
	})
}
