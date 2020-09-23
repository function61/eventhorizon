package ehcli

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/function61/eventhorizon/pkg/ehclient"
	"github.com/function61/eventhorizon/pkg/ehclientfactory"
	"github.com/function61/eventhorizon/pkg/ehevent"
	"github.com/function61/eventhorizon/pkg/system/ehcred"
	"github.com/function61/eventhorizon/pkg/system/ehcreddomain"
	"github.com/function61/gokit/log/logex"
	"github.com/function61/gokit/os/osutil"
	"github.com/function61/gokit/sliceutil"
	"github.com/function61/gokit/time/timeutil"
	"github.com/scylladb/termtables"
	"github.com/spf13/cobra"
)

func policiesList(ctx context.Context, logger *log.Logger) error {
	client, err := ehclientfactory.SystemClientFrom(ehclient.ConfigFromEnv, logger)
	if err != nil {
		return err
	}

	credState, err := ehcred.LoadUntilRealtime(ctx, client, logger)
	if err != nil {
		return err
	}

	view := termtables.CreateTable()
	view.AddHeaders("Id", "Name", "Created")

	for _, pol := range credState.State.Policies() {
		view.AddRow(
			pol.Id,
			pol.Name,
			timeutil.HumanizeDuration(time.Since(pol.Created)),
		)
	}

	fmt.Println(view.Render())

	return nil
}

func policyPrint(ctx context.Context, id string, logger *log.Logger) error {
	client, err := ehclientfactory.SystemClientFrom(ehclient.ConfigFromEnv, logger)
	if err != nil {
		return err
	}

	credState, err := ehcred.LoadUntilRealtime(ctx, client, logger)
	if err != nil {
		return err
	}

	pol, err := findPolicyById(id, credState.State)
	if err != nil {
		return err
	}

	policyJson, err := json.MarshalIndent(pol.Content, "", "  ")
	if err != nil {
		return err
	}

	fmt.Printf(
		"  ID: %s\nName: %s\n\n%s\n",
		pol.Id,
		pol.Name,
		policyJson)

	return nil
}

func policyDelete(ctx context.Context, id string, logger *log.Logger) error {
	client, err := ehclientfactory.SystemClientFrom(ehclient.ConfigFromEnv, logger)
	if err != nil {
		return err
	}

	credState, err := ehcred.LoadUntilRealtime(ctx, client, logger)
	if err != nil {
		return err
	}

	return credState.Reader.TransactWrite(ctx, func() error {
		if err := credState.State.PolicyExistsAndIsAbleToDelete(id); err != nil {
			return err
		}

		return client.AppendAfter(
			ctx,
			credState.State.Version(),
			ehcreddomain.NewPolicyRemoved(id, ehevent.MetaSystemUser(time.Now())))
	})
}

func policyRename(ctx context.Context, id string, newName string, logger *log.Logger) error {
	client, err := ehclientfactory.SystemClientFrom(ehclient.ConfigFromEnv, logger)
	if err != nil {
		return err
	}

	credState, err := ehcred.LoadUntilRealtime(ctx, client, logger)
	if err != nil {
		return err
	}

	return credState.Reader.TransactWrite(ctx, func() error {
		pol, err := findPolicyById(id, credState.State)
		if err != nil {
			return err
		}

		if pol.Name == newName {
			return fmt.Errorf("policy '%s' name unchanged", newName)
		}

		return client.AppendAfter(
			ctx,
			credState.State.Version(),
			ehcreddomain.NewPolicyRenamed(id, newName, ehevent.MetaSystemUser(time.Now())))
	})
}

func policyAttach(ctx context.Context, credentialId string, policyId string, logger *log.Logger) error {
	client, err := ehclientfactory.SystemClientFrom(ehclient.ConfigFromEnv, logger)
	if err != nil {
		return err
	}

	credState, err := ehcred.LoadUntilRealtime(ctx, client, logger)
	if err != nil {
		return err
	}

	return credState.Reader.TransactWrite(ctx, func() error {
		exists, err := credentialHasPolicyAttached(credentialId, policyId, credState.State)
		if err != nil {
			return err
		}

		if exists {
			return fmt.Errorf(
				"cannot attach because policy '%s' already attached to '%s'",
				policyId,
				credentialId)
		}

		return client.AppendAfter(
			ctx,
			credState.State.Version(),
			ehcreddomain.NewCredentialPolicyAttached(credentialId, policyId, ehevent.MetaSystemUser(time.Now())))
	})
}

func policyDetach(ctx context.Context, credentialId string, policyId string, logger *log.Logger) error {
	client, err := ehclientfactory.SystemClientFrom(ehclient.ConfigFromEnv, logger)
	if err != nil {
		return err
	}

	credState, err := ehcred.LoadUntilRealtime(ctx, client, logger)
	if err != nil {
		return err
	}

	return credState.Reader.TransactWrite(ctx, func() error {
		exists, err := credentialHasPolicyAttached(credentialId, policyId, credState.State)
		if err != nil {
			return err
		}

		if !exists {
			return fmt.Errorf(
				"cannot detach because policy '%s' not attached to '%s'",
				policyId,
				credentialId)
		}

		return client.AppendAfter(
			ctx,
			credState.State.Version(),
			ehcreddomain.NewCredentialPolicyDetached(credentialId, policyId, ehevent.MetaSystemUser(time.Now())))
	})
}

func policiesEntrypoint() *cobra.Command {
	parentCmd := &cobra.Command{
		Use:   "policy",
		Short: "Policies management",
	}

	parentCmd.AddCommand(&cobra.Command{
		Use:   "ls",
		Short: "List policies",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			rootLogger := logex.StandardLogger()

			osutil.ExitIfError(policiesList(
				osutil.CancelOnInterruptOrTerminate(rootLogger),
				rootLogger))
		},
	})

	parentCmd.AddCommand(&cobra.Command{
		Use:   "cat [id]",
		Short: "Display policy",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			rootLogger := logex.StandardLogger()

			osutil.ExitIfError(policyPrint(
				osutil.CancelOnInterruptOrTerminate(rootLogger),
				args[0],
				rootLogger))
		},
	})

	parentCmd.AddCommand(&cobra.Command{
		Use:   "rm [id]",
		Short: "Delete policy",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			rootLogger := logex.StandardLogger()

			osutil.ExitIfError(policyDelete(
				osutil.CancelOnInterruptOrTerminate(rootLogger),
				args[0],
				rootLogger))
		},
	})

	parentCmd.AddCommand(&cobra.Command{
		Use:   "mv [id] [newName]",
		Short: "Rename policy",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			rootLogger := logex.StandardLogger()

			osutil.ExitIfError(policyRename(
				osutil.CancelOnInterruptOrTerminate(rootLogger),
				args[0],
				args[1],
				rootLogger))
		},
	})

	parentCmd.AddCommand(&cobra.Command{
		Use:   "attach [credentialId] [policyId]",
		Short: "Attach policy to credential",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			rootLogger := logex.StandardLogger()

			osutil.ExitIfError(policyAttach(
				osutil.CancelOnInterruptOrTerminate(rootLogger),
				args[0],
				args[1],
				rootLogger))
		},
	})

	parentCmd.AddCommand(&cobra.Command{
		Use:   "detach [credentialId] [policyId]",
		Short: "Detach policy from credential",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			rootLogger := logex.StandardLogger()

			osutil.ExitIfError(policyDetach(
				osutil.CancelOnInterruptOrTerminate(rootLogger),
				args[0],
				args[1],
				rootLogger))
		},
	})

	return parentCmd
}

func findPolicyById(id string, state *ehcred.Store) (*ehcred.Policy, error) {
	for _, pol := range state.Policies() {
		if pol.Id == id {
			return &pol, nil
		}
	}

	return nil, fmt.Errorf("policy by ID not found: %s", id)
}

func credentialHasPolicyAttached(credentialId string, policyId string, state *ehcred.Store) (bool, error) {
	policyIds, err := state.CredentialAttachedPolicyIds(credentialId)
	if err != nil {
		return false, err
	}

	return sliceutil.ContainsString(policyIds, policyId), nil
}
