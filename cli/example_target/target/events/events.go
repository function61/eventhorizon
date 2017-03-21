package events

import (
	"github.com/function61/pyramid/cli/example_target/target/transaction"
)

var EventNameToApplyFn = map[string]func(*transaction.Tx, string) error{
	"CompanyCreated":  applyCompanyCreated,
	"UserCreated":     applyUserCreated,
	"UserNameChanged": applyUserNameChanged,
}
