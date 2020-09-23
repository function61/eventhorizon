// AWS-inspired policy language
package policy

import (
	"fmt"
	"strings"
)

type StatementEffect string

const (
	Allow StatementEffect = "Allow"
	Deny  StatementEffect = "Deny"
)

type Policy struct {
	Statements []Statement `json:"statements"`
}

type Statement struct {
	Effect    StatementEffect `json:"effect"`
	Actions   []string        `json:"actions"`
	Resources []string        `json:"resources"`
}

// helpers for building policies

func NewAllowStatement(actions []Action, resources ...ResourceName) Statement {
	actionsStr := []string{}
	for _, action := range actions {
		actionsStr = append(actionsStr, action.String())
	}

	resourcesStr := []string{}
	for _, resource := range resources {
		resourcesStr = append(resourcesStr, resource.String())
	}

	return Statement{
		Effect:    Allow,
		Actions:   actionsStr,
		Resources: resourcesStr,
	}
}

// helper for creating a simple policy with just one statement
func NewPolicy(statements ...Statement) Policy {
	return Policy{
		Statements: statements,
	}
}

func HumanReadableStatement(statement Statement) string {
	// "Allow <action> on <resource>"
	return fmt.Sprintf(
		"%s (%s) on (%s)",
		statement.Effect,
		strings.Join(statement.Actions, ", "),
		strings.Join(statement.Resources, ", "))
}

func Merge(pol ...Policy) Policy {
	result := Policy{}
	for _, p := range pol {
		result.Statements = append(result.Statements, p.Statements...)
	}

	return result
}
