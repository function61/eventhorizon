package policy

import (
	"fmt"
	"strings"
)

type Action struct {
	name string
}

func NewAction(name string) Action {
	return Action{name}
}

func (a Action) String() string {
	return a.name
}

func (p *Policy) Authorize(action Action, resource ResourceName) error {
	actionRaw := action.String()
	resourceRaw := resource.String()

	for _, statement := range p.Statements {
		if statement.matches(actionRaw, resourceRaw) {
			if statement.Effect == Allow {
				return nil
			} else {
				return fmt.Errorf("%s explicitly denied to %s", actionRaw, resourceRaw)
			}
		}
	}

	return fmt.Errorf("%s implicitly denied to %s", actionRaw, resourceRaw)
}

func (p *Statement) matches(action string, resource string) bool {
	for _, candidateAction := range p.Actions {
		// TODO: wildcard support for actions?
		if candidateAction != action {
			continue
		}

		for _, candidateResource := range p.Resources {
			if stringMaybeWildcardEquals(resource, candidateResource) {
				return true
			}
		}
	}

	return false
}

// the "subject" cannot contain wildcards
func stringMaybeWildcardEquals(subject string, maybePattern string) bool {
	if !strings.Contains(maybePattern, "*") { // fast path
		return subject == maybePattern
	}

	// maybePattern now definitely a pattern
	return glob(maybePattern, subject)
}
