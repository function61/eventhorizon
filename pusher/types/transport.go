package types

type Transport interface {
	Push(*PushInput) (*PushOutput, error)
}
