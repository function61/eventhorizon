package metaevents

// .{"_":"AuthorityChange","peers":["127.0.0.1"]}
type AuthorityChanged struct {
	Type  string   `json:"_"`
	Peers []string `json:"peers"`
}

func NewAuthorityChanged(peers []string) *AuthorityChanged {
	return &AuthorityChanged{
		Type:  "AuthorityChanged",
		Peers: peers,
	}
}
