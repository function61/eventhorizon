package types

type DiscoveryFile struct {
	WriterIp      string `json:"writer_ip"`
	AuthToken     string `json:"auth_token"`
	CaCertificate string `json:"ca_certificate"`
	CaPrivateKey  string `json:"ca_private_key"`
}
