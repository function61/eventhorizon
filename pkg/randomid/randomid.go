// Generates random IDs
package randomid

import (
	"github.com/function61/gokit/crypto/cryptoutil"
)

// 0.1 % collision probability for a collection with 16 k records
// Example: ZE-1
func Shorter() string {
	return cryptoutil.RandBase64UrlWithoutLeadingDash(3)
}

// 0.1 % collision probability for a collection with 4 M records
// Example: f5Vr0A
func Short() string {
	return cryptoutil.RandBase64UrlWithoutLeadingDash(4)
}

// 0.1 % collision probability for a collection with 18446744 billion records
// Example: w0dp0YmBXnE
func Long() string {
	return cryptoutil.RandBase64UrlWithoutLeadingDash(8)
}

// suitable for preventing online attacks
// Example: IPrqwqmuKJRQjnNUxp7CDQ
func AlmostCryptoLong() string {
	return cryptoutil.RandBase64UrlWithoutLeadingDash(16)
}

// suitable for preventing offline attacks
// Example: DnC51zHFiog1U2XainefYWPYFfTAeGi325bYeiqWdmI
func CryptoLong() string {
	return cryptoutil.RandBase64UrlWithoutLeadingDash(32)
}
