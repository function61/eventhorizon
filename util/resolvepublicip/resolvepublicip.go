package resolvepublicip

import (
	"io/ioutil"
	"net/http"
)

const ServiceDisplayName = "ipify.org"

func Resolve() (ip_ string, err error) {
	res, getErr := http.Get("https://api.ipify.org/")
	if getErr != nil {
		return "", getErr
	}

	ip, readErr := ioutil.ReadAll(res.Body)
	if readErr != nil {
		return "", readErr
	}

	return string(ip), nil
}
