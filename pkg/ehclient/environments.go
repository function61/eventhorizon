package ehclient

type Environment struct {
	events   string
	regionId string
}

func Production() Environment {
	return Environment{"prod_eh_events", "eu-central-1"}
}

func Development() Environment {
	return Environment{"dev_eh_events", "eu-central-1"}
}
