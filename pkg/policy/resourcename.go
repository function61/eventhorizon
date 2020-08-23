package policy

var (
	F61rn = ResourceName{"f61rn"}
)

type ResourceName struct {
	name string
}

func (r ResourceName) Child(name string) ResourceName {
	return ResourceName{r.name + ":" + name}
}

func (r ResourceName) String() string {
	return r.name
}
