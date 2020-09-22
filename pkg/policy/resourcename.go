package policy

var (
	F61rn = ResourceName("f61rn")
)

type ResourceName string

func (r ResourceName) Child(name string) ResourceName {
	return ResourceName(string(r) + ":" + name)
}

func (r ResourceName) String() string {
	return string(r)
}
