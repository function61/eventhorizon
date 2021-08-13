package policy

var (
	F61 = ResourceName("f61")
)

type ResourceName string

func (r ResourceName) Child(name string) ResourceName {
	return ResourceName(string(r) + ":" + name)
}

func (r ResourceName) String() string {
	return string(r)
}
