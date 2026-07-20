package contract

type Platform struct {
	os   string
	arch string
}

func (p Platform) String() string {
	return p.os + "/" + p.arch
}

type CPUProfile struct {
	Vendor string
	Model  string
	Family string
	Flags  string

	Mhz   float64
	Cores int
}

type StorageMediumProfile struct {
	Capacity  int
	Available int
}

type StorageProfile []StorageMediumProfile

type HostProfile struct {
	Name      string
	Platform  Platform
	OsFamily  string
	OsVersion string

	VirtualizationSystem string
	VirtualizationRole   string

	CPU            CPUProfile
	MemoryCapacity int // in bytes

	Storage StorageProfile
}
