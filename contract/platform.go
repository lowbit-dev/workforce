package contract

type Platform struct {
	os   string
	arch string
}

func (p Platform) String() string {
	return p.os + "/" + p.arch
}
