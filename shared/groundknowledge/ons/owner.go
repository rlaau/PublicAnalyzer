package ons

type Owner interface {
	ONSInfo() OwnerInONSInfo
}
type OwnerInONSInfo struct {
	ModuleName ModuleName
	OwnerName  OwnerName
	ownerDescs string
}
