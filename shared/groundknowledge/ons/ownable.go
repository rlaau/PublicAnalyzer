package ons

type Ownalbe interface {
	SetOwner(o OwnerName)
	GetOwnerName() OwnerName
	GetStatsByOwner() string
}
