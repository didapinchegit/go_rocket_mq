package rocketmq

import (
	"bytes"
)

type permName struct {
	PermPriority int
	PermRead     int
	PermWrite    int
	PermInherit  int
}

var PermName = permName{
	PermPriority: 0x1 << 3,
	PermRead:     0x1 << 2,
	PermWrite:    0x1 << 1,
	PermInherit:  0x1 << 0,
}

func (p permName) perm2String(perm int) string {
	stringBuffer := bytes.NewBuffer([]byte{})
	if PermName.isReadable(perm) {
		stringBuffer.WriteString("R")
	} else {
		stringBuffer.WriteString("-")
	}
	if PermName.isWritable(perm) {
		stringBuffer.WriteString("W")
	} else {
		stringBuffer.WriteString("-")
	}
	if PermName.isInherited(perm) {
		stringBuffer.WriteString("X")
	} else {
		stringBuffer.WriteString("-")
	}

	return stringBuffer.String()
}

func (p permName) isReadable(perm int) bool {
	return (perm & p.PermRead) == p.PermRead
}

func (p permName) isWritable(perm int) bool {
	return (perm & p.PermWrite) == p.PermWrite
}

func (p permName) isInherited(perm int) bool {
	return (perm & p.PermInherit) == p.PermInherit
}
