package rocketmq

import (
	"net"
	"strings"
)

// Get local IPV4 Address
func GetLocalIp4() (ip string) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return
	}

	for _, face := range interfaces {
		if strings.Contains(face.Name, "lo") {
			continue
		}
		addrs, err := face.Addrs()
		if err != nil {
			return
		}
		for _, addr := range addrs {
			if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
				if ipnet.IP.To4() != nil {
					currIp := ipnet.IP.String()
					if !strings.Contains(currIp, ":") && currIp != "127.0.0.1" && isIntranetIpv4(currIp) {
						ip = currIp
					}
				}
			}
		}
	}

	return
}

func isIntranetIpv4(ip string) bool {
	if strings.HasPrefix(ip, "192.168.") || strings.HasPrefix(ip, "169.254.") {
		return true
	}
	return false
}
