package main

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/left2right/codis/pkg/models"
	log "github.com/ngaut/logging"
	"hash/crc32"
	"strconv"
	"time"
)

type duraSlice []time.Duration

type outElem struct {
	key     string
	latency time.Duration
	proxy   string
	server  string
	slot    int
}

type cmdLatency struct {
	groups  []models.ServerGroup
	proxys  []models.ProxyInfo
	slots   []models.Slot
	outList []outElem
}

func (cmd *cmdLatency) Main() {
	groups, err := GetServerGroups()
	cmd.groups = groups
	if err != nil {
		log.Error(errors.ErrorStack(err))
		return
	}
	proxys, err := GetProxyList()
	cmd.proxys = proxys
	if err != nil {
		log.Error(errors.ErrorStack(err))
		return
	}
	slots, err := GetSlotList()
	cmd.slots = slots
	if err != nil {
		log.Error(errors.ErrorStack(err))
		return
	}
	cmd.CheckAllProxyLatency(proxys)
	cmd.OutputLatency()
}

func (cmd *cmdLatency) CheckAllProxyLatency(proxys []models.ProxyInfo) {
	for _, proxy := range proxys {
		cmd.CheckProxyLatency(proxy)
	}
}

func (cmd *cmdLatency) CheckProxyLatency(proxy models.ProxyInfo) {
	rc := acf(proxy.Addr, 3*time.Second)
	for i := 0; i < 2048; i++ {
		var out outElem
		out.key = "codis:test:" + strconv.Itoa(i)
		out.slot = int(HashSlot(out.key))
		out.proxy = proxy.Addr
		out.server = cmd.GetSlotServer(out.slot)
		out.latency = rc.SetLatency(out.key, 100)
		cmd.outList = append(cmd.outList, out)
	}
}

func (cmd *cmdLatency) GetSlotServer(slot int) string {
	for _, s := range cmd.slots {
		if slot == s.Id {
			for _, g := range cmd.groups {
				if s.GroupId == g.Id {
					for _, r := range g.Servers {
						if r.Type == "master" {
							return r.Addr
						} else {
							continue
						}
					}
				} else {
					continue
				}
			}
		} else {
			continue
		}
	}
	log.Errorf("slot not found in range 0~1023: %d", slot)
	return "Error Get Server"
}

func (cmd *cmdLatency) OutputLatency() {
	var total time.Duration
	var count int
	latencyMap := make(map[string]duraSlice)
	for _, out := range cmd.outList {
		total += out.latency
		count++
		latencyMap[out.proxy] = append(latencyMap[out.proxy], out.latency)
		latencyMap[out.server] = append(latencyMap[out.server], out.latency)
		if !args.quiet {
			fmt.Printf("Latency:%q; Proxy:%s; Server:%s; Slot:%d; Key:%s\n", out.latency, out.proxy, out.server, out.slot, out.key)
		}
	}

	cmd.outputServersStatus(latencyMap)

	//fmt.Printf("Codis latency: %f ms\n", float32(total/time.Duration(count))/1000000)
}

func (cmd *cmdLatency) outputServersStatus(latencyMap map[string]duraSlice) {
	var status string
	var serverDown bool = false
	for server, latencys := range latencyMap {
		var lsum time.Duration
		if cmd.IsServer(server) {
			for _, l := range latencys {
				lsum += l
			}
			var average float32 = float32(lsum/time.Duration(len(latencys))) / 1000000
			if average > 2800 {
				status = "down"
				serverDown = true
			} else if average > 30 {
				status = "slow"
			} else {
				status = "normal"
			}

			fmt.Printf("Server %s latency: %f ms and status %s\n", server, average, status)
		} else {
			continue
		}
	}

	for server, latencys := range latencyMap {
		var lsum time.Duration
		if cmd.IsServer(server) {
			continue
		} else {
			var count int = 0
			for _, l := range latencys {
				lsum += l
				if l > 2*time.Second {
					count++
				}
			}
			var average float32 = float32(lsum/time.Duration(len(latencys))) / 1000000
			if average > 2800 {
				status = "down"
				serverDown = true
			} else if average > 30 {
				status = "slow"
			} else {
				status = "normal"
			}

			if count > 4 {
				status = "down"
			}

			if serverDown {
				status = "normal"
			}

			fmt.Printf("Server %s latency: %f ms and status %s\n", server, average, status)
		}
	}
}

func (cmd *cmdLatency) IsServer(server string) bool {
	var isServer bool = true
	for _, proxy := range cmd.proxys {
		if server == proxy.Addr {
			isServer = false
		}
	}
	return isServer
}

func HashSlot(s string) uint32 {
	return crc32.ChecksumIEEE([]byte(s)) % 1024
}
