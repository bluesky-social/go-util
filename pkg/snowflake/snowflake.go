package snowflake

import (
	"sync"
	"time"
)

const BlueskyOspreyEpoch int64 = 1758346154000

const (
	machineBits  uint = 10
	seqBits      uint = 12
	machineMask       = (1 << machineBits) - 1
	seqMask           = (1 << seqBits) - 1
	seqShift          = 0
	machineShift      = seqBits
	timeShift         = machineBits + seqBits
)

type Generator struct {
	mu        sync.Mutex
	epoch     int64
	machineID uint64
	lastMs    int64
	seq       uint64
}

func New(epoch int64, machineID uint16) *Generator {
	return &Generator{
		epoch:     epoch,
		machineID: uint64(machineID) & machineMask,
	}
}

func (g *Generator) Next() uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()

	for {
		now := time.Now().UnixMilli()
		if now < g.lastMs {
			time.Sleep(time.Duration(g.lastMs-now) * time.Millisecond)
			continue
		}
		if now == g.lastMs {
			g.seq = (g.seq + 1) & seqMask
			if g.seq == 0 {
				for now <= g.lastMs {
					now = time.Now().UnixMilli()
				}
			}
		} else {
			g.seq = 0
		}
		g.lastMs = now

		tsPart := (uint64(now-g.epoch) << timeShift)
		mcPart := (g.machineID & machineMask) << machineShift
		seqPart := (g.seq & seqMask) << seqShift
		return tsPart | mcPart | seqPart
	}
}
