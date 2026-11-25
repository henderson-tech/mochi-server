// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2023 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co

package mqtt

import (
	"sort"
	"sync"
	"sync/atomic"

	"github.com/mochi-mqtt/server/v2/packets"
)

// Inflight is a map of InflightMessage keyed on packet id.
type Inflight struct {
	sync.RWMutex
	internal            map[uint16]packets.Packet // internal contains the inflight packets
	held                map[uint16]struct{}       // ids of outbound packets waiting for send quota, see hold
	receiveQuota        int32                     // remaining inbound qos quota for flow control
	sendQuota           int32                     // remaining outbound qos quota for flow control
	maximumReceiveQuota int32                     // maximum allowed receive quota
	maximumSendQuota    int32                     // maximum allowed send quota
}

// NewInflights returns a new instance of an Inflight packets map.
func NewInflights() *Inflight {
	return &Inflight{
		internal: map[uint16]packets.Packet{},
		held:     map[uint16]struct{}{},
	}
}

// Set adds or updates an inflight packet by packet id.
func (i *Inflight) Set(m packets.Packet) bool {
	i.Lock()
	defer i.Unlock()

	_, ok := i.internal[m.PacketID]
	i.internal[m.PacketID] = m
	delete(i.held, m.PacketID) // a packet set under an id starts out not held
	return !ok
}

// hold marks the packet under id as waiting for send quota: it has not been
// written, and goes out through NextImmediate once quota frees.
//
// The mark used to be Expiry -1, which overwrote the packet's absolute
// expiry: a held delivery then never expired on its own deadline and went
// out with no Message Expiry Interval. Kept beside the packet, the mark
// leaves the expiry intact.
func (i *Inflight) hold(id uint16) {
	i.Lock()
	defer i.Unlock()

	if _, ok := i.internal[id]; !ok {
		return
	}
	if i.held == nil {
		i.held = map[uint16]struct{}{}
	}
	i.held[id] = struct{}{}
}

// release clears the hold on the packet under id once it has been written:
// it is then in flight like any other delivery.
func (i *Inflight) release(id uint16) {
	i.Lock()
	defer i.Unlock()
	delete(i.held, id)
}

// isHeld reports whether the packet under id waits for send quota.
func (i *Inflight) isHeld(id uint16) bool {
	i.RLock()
	defer i.RUnlock()
	_, ok := i.held[id]
	return ok
}

// Get returns an inflight packet by packet id.
func (i *Inflight) Get(id uint16) (packets.Packet, bool) {
	i.RLock()
	defer i.RUnlock()

	if m, ok := i.internal[id]; ok {
		return m, true
	}

	return packets.Packet{}, false
}

// Len returns the size of the inflight messages map.
func (i *Inflight) Len() int {
	i.RLock()
	defer i.RUnlock()
	return len(i.internal)
}

// Clone returns a new instance of Inflight with the same message data.
// This is used when transferring inflights from a taken-over session.
func (i *Inflight) Clone() *Inflight {
	c := NewInflights()
	i.RLock()
	defer i.RUnlock()
	for k, v := range i.internal {
		c.internal[k] = v
	}
	for k := range i.held {
		c.held[k] = struct{}{}
	}
	return c
}

// GetAll returns all the inflight messages.
func (i *Inflight) GetAll(immediate bool) []packets.Packet {
	i.RLock()
	defer i.RUnlock()

	m := []packets.Packet{}
	for k, v := range i.internal {
		if _, held := i.held[k]; !immediate || held {
			m = append(m, v)
		}
	}

	// MQTT-4.6.0-1 requires that re-sent PUBLISH packets go out in the order
	// the originals were sent, so this comparison has to be a total order.
	// Created alone is not one: it is a unix timestamp in seconds, so every
	// packet made in the same second compares equal, and sort.Slice is not
	// stable — a tie is broken by map iteration order, which Go randomises.
	// Under any real publish rate most of a session's window is one second
	// wide.
	//
	// The packet identifier breaks the tie. It is allocated in increasing
	// order, so within a second it is the order the packets were sent.
	sort.Slice(m, func(i, j int) bool {
		if m[i].Created != m[j].Created {
			return m[i].Created < m[j].Created
		}
		return m[i].PacketID < m[j].PacketID
	})

	return m
}

// NextImmediate returns the next inflight packet which is indicated to be sent immediately.
// This typically occurs when the quota has been exhausted, and we need to wait until new quota
// is free to continue sending.
func (i *Inflight) NextImmediate() (packets.Packet, bool) {
	// No lock is taken here: GetAll takes the read lock itself, and
	// sync.RWMutex is not reentrant for readers. Holding it across that call
	// deadlocks whenever a writer arrives between the two acquisitions — the
	// second RLock waits for the queued writer, and the writer waits for the
	// first RLock.
	m := i.GetAll(true)
	if len(m) > 0 {
		return m[0], true
	}

	return packets.Packet{}, false
}

// Delete removes an in-flight message from the map. Returns true if the message existed.
func (i *Inflight) Delete(id uint16) bool {
	i.Lock()
	defer i.Unlock()

	_, ok := i.internal[id]
	delete(i.internal, id)
	delete(i.held, id)

	return ok
}

// TakeRecieveQuota reduces the receive quota by 1.
func (i *Inflight) DecreaseReceiveQuota() {
	if atomic.LoadInt32(&i.receiveQuota) > 0 {
		atomic.AddInt32(&i.receiveQuota, -1)
	}
}

// TakeRecieveQuota increases the receive quota by 1.
func (i *Inflight) IncreaseReceiveQuota() {
	if atomic.LoadInt32(&i.receiveQuota) < atomic.LoadInt32(&i.maximumReceiveQuota) {
		atomic.AddInt32(&i.receiveQuota, 1)
	}
}

// ResetReceiveQuota resets the receive quota to the maximum allowed value.
func (i *Inflight) ResetReceiveQuota(n int32) {
	atomic.StoreInt32(&i.receiveQuota, n)
	atomic.StoreInt32(&i.maximumReceiveQuota, n)
}

// DecreaseSendQuota reduces the send quota by 1.
func (i *Inflight) DecreaseSendQuota() {
	if atomic.LoadInt32(&i.sendQuota) > 0 {
		atomic.AddInt32(&i.sendQuota, -1)
	}
}

// IncreaseSendQuota increases the send quota by 1.
func (i *Inflight) IncreaseSendQuota() {
	if atomic.LoadInt32(&i.sendQuota) < atomic.LoadInt32(&i.maximumSendQuota) {
		atomic.AddInt32(&i.sendQuota, 1)
	}
}

// ResetSendQuota resets the send quota to the maximum allowed value.
func (i *Inflight) ResetSendQuota(n int32) {
	atomic.StoreInt32(&i.sendQuota, n)
	atomic.StoreInt32(&i.maximumSendQuota, n)
}
