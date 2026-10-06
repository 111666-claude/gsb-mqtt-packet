// Package mqttpkt 解析 MQTT 报文并按订阅分发。
package mqttpkt

import (
	"encoding/binary"
	"errors"
	"strconv"
	"strings"
)

// Counter 记录比较次数。
type Counter struct {
	Scanned int
}

// Packet 是一个控制报文。
type Packet struct {
	Type     byte
	Flags    byte
	QoS      byte
	Topic    string
	PacketID int
	Payload  []byte
}

func decodeOne(data []byte, pos int, c *Counter) (*Packet, int, error) {
	c.Scanned++
	if pos+2 > len(data) {
		return nil, pos, errors.New("short")
	}
	b0 := data[pos]
	ptype := b0 >> 4
	flags := b0 & 0x0f
	length := int(data[pos+1])
	start := pos + 2
	body := data[start:]
	p := &Packet{Type: ptype, Flags: flags}
	if ptype == 3 {
		if len(body) < 2 {
			return nil, pos, errors.New("short")
		}
		p.QoS = (flags >> 1) & 0x3
		tl := int(binary.BigEndian.Uint16(body))
		if 2+tl > len(body) {
			tl = len(body) - 2
		}
		p.Topic = string(body[2 : 2+tl])
	}
	return p, start + length, nil
}

// Stream 是一串拼接的报文。
type Stream struct {
	data    []byte
	offsets []int
}

// NewStream 建一个报文流。
func NewStream(data []byte, c *Counter) *Stream {
	s := &Stream{data: data}
	pos := 0
	for pos < len(data) {
		_, next, err := decodeOne(data, pos, c)
		if err != nil || next <= pos {
			break
		}
		s.offsets = append(s.offsets, pos)
		pos = next
	}
	return s
}

// At 取第 i 个报文。
func (s *Stream) At(i int, c *Counter) (*Packet, error) {
	if i < 0 || i >= len(s.offsets) {
		return nil, errors.New("out-of-range")
	}
	p, _, err := decodeOne(s.data, s.offsets[i], c)
	return p, err
}

// Len 返回报文数。
func (s *Stream) Len() int {
	return len(s.offsets)
}

// Sub 是一条订阅。
type Sub struct {
	Client string
	Filter string
	QoS    byte
}

// Delivery 是一次投递。
type Delivery struct {
	Client string
	Topic  string
	QoS    byte
}

// Broker 保存订阅并按主题分发。
type Broker struct {
	subs     []Sub
	inflight map[string]bool
}

// NewBroker 建一个分发器。
func NewBroker() *Broker {
	return &Broker{inflight: map[string]bool{}}
}

// Subscribe 记一条订阅。
func (b *Broker) Subscribe(client, filter string, qos byte) {
	b.subs = append(b.subs, Sub{Client: client, Filter: filter, QoS: qos})
}

func matchFilter(filter, topic string) bool {
	a := strings.Split(filter, "/")
	z := strings.Split(topic, "/")
	if len(a) != len(z) {
		return false
	}
	for i := range a {
		if a[i] != z[i] {
			return false
		}
	}
	return true
}

// Publish 把一条报文分发给匹配的订阅。
func (b *Broker) Publish(client string, p *Packet, c *Counter) []Delivery {
	var out []Delivery
	if p.QoS > 0 {
		b.inflight[client+"#"+strconv.Itoa(p.PacketID)] = true
	}
	for _, s := range b.subs {
		c.Scanned++
		if matchFilter(s.Filter, p.Topic) {
			out = append(out, Delivery{Client: s.Client, Topic: p.Topic, QoS: p.QoS})
		}
	}
	return out
}
