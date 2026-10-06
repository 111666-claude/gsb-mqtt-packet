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
	length := 0
	shift := uint(0)
	start := pos + 1
	for {
		if start >= len(data) {
			return nil, pos, errors.New("short")
		}
		d := data[start]
		start++
		length |= int(d&0x7f) << shift
		if d&0x80 == 0 {
			break
		}
		shift += 7
		if shift >= 28 {
			return nil, pos, errors.New("malformed")
		}
	}
	if start+length > len(data) {
		return nil, pos, errors.New("short")
	}
	body := data[start : start+length]
	p := &Packet{Type: ptype, Flags: flags}
	if ptype == 3 {
		if len(body) < 2 {
			return nil, pos, errors.New("short")
		}
		p.QoS = (flags >> 1) & 0x3
		tl := int(binary.BigEndian.Uint16(body))
		if 2+tl > len(body) {
			return nil, pos, errors.New("short")
		}
		p.Topic = string(body[2 : 2+tl])
		rest := body[2+tl:]
		if p.QoS > 0 {
			if len(rest) < 2 {
				return nil, pos, errors.New("short")
			}
			p.PacketID = int(binary.BigEndian.Uint16(rest))
			rest = rest[2:]
		}
		p.Payload = rest
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
	root     *subNode
	inflight map[string]bool
}

// subNode 是订阅索引里的一个层级节点。
type subNode struct {
	children map[string]*subNode
	plus     *subNode
	hash     []Sub
	subs     []Sub
}

// NewBroker 建一个分发器。
func NewBroker() *Broker {
	return &Broker{root: &subNode{}, inflight: map[string]bool{}}
}

// Subscribe 记一条订阅。
func (b *Broker) Subscribe(client, filter string, qos byte) {
	s := Sub{Client: client, Filter: filter, QoS: qos}
	b.subs = append(b.subs, s)
	n := b.root
	for _, seg := range strings.Split(filter, "/") {
		switch seg {
		case "#":
			n.hash = append(n.hash, s)
			return
		case "+":
			if n.plus == nil {
				n.plus = &subNode{}
			}
			n = n.plus
		default:
			if n.children == nil {
				n.children = map[string]*subNode{}
			}
			next, ok := n.children[seg]
			if !ok {
				next = &subNode{}
				n.children[seg] = next
			}
			n = next
		}
	}
	n.subs = append(n.subs, s)
}

func matchFilter(filter, topic string) bool {
	a := strings.Split(filter, "/")
	z := strings.Split(topic, "/")
	for i, seg := range a {
		if seg == "#" {
			return i == len(a)-1
		}
		if i >= len(z) {
			return false
		}
		if seg != "+" && seg != z[i] {
			return false
		}
	}
	return len(a) == len(z)
}

// Publish 把一条报文分发给匹配的订阅。
func (b *Broker) Publish(client string, p *Packet, c *Counter) []Delivery {
	if p.QoS > 0 {
		key := client + "#" + strconv.Itoa(p.PacketID)
		if b.inflight[key] {
			return nil
		}
		b.inflight[key] = true
	}
	var out []Delivery
	levels := strings.Split(p.Topic, "/")
	var walk func(n *subNode, i int)
	walk = func(n *subNode, i int) {
		c.Scanned++
		for _, s := range n.hash {
			out = append(out, Delivery{Client: s.Client, Topic: p.Topic, QoS: min(p.QoS, s.QoS)})
		}
		if i == len(levels) {
			for _, s := range n.subs {
				out = append(out, Delivery{Client: s.Client, Topic: p.Topic, QoS: min(p.QoS, s.QoS)})
			}
			return
		}
		if next, ok := n.children[levels[i]]; ok {
			walk(next, i+1)
		}
		if n.plus != nil {
			walk(n.plus, i+1)
		}
	}
	walk(b.root, 0)
	return out
}
