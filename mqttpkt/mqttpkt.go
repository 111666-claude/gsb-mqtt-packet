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
	mult := 1
	i := pos + 1
	for {
		if i >= len(data) {
			return nil, pos, errors.New("short")
		}
		d := data[i]
		length += int(d&0x7f) * mult
		i++
		if d&0x80 == 0 {
			break
		}
		mult *= 128
		if mult > 128*128*128 {
			return nil, pos, errors.New("malformed")
		}
	}
	start := i
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

// trieNode 是按主题段建立的订阅索引。
type trieNode struct {
	children map[string]*trieNode
	plus     *trieNode
	hash     []Sub
	subs     []Sub
}

func (n *trieNode) insert(segments []string, s Sub) {
	if len(segments) == 0 {
		n.subs = append(n.subs, s)
		return
	}
	seg := segments[0]
	if seg == "#" {
		n.hash = append(n.hash, s)
		return
	}
	if seg == "+" {
		if n.plus == nil {
			n.plus = &trieNode{}
		}
		n.plus.insert(segments[1:], s)
		return
	}
	if n.children == nil {
		n.children = map[string]*trieNode{}
	}
	child := n.children[seg]
	if child == nil {
		child = &trieNode{}
		n.children[seg] = child
	}
	child.insert(segments[1:], s)
}

func (n *trieNode) match(segments []string, i int, c *Counter, hit func(Sub)) {
	c.Scanned++
	for _, s := range n.hash {
		hit(s)
	}
	if i == len(segments) {
		for _, s := range n.subs {
			hit(s)
		}
		return
	}
	if child := n.children[segments[i]]; child != nil {
		child.match(segments, i+1, c, hit)
	}
	if n.plus != nil {
		n.plus.match(segments, i+1, c, hit)
	}
}

// Broker 保存订阅并按主题分发。
type Broker struct {
	subs     []Sub
	root     *trieNode
	inflight map[string]bool
}

// NewBroker 建一个分发器。
func NewBroker() *Broker {
	return &Broker{root: &trieNode{}, inflight: map[string]bool{}}
}

// Subscribe 记一条订阅。
func (b *Broker) Subscribe(client, filter string, qos byte) {
	s := Sub{Client: client, Filter: filter, QoS: qos}
	b.subs = append(b.subs, s)
	b.root.insert(strings.Split(filter, "/"), s)
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
	b.root.match(strings.Split(p.Topic, "/"), 0, c, func(s Sub) {
		qos := p.QoS
		if s.QoS < qos {
			qos = s.QoS
		}
		out = append(out, Delivery{Client: s.Client, Topic: p.Topic, QoS: qos})
	})
	return out
}
