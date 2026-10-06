package mqttpkt

import "fmt"

// Scenario 是一个固定场景。
type Scenario struct {
	Name string
	Run  func() map[string]any
}

func publish(topic string, qos byte, packetID int, payload []byte) []byte {
	body := []byte{byte(len(topic) >> 8), byte(len(topic))}
	body = append(body, topic...)
	if qos > 0 {
		body = append(body, byte(packetID>>8), byte(packetID))
	}
	body = append(body, payload...)
	out := []byte{0x30 | (qos << 1)}
	n := len(body)
	for {
		d := byte(n % 128)
		n /= 128
		if n > 0 {
			out = append(out, d|0x80)
		} else {
			out = append(out, d)
			break
		}
	}
	return append(out, body...)
}

// EncodePublish 供测试与场景使用。
func EncodePublish(topic string, qos byte, packetID int, payload []byte) []byte {
	return publish(topic, qos, packetID, payload)
}

// Samples 返回全部场景。
func Samples() []Scenario {
	return []Scenario{
		{Name: "varlen", Run: func() map[string]any {
			s := NewStream(publish("t", 0, 0, make([]byte, 200)), &Counter{})
			p, err := s.At(0, &Counter{})
			if err != nil {
				return map[string]any{"error": err.Error()}
			}
			return map[string]any{"payload": len(p.Payload)}
		}},
		{Name: "wildcard", Run: func() map[string]any {
			b := NewBroker()
			b.Subscribe("c1", "a/+/c", 0)
			s := NewStream(publish("a/b/c", 0, 0, []byte("x")), &Counter{})
			p, _ := s.At(0, &Counter{})
			return map[string]any{"delivered": len(b.Publish("pub", p, &Counter{}))}
		}},
		{Name: "hash", Run: func() map[string]any {
			b := NewBroker()
			b.Subscribe("c1", "a/#", 0)
			s := NewStream(publish("a", 0, 0, []byte("x")), &Counter{})
			p, _ := s.At(0, &Counter{})
			return map[string]any{"delivered": len(b.Publish("pub", p, &Counter{}))}
		}},
		{Name: "qos", Run: func() map[string]any {
			b := NewBroker()
			b.Subscribe("c1", "t", 0)
			s := NewStream(publish("t", 2, 7, []byte("x")), &Counter{})
			p, _ := s.At(0, &Counter{})
			d := b.Publish("pub", p, &Counter{})
			if len(d) == 0 {
				return map[string]any{"qos": -1}
			}
			return map[string]any{"qos": d[0].QoS}
		}},
		{Name: "dup", Run: func() map[string]any {
			b := NewBroker()
			b.Subscribe("c1", "t", 1)
			s := NewStream(append(publish("t", 1, 5, []byte("x")), publish("t", 1, 5, []byte("y"))...), &Counter{})
			total := 0
			for i := 0; i < s.Len(); i++ {
				p, _ := s.At(i, &Counter{})
				total += len(b.Publish("pub", p, &Counter{}))
			}
			return map[string]any{"delivered": total}
		}},
	}
}

// Work 跑规模线场景。
func Work(n int) map[string]any {
	b := NewBroker()
	for i := 0; i < n; i++ {
		b.Subscribe("c1", fmt.Sprintf("t%04d", i), 0)
	}
	c := &Counter{}
	for i := 0; i < n; i++ {
		s := NewStream(publish(fmt.Sprintf("t%04d", i), 0, 0, []byte("x")), &Counter{})
		p, _ := s.At(0, &Counter{})
		b.Publish("pub", p, c)
	}
	return map[string]any{"subs": n, "scanned": c.Scanned}
}
