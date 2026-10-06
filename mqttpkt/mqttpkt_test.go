package mqttpkt

import "testing"

func TestStreamLen(t *testing.T) {
	data := append(publish("t", 0, 0, []byte("x")), publish("u", 0, 0, []byte("y"))...)
	if got := NewStream(data, &Counter{}).Len(); got != 2 {
		t.Fatalf("应有 2 个报文：%d", got)
	}
}

func TestAtInRange(t *testing.T) {
	s := NewStream(publish("t", 0, 0, []byte("x")), &Counter{})
	if _, err := s.At(0, &Counter{}); err != nil {
		t.Fatalf("取第 0 个不该报错：%v", err)
	}
}

func TestAtOutOfRange(t *testing.T) {
	s := NewStream(publish("t", 0, 0, []byte("x")), &Counter{})
	if _, err := s.At(5, &Counter{}); err == nil {
		t.Fatal("越界应报错")
	}
}

func TestBrokerStartsEmpty(t *testing.T) {
	if len(NewBroker().subs) != 0 {
		t.Fatal("新分发器不该有订阅")
	}
}

func TestSubscribeCount(t *testing.T) {
	b := NewBroker()
	b.Subscribe("c1", "t", 0)
	if len(b.subs) != 1 {
		t.Fatalf("应有一条订阅：%d", len(b.subs))
	}
}

func TestCounterCounts(t *testing.T) {
	b := NewBroker()
	b.Subscribe("c1", "t", 0)
	s := NewStream(publish("t", 0, 0, []byte("x")), &Counter{})
	p, _ := s.At(0, &Counter{})
	c := &Counter{}
	b.Publish("pub", p, c)
	if c.Scanned == 0 {
		t.Fatal("应统计比较次数")
	}
}
