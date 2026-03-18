package stream

import (
	"encoding/json"
	"log"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/xiezhayang/Carve/datamanager"
)

// RowPayload 与 CSV 行一致：ts, metric, value, scope
type RowPayload struct {
	Ts     int64   `json:"ts"`
	Metric string  `json:"metric"`
	Value  float64 `json:"value"`
	Scope  string  `json:"scope"`
}

type Hub struct {
	mu    sync.RWMutex
	conns map[string]map[*websocket.Conn]struct{} // target -> conn set
}

func NewHub() *Hub {
	return &Hub{
		mu:    sync.RWMutex{},
		conns: make(map[string]map[*websocket.Conn]struct{})}
}

func (h *Hub) Subscribe(target string, conn *websocket.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.conns[target] == nil {
		h.conns[target] = make(map[*websocket.Conn]struct{})
	}
	h.conns[target][conn] = struct{}{}
}

func (h *Hub) Unsubscribe(target string, conn *websocket.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if m := h.conns[target]; m != nil {
		delete(m, conn)
		if len(m) == 0 {
			delete(h.conns, target)
		}
	}
	_ = conn.Close()
}

// Broadcast 将 rows 按「写 CSV 同构」的格式发给该 target 的所有连接
func (h *Hub) Broadcast(target string, rows []datamanager.Row) {
	if len(rows) == 0 {
		return
	}
	payload := make([]RowPayload, len(rows))
	for i, r := range rows {
		payload[i] = RowPayload{Ts: r.TsMs, Metric: r.Metric, Value: r.Value, Scope: r.ScopeName}
	}
	body, err := json.Marshal(payload)
	if err != nil {
		log.Printf("[stream] marshal error: %v", err)
		return
	}

	h.mu.RLock()
	conns := make([]*websocket.Conn, 0, len(h.conns[target]))
	for c := range h.conns[target] {
		conns = append(conns, c)
	}
	h.mu.RUnlock()

	for _, conn := range conns {
		if err := conn.WriteMessage(websocket.TextMessage, body); err != nil {
			log.Printf("[stream] write error: %v", err)
			h.Unsubscribe(target, conn)
		}
	}
}

func (h *Hub) HasSubscribers(target string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if _, ok := h.conns[target]; !ok {
		return false
	}
	return len(h.conns[target]) > 0
}
