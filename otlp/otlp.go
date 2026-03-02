package otlp

import (
	"log"
	"sort"
	"strings"

	"github.com/xiezhayang/Carve/datamanager"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
)

// Parsed 表示一次 OTLP 解析结果
type Parsed struct {
	names   []string
	allRows []datamanager.Row
}

type FilterFailureStats struct {
	ParsedRows     int
	FailedMetric   int
	FailedResource int
	FailedScope    int
	FailedAttr     int
}

func attrsToMap(m pcommon.Map) map[string]string {
	out := make(map[string]string)
	m.Range(func(k string, v pcommon.Value) bool {
		out[k] = v.AsString()
		return true
	})
	return out
}

func Parse(raw []byte) (*Parsed, error) {
	req := pmetricotlp.NewExportRequest()
	if err := req.UnmarshalJSON(raw); err != nil {
		return nil, err
	}
	md := req.Metrics()
	seen := make(map[string]struct{})
	var allRows []datamanager.Row
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		rm := md.ResourceMetrics().At(i)
		resource := attrsToMap(rm.Resource().Attributes())
		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			sm := rm.ScopeMetrics().At(j)
			scopeName := sm.Scope().Name()
			for k := 0; k < sm.Metrics().Len(); k++ {
				m := sm.Metrics().At(k)
				name := m.Name()
				if name != "" {
					seen[name] = struct{}{}
				}
				switch m.Type() {
				case pmetric.MetricTypeGauge:
					dps := m.Gauge().DataPoints()
					for l := 0; l < dps.Len(); l++ {
						dp := dps.At(l)
						attr := attrsToMap(dp.Attributes())
						allRows = append(allRows, numberDPToRow(dp, name, resource, scopeName, attr))
					}
				case pmetric.MetricTypeSum:
					dps := m.Sum().DataPoints()
					for l := 0; l < dps.Len(); l++ {
						dp := dps.At(l)
						attr := attrsToMap(dp.Attributes())
						allRows = append(allRows, numberDPToRow(dp, name, resource, scopeName, attr))
					}
				}
			}
		}
	}
	names := make([]string, 0, len(seen))
	for n := range seen {
		names = append(names, n)
	}
	return &Parsed{names: names, allRows: allRows}, nil
}

func (p *Parsed) MetricNames() []string {
	return p.names
}

func metricMatch(name string, list []string) bool {
	if len(list) == 0 {
		return true
	}
	for _, s := range list {
		if s != "" && strings.Contains(name, s) {
			return true
		}
	}
	return false
}

func resourceMatch(rowRes, filterRes map[string]string) bool {
	for k, v := range filterRes {
		if rowRes[k] != v {
			return false
		}
	}
	return true
}

func attrMatch(rowAttr, filterAttr map[string]string) bool {
	for k, v := range filterAttr {
		if rowAttr[k] != v {
			return false
		}
	}
	return true
}

func (p *Parsed) RowsForFilterWithStats(f datamanager.Filter, stats *FilterFailureStats) []datamanager.Row {
	if len(p.allRows) == 0 {
		if stats != nil {
			stats.ParsedRows = 0
		}
		return nil
	}
	if stats != nil {
		stats.ParsedRows = len(p.allRows)
	}
	out := make([]datamanager.Row, 0)
	for _, r := range p.allRows {
		if !metricMatch(r.Metric, f.Metrics) {
			if stats != nil {
				stats.FailedMetric++
			}
			continue
		}
		if !resourceMatch(r.Resource, f.Resource) {
			if stats != nil {
				stats.FailedResource++
			}
			continue
		}
		if f.ScopeName != "" && !strings.Contains(r.ScopeName, f.ScopeName) {
			if stats != nil {
				stats.FailedScope++
			}
			continue
		}
		if !attrMatch(r.Attr, f.Attr) {
			if stats != nil {
				stats.FailedAttr++
			}
			continue
		}
		out = append(out, r)
	}
	return out
}

func numberDPToRow(dp pmetric.NumberDataPoint, name string, resource map[string]string, scopeName string, attr map[string]string) datamanager.Row {
	tsNano := dp.Timestamp()
	tsMs := int64(tsNano) / 1_000_000
	var value float64
	switch dp.ValueType() {
	case pmetric.NumberDataPointValueTypeDouble:
		value = dp.DoubleValue()
	case pmetric.NumberDataPointValueTypeInt:
		value = float64(dp.IntValue())
	}
	return datamanager.Row{
		TsMs:      tsMs,
		Metric:    name,
		Value:     value,
		Resource:  resource,
		ScopeName: scopeName,
		Attr:      attr,
	}
}

// DebugLogRawPayload 用本次请求解析出的原始数据（未过滤）打调试日志：
// 1) 本 payload 里出现过的所有 resource 键名；
// 2) 最多 sampleRows 条完整行（ts, metric, value, scope, resource, attr），仅打印指标名包含 metricSubstr 的行（metricSubstr 为空则取前 sampleRows 条）。
// 便于确认有哪些 resource/scope/attr 可用于过滤，以及 CPU load 等条目的完整内容。
func (p *Parsed) DebugLogRawPayload(sampleRows int, metricSubstr string) {
	if len(p.allRows) == 0 {
		log.Printf("[carve] DEBUG_RAW parsed_rows=0 (no data)")
		return
	}
	resourceKeys := make(map[string]struct{})
	for _, r := range p.allRows {
		for k := range r.Resource {
			resourceKeys[k] = struct{}{}
		}
	}
	keys := make([]string, 0, len(resourceKeys))
	for k := range resourceKeys {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	log.Printf("[carve] DEBUG_RAW parsed_rows=%d resource_keys_seen=%v", len(p.allRows), keys)
	count := 0
	for _, r := range p.allRows {
		if count >= sampleRows {
			break
		}
		if metricSubstr != "" && !strings.Contains(r.Metric, metricSubstr) {
			continue
		}
		count++
		log.Printf("[carve] DEBUG_RAW row #%d ts=%d metric=%s value=%v scope=%s resource=%v attr=%v",
			count, r.TsMs, r.Metric, r.Value, r.ScopeName, r.Resource, r.Attr)
	}
	if count == 0 && metricSubstr != "" {
		log.Printf("[carve] DEBUG_RAW no row with metric containing %q in this payload", metricSubstr)
	}
}

// ResourceKeys 返回本批解析结果里出现过的 resource 键名（去重、排序），供 State 做“已知 resource key”列表
func (p *Parsed) ResourceKeys() []string {
	keys := make(map[string]struct{})
	for _, r := range p.allRows {
		for k := range r.Resource {
			keys[k] = struct{}{}
		}
	}
	out := make([]string, 0, len(keys))
	for k := range keys {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
