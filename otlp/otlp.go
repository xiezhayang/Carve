// Package otlp parses OTLP JSON metrics using the OpenTelemetry Collector pdata
// model, so all OTLP fields (resource, scope, metric name/unit/description,
// data point attributes) are available for filtering. Currently we filter by
// metric name substring and output Gauge/Sum number data points as CSV rows.
package otlp

import (
	"strings"

	"github.com/xiezhayang/Carve/datamanager"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
)

// Parsed 表示一次 OTLP 解析结果；Parse 时一次遍历同时得到指标名和全部 rows
type Parsed struct {
	names   []string          // 本 payload 出现的指标名（去重）
	allRows []datamanager.Row // 本 payload 所有 Gauge/Sum 的 rows，未按 filter 筛
}

// Parse 解析 OTLP JSON，一次遍历同时：收集指标名 + 把所有 Gauge/Sum 转成 Row
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
		service := getServiceName(rm.Resource().Attributes())
		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			sm := rm.ScopeMetrics().At(j)
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
						allRows = append(allRows, numberDPToRow(dps.At(l), name, service))
					}
				case pmetric.MetricTypeSum:
					dps := m.Sum().DataPoints()
					for l := 0; l < dps.Len(); l++ {
						allRows = append(allRows, numberDPToRow(dps.At(l), name, service))
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

// MetricNames 返回本 payload 里出现的所有指标名（供更新「可选用指标」）
func (p *Parsed) MetricNames() []string {
	return p.names
}

// RowsForFilter 按 allowList 在已缓存的 allRows 上做子串过滤，得到要收集的 rows
func (p *Parsed) RowsForFilter(allowList []string) []datamanager.Row {
	if len(p.allRows) == 0 {
		return nil
	}
	if len(allowList) == 0 {
		return p.allRows
	}
	out := make([]datamanager.Row, 0)
	for _, r := range p.allRows {
		if metricAllowed(r.Metric, allowList) {
			out = append(out, r)
		}
	}
	return out
}

func getServiceName(attrs pcommon.Map) string {
	if v, ok := attrs.Get("service.name"); ok {
		return v.Str()
	}
	return ""
}

func metricAllowed(name string, allowList []string) bool {
	if len(allowList) == 0 {
		return true
	}
	for _, sub := range allowList {
		if sub != "" && strings.Contains(name, sub) {
			return true
		}
	}
	return false
}

func numberDPToRow(dp pmetric.NumberDataPoint, name, service string) datamanager.Row {
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
		TsMs:    tsMs,
		Metric:  name,
		Value:   value,
		Service: service,
	}
}
