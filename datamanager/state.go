package datamanager

import (
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

var ErrInvalidFilename = fmt.Errorf("invalid filename")

// Target 表示一路收集：独立 filter + 一个 CSV 文件；Collecting 表示当前是否在收
// Filter 按 OTLP 层级：Metrics 子串 OR，Resource/Attr 全匹配 AND，ScopeName 子串
type Filter struct {
	Metrics   []string
	Resource  map[string]string
	ScopeName string
	Attr      map[string]string
}

// Target 表示一路收集：Filter + 一个 CSV 文件；Path 由 AllTargets/ActiveTargets 填充，不持久化
type Target struct {
	Name       string
	Filename   string
	Path       string `json:"-"` // 仅返回用，不参与 JSON 持久化
	Filter     Filter
	Collecting bool
}

type State struct {
	mu                sync.RWMutex
	targets           map[string]*Target
	csvDir            string
	stateFilePath     string
	knownMetrics      map[string]struct{} // 从 OTLP 里发现过的指标名，供用户选择
	knownResourceKeys map[string]struct{} // 从 OTLP 里发现过的 resource key，供用户选择
}

func NewState(csvDir string) *State {
	return &State{
		csvDir:            csvDir,
		stateFilePath:     filepath.Join(csvDir, "state.json"),
		targets:           make(map[string]*Target),
		knownMetrics:      make(map[string]struct{}),
		knownResourceKeys: make(map[string]struct{}),
	}
}

func SafeFilename(name string) bool {
	name = strings.TrimSpace(name)
	if name == "" || len(name) > 200 || strings.Contains(name, "..") ||
		strings.Contains(name, "/") || strings.Contains(name, "\\") {
		return false
	}
	for _, c := range name {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '-' || c == '.') {
			return false
		}
	}
	return true
}

func copyFilter(f Filter) Filter {
	out := Filter{
		Metrics:   make([]string, len(f.Metrics)),
		ScopeName: f.ScopeName,
		Resource:  make(map[string]string),
		Attr:      make(map[string]string),
	}
	copy(out.Metrics, f.Metrics)
	maps.Copy(out.Resource, f.Resource)
	maps.Copy(out.Attr, f.Attr)
	return out
}

func (s *State) StartCollect(name, filename string, filter Filter) error {
	name = strings.TrimSpace(name)
	if name == "" || !SafeFilename(name) || !SafeFilename(filename) {
		return ErrInvalidFilename
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	fcopy := copyFilter(filter)
	if t, ok := s.targets[name]; ok {
		t.Filename = filename
		t.Filter = fcopy
		t.Collecting = true
	} else {
		s.targets[name] = &Target{Name: name, Filename: filename, Filter: fcopy, Collecting: true}
	}
	return nil
}

func (s *State) StopCollect(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if name == "" {
		for _, t := range s.targets {
			t.Collecting = false
		}
		return
	}
	if t, ok := s.targets[name]; ok {
		t.Collecting = false
	}
}

// Collecting 返回当前是否有在收集的路，以及简要描述（如 "2 targets: a, b"）
func (s *State) Collecting() (bool, string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var names []string
	for name, t := range s.targets {
		if t.Collecting {
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		return false, ""
	}
	summary := strings.Join(names, ", ")
	if len(names) > 1 {
		summary = fmt.Sprintf("%d targets: %s", len(names), summary)
	}
	return true, summary
}

// ActiveTargets 只返回当前 Collecting==true 的路，用于写文件
func (s *State) ActiveTargets() []Target {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []Target
	for _, t := range s.targets {
		if !t.Collecting {
			continue
		}
		out = append(out, Target{
			Name:       t.Name,
			Filename:   t.Filename,
			Path:       filepath.Join(s.csvDir, t.Filename),
			Filter:     copyFilter(t.Filter),
			Collecting: t.Collecting,
		})
	}
	return out
}

// AllTargets 返回所有已定义的路（含未在收集的），用于状态展示
func (s *State) AllTargets() []Target {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Target, 0, len(s.targets))
	for _, t := range s.targets {
		out = append(out, Target{
			Name:       t.Name,
			Filename:   t.Filename,
			Path:       filepath.Join(s.csvDir, t.Filename),
			Filter:     copyFilter(t.Filter),
			Collecting: t.Collecting,
		})
	}
	return out
}

// AddKnownMetrics 把本次 OTLP 解析出的指标名并入已知列表（/v1/metrics 成功后调用）
func (s *State) AddKnownMetrics(names []string) {
	if len(names) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, n := range names {
		if n != "" {
			s.knownMetrics[n] = struct{}{}
		}
	}
}

// KnownMetrics 返回当前已知的指标名列表（已排序），供用户选择要收集哪些
func (s *State) KnownMetrics() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.knownMetrics))
	for n := range s.knownMetrics {
		out = append(out, n)
	}
	// 排序，便于前端展示
	// 用 strings 比较即可
	for i := 0; i < len(out); i++ {
		for j := i + 1; j < len(out); j++ {
			if out[j] < out[i] {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

// DeleteTarget 从 state 中移除指定 name 的 target。name 为空则删除全部。
// 返回被删除的 target 是否曾存在（用于 404）。
func (s *State) DeleteTarget(name string) (existed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if name == "" {
		existed = len(s.targets) > 0
		s.targets = make(map[string]*Target)
		return existed
	}
	if _, ok := s.targets[name]; ok {
		delete(s.targets, name)
		return true
	}
	return false
}

// AddKnownResourceKeys 把本次 OTLP 里出现的 resource 键名并入已知列表
func (s *State) AddKnownResourceKeys(keys []string) {
	if len(keys) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range keys {
		if k != "" {
			s.knownResourceKeys[k] = struct{}{}
		}
	}
}

// KnownResourceKeys 返回当前已知的 resource 键名列表（已排序），供用户选择过滤维度
func (s *State) KnownResourceKeys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.knownResourceKeys))
	for k := range s.knownResourceKeys {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// LoadTargets 从 stateFilePath 恢复 target；文件不存在则扫描 csvDir 下已有 .csv 生成占位 target 并落盘。
func (s *State) LoadTargets() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := os.ReadFile(s.stateFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			s.loadTargetsFromCSVDir()
			return nil
		}
		return err
	}
	var list []Target
	if err := json.Unmarshal(data, &list); err != nil {
		return err
	}
	s.targets = make(map[string]*Target)
	for i := range list {
		t := &list[i]
		if t.Name == "" || !SafeFilename(t.Name) || !SafeFilename(t.Filename) {
			continue
		}
		if t.Filter.Resource == nil {
			t.Filter.Resource = make(map[string]string)
		}
		if t.Filter.Attr == nil {
			t.Filter.Attr = make(map[string]string)
		}
		s.targets[t.Name] = &Target{
			Name:       t.Name,
			Filename:   t.Filename,
			Filter:     copyFilter(t.Filter),
			Collecting: false,
		}
	}
	for _, t := range s.targets {
		if !filterEmpty(t.Filter) {
			continue
		}
		full := filepath.Join(s.csvDir, t.Filename)
		if _, err := os.Stat(full); err != nil {
			continue
		}
		if _, f, ok := ReadTargetMeta(full); ok {
			t.Filter = copyFilter(f)
		}
	}
	return nil
}

func (s *State) loadTargetsFromCSVDir() {
	entries, err := os.ReadDir(s.csvDir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		base := e.Name()
		if !strings.HasSuffix(strings.ToLower(base), ".csv") {
			continue
		}
		full := filepath.Join(s.csvDir, base)
		metaName, metaFilter, hasMeta := ReadTargetMeta(full)
		name := strings.TrimSuffix(base, ".csv")
		if name == "" || !SafeFilename(name) || !SafeFilename(base) {
			continue
		}
		if hasMeta && metaName != "" && SafeFilename(metaName) {
			name = metaName
		}
		if _, ok := s.targets[name]; ok {
			continue
		}
		filter := Filter{Resource: make(map[string]string), Attr: make(map[string]string)}
		if hasMeta {
			filter = copyFilter(metaFilter)
		}
		s.targets[name] = &Target{
			Name:       name,
			Filename:   base,
			Filter:     filter,
			Collecting: false,
		}
	}
	s.saveTargetsUnlocked()
}

func (s *State) saveTargetsUnlocked() {
	list := make([]Target, 0, len(s.targets))
	for _, t := range s.targets {
		list = append(list, Target{
			Name:       t.Name,
			Filename:   t.Filename,
			Filter:     t.Filter,
			Collecting: t.Collecting,
		})
	}
	data, err := json.MarshalIndent(list, "", "\t")
	if err != nil {
		return
	}
	_ = os.WriteFile(s.stateFilePath, data, 0644)
}

// SaveTargets 将当前 target 列表写回 stateFilePath。
func (s *State) SaveTargets() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saveTargetsUnlocked()
	return nil
}

func filterEmpty(f Filter) bool {
	return len(f.Metrics) == 0 && f.ScopeName == "" &&
		len(f.Resource) == 0 && len(f.Attr) == 0
}
