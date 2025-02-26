package common

// 要收集的日志的配置
type CollectEntry struct {
	Path  string `json:"path"`
	Topic string `json:"topic"`
}
