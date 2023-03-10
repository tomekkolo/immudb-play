package lineparser

type K8sAuditLineParser struct {
}

func NewK8sAuditLineParser() *K8sAuditLineParser {
	return &K8sAuditLineParser{}
}

func (*K8sAuditLineParser) Parse(line string) ([]byte, error) {
	return []byte(line), nil
}
