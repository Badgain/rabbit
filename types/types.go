package types

type ContentType string

func (c ContentType) String() string {
	return string(c)
}

const (
	ContentTypePlainText       ContentType = "text/plain"
	ContentTypeApplicationJson ContentType = "application/json"
)
