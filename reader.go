package main

type Reader interface {
	// GetHeaders returns list of column names
	GetHeader() []string

	// GetRows returns next one file row or io.EOF
	GetRow(asString bool) ([]any, error)

	// Options returns options
	Options() *Options

	Close() error
}

func getHeader(r Reader) ([]string, error) {
	headerAny, err := r.GetRow(true)
	if err != nil {
		return nil, err
	}

	header := make([]string, 0, len(headerAny))
	for _, v := range headerAny {
		header = append(header, v.(string))
	}

	return header, nil
}
