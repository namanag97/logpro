package sources

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/logflow/logflow/pkg/ingest/core"
)

// HTTPSource fetches data from HTTP/HTTPS URLs.
type HTTPSource struct {
	url        *url.URL
	client     *http.Client
	headers    map[string]string
	format     core.Format
	size       int64
	modTime    time.Time
	metadata   map[string]string
	fetched    bool
}

// HTTPSourceOptions configures HTTP source behavior.
type HTTPSourceOptions struct {
	// Custom HTTP client
	Client *http.Client

	// Custom headers
	Headers map[string]string

	// Force format (otherwise detected from URL/content-type)
	Format core.Format

	// Timeout
	Timeout time.Duration

	// Auth
	BasicAuth *BasicAuth
	BearerToken string
}

// BasicAuth for HTTP basic authentication.
type BasicAuth struct {
	Username string
	Password string
}

// NewHTTPSource creates an HTTP source from a URL.
func NewHTTPSource(rawURL string, opts *HTTPSourceOptions) (*HTTPSource, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("invalid URL: %w", err)
	}

	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("unsupported scheme: %s", parsed.Scheme)
	}

	s := &HTTPSource{
		url:      parsed,
		metadata: make(map[string]string),
		size:     -1, // Unknown until HEAD request
	}

	if opts == nil {
		opts = &HTTPSourceOptions{}
	}

	// Set up client
	if opts.Client != nil {
		s.client = opts.Client
	} else {
		timeout := opts.Timeout
		if timeout == 0 {
			timeout = 30 * time.Second
		}
		s.client = &http.Client{
			Timeout: timeout,
		}
	}

	// Set up headers
	s.headers = make(map[string]string)
	for k, v := range opts.Headers {
		s.headers[k] = v
	}

	// Auth
	if opts.BasicAuth != nil {
		// Will be set on request
		s.metadata["auth_type"] = "basic"
	}
	if opts.BearerToken != "" {
		s.headers["Authorization"] = "Bearer " + opts.BearerToken
	}

	// Format
	if opts.Format != core.FormatUnknown {
		s.format = opts.Format
	} else {
		s.format = detectFormatFromURL(parsed)
	}

	return s, nil
}

// ID returns the URL as identifier.
func (s *HTTPSource) ID() string {
	return s.url.String()
}

// Location returns the URL.
func (s *HTTPSource) Location() string {
	return s.url.String()
}

// Format returns the detected format.
func (s *HTTPSource) Format() core.Format {
	return s.format
}

// Size returns the content length, or -1 if unknown.
func (s *HTTPSource) Size() int64 {
	return s.size
}

// ModTime returns the last modified time.
func (s *HTTPSource) ModTime() time.Time {
	return s.modTime
}

// Metadata returns HTTP metadata.
func (s *HTTPSource) Metadata() map[string]string {
	return s.metadata
}

// Open fetches the URL and returns a reader.
func (s *HTTPSource) Open(ctx context.Context) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", s.url.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Set headers
	for k, v := range s.headers {
		req.Header.Set(k, v)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, resp.Status)
	}

	// Update metadata from response
	s.updateMetadata(resp)

	return resp.Body, nil
}

// FetchMetadata fetches metadata via HEAD request without downloading content.
func (s *HTTPSource) FetchMetadata(ctx context.Context) error {
	if s.fetched {
		return nil
	}

	req, err := http.NewRequestWithContext(ctx, "HEAD", s.url.String(), nil)
	if err != nil {
		return err
	}

	for k, v := range s.headers {
		req.Header.Set(k, v)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, resp.Status)
	}

	s.updateMetadata(resp)
	s.fetched = true
	return nil
}

func (s *HTTPSource) updateMetadata(resp *http.Response) {
	// Size
	if cl := resp.Header.Get("Content-Length"); cl != "" {
		if size, err := strconv.ParseInt(cl, 10, 64); err == nil {
			s.size = size
		}
	}

	// Modified time
	if lm := resp.Header.Get("Last-Modified"); lm != "" {
		if t, err := time.Parse(time.RFC1123, lm); err == nil {
			s.modTime = t
		}
	}

	// Content type
	if ct := resp.Header.Get("Content-Type"); ct != "" {
		s.metadata["content_type"] = ct
		if s.format == core.FormatUnknown {
			s.format = detectFormatFromContentType(ct)
		}
	}

	// ETag
	if etag := resp.Header.Get("ETag"); etag != "" {
		s.metadata["etag"] = etag
	}
}

func detectFormatFromURL(u *url.URL) core.Format {
	ext := strings.ToLower(path.Ext(u.Path))

	switch ext {
	case ".csv":
		return core.FormatCSV
	case ".tsv":
		return core.FormatTSV
	case ".json":
		return core.FormatJSON
	case ".jsonl", ".ndjson":
		return core.FormatJSONL
	case ".parquet":
		return core.FormatParquet
	case ".xml":
		return core.FormatXML
	case ".xes":
		return core.FormatXES
	case ".xlsx", ".xls":
		return core.FormatXLSX
	case ".avro":
		return core.FormatAvro
	case ".orc":
		return core.FormatORC
	case ".gz", ".gzip":
		return core.FormatGzip
	default:
		return core.FormatUnknown
	}
}

func detectFormatFromContentType(ct string) core.Format {
	ct = strings.ToLower(ct)

	switch {
	case strings.Contains(ct, "text/csv"):
		return core.FormatCSV
	case strings.Contains(ct, "text/tab-separated"):
		return core.FormatTSV
	case strings.Contains(ct, "application/json"):
		return core.FormatJSON
	case strings.Contains(ct, "application/x-ndjson"):
		return core.FormatJSONL
	case strings.Contains(ct, "application/xml"), strings.Contains(ct, "text/xml"):
		return core.FormatXML
	case strings.Contains(ct, "application/vnd.openxmlformats"):
		return core.FormatXLSX
	case strings.Contains(ct, "application/x-parquet"):
		return core.FormatParquet
	case strings.Contains(ct, "application/avro"):
		return core.FormatAvro
	case strings.Contains(ct, "application/gzip"):
		return core.FormatGzip
	default:
		return core.FormatUnknown
	}
}

// Verify interface compliance
var _ core.Source = (*HTTPSource)(nil)
