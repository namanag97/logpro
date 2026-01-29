// Package s3 provides S3 Select pushdown queries for filtering data at the storage layer.
package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// SelectFormat specifies the data format for S3 Select queries.
type SelectFormat string

const (
	SelectFormatCSV     SelectFormat = "CSV"
	SelectFormatJSON    SelectFormat = "JSON"
	SelectFormatParquet SelectFormat = "Parquet"
)

// SelectQuery defines an S3 Select query.
type SelectQuery struct {
	// Bucket is the S3 bucket name
	Bucket string

	// Key is the S3 object key
	Key string

	// Expression is the SQL expression to execute
	// Example: "SELECT * FROM S3Object WHERE timestamp > '2024-01-01'"
	Expression string

	// InputFormat specifies the input file format
	InputFormat SelectFormat

	// OutputFormat specifies the output format
	OutputFormat SelectFormat

	// InputConfig provides format-specific input configuration
	InputConfig SelectInputConfig

	// OutputConfig provides format-specific output configuration
	OutputConfig SelectOutputConfig

	// CompressionType for the input (NONE, GZIP, BZIP2)
	CompressionType types.CompressionType

	// ScanRange for byte-range queries (optional)
	ScanRangeStart *int64
	ScanRangeEnd   *int64
}

// SelectInputConfig configures input format parsing.
type SelectInputConfig struct {
	// CSV options
	CSVFieldDelimiter    string
	CSVRecordDelimiter   string
	CSVQuoteCharacter    string
	CSVQuoteEscapeChar   string
	CSVFileHeaderInfo    types.FileHeaderInfo // USE, IGNORE, NONE
	CSVComments          string
	CSVAllowQuotedRecordDelimiter bool

	// JSON options
	JSONType types.JSONType // DOCUMENT, LINES
}

// SelectOutputConfig configures output format.
type SelectOutputConfig struct {
	// CSV options
	CSVFieldDelimiter  string
	CSVRecordDelimiter string
	CSVQuoteCharacter  string
	CSVQuoteFields     types.QuoteFields

	// JSON options
	JSONRecordDelimiter string
}

// DefaultSelectQuery creates a query with sensible defaults.
func DefaultSelectQuery(bucket, key, expression string, format SelectFormat) SelectQuery {
	return SelectQuery{
		Bucket:          bucket,
		Key:             key,
		Expression:      expression,
		InputFormat:     format,
		OutputFormat:    format,
		CompressionType: types.CompressionTypeNone,
		InputConfig: SelectInputConfig{
			CSVFieldDelimiter:  ",",
			CSVRecordDelimiter: "\n",
			CSVQuoteCharacter:  "\"",
			CSVFileHeaderInfo:  types.FileHeaderInfoUse,
			JSONType:           types.JSONTypeLines,
		},
		OutputConfig: SelectOutputConfig{
			CSVFieldDelimiter:   ",",
			CSVRecordDelimiter:  "\n",
			JSONRecordDelimiter: "\n",
		},
	}
}

// SelectQuery executes an S3 Select query and returns a reader for the results.
func (c *Client) SelectQuery(ctx context.Context, q SelectQuery) (io.ReadCloser, error) {
	if q.Bucket == "" {
		q.Bucket = c.cfg.Bucket
	}

	input := &s3.SelectObjectContentInput{
		Bucket:         aws.String(q.Bucket),
		Key:            aws.String(q.Key),
		Expression:     aws.String(q.Expression),
		ExpressionType: types.ExpressionTypeSql,
	}

	// Configure input serialization
	input.InputSerialization = buildInputSerialization(q)

	// Configure output serialization
	input.OutputSerialization = buildOutputSerialization(q)

	// Add scan range if specified
	if q.ScanRangeStart != nil || q.ScanRangeEnd != nil {
		input.ScanRange = &types.ScanRange{
			Start: q.ScanRangeStart,
			End:   q.ScanRangeEnd,
		}
	}

	// Execute query
	output, err := c.client.SelectObjectContent(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("S3 Select query failed: %w", err)
	}

	// Create reader that processes the event stream
	return newSelectReader(output), nil
}

func buildInputSerialization(q SelectQuery) *types.InputSerialization {
	is := &types.InputSerialization{
		CompressionType: q.CompressionType,
	}

	switch q.InputFormat {
	case SelectFormatCSV:
		is.CSV = &types.CSVInput{
			FieldDelimiter:  aws.String(q.InputConfig.CSVFieldDelimiter),
			RecordDelimiter: aws.String(q.InputConfig.CSVRecordDelimiter),
			QuoteCharacter:  aws.String(q.InputConfig.CSVQuoteCharacter),
			FileHeaderInfo:  q.InputConfig.CSVFileHeaderInfo,
		}
		if q.InputConfig.CSVQuoteEscapeChar != "" {
			is.CSV.QuoteEscapeCharacter = aws.String(q.InputConfig.CSVQuoteEscapeChar)
		}
		if q.InputConfig.CSVComments != "" {
			is.CSV.Comments = aws.String(q.InputConfig.CSVComments)
		}
		is.CSV.AllowQuotedRecordDelimiter = aws.Bool(q.InputConfig.CSVAllowQuotedRecordDelimiter)

	case SelectFormatJSON:
		is.JSON = &types.JSONInput{
			Type: q.InputConfig.JSONType,
		}

	case SelectFormatParquet:
		is.Parquet = &types.ParquetInput{}
	}

	return is
}

func buildOutputSerialization(q SelectQuery) *types.OutputSerialization {
	os := &types.OutputSerialization{}

	switch q.OutputFormat {
	case SelectFormatCSV:
		os.CSV = &types.CSVOutput{
			FieldDelimiter:  aws.String(q.OutputConfig.CSVFieldDelimiter),
			RecordDelimiter: aws.String(q.OutputConfig.CSVRecordDelimiter),
		}
		if q.OutputConfig.CSVQuoteCharacter != "" {
			os.CSV.QuoteCharacter = aws.String(q.OutputConfig.CSVQuoteCharacter)
		}
		if q.OutputConfig.CSVQuoteFields != "" {
			os.CSV.QuoteFields = q.OutputConfig.CSVQuoteFields
		}

	case SelectFormatJSON:
		os.JSON = &types.JSONOutput{}
		if q.OutputConfig.JSONRecordDelimiter != "" {
			os.JSON.RecordDelimiter = aws.String(q.OutputConfig.JSONRecordDelimiter)
		}
	}

	return os
}

// selectReader processes S3 Select event stream and provides a Reader interface.
type selectReader struct {
	eventStream *s3.SelectObjectContentEventStream
	buf         bytes.Buffer
	err         error
	closed      bool
	stats       *SelectStats
}

func newSelectReader(output *s3.SelectObjectContentOutput) *selectReader {
	return &selectReader{
		eventStream: output.GetStream(),
		stats:       &SelectStats{},
	}
}

func (r *selectReader) Read(p []byte) (n int, err error) {
	// First, try to read from buffer
	if r.buf.Len() > 0 {
		return r.buf.Read(p)
	}

	if r.closed || r.err != nil {
		if r.err != nil {
			return 0, r.err
		}
		return 0, io.EOF
	}

	// Read from event stream
	for {
		event := r.eventStream.Events()
		if event == nil {
			r.closed = true
			return 0, io.EOF
		}

		select {
		case ev, ok := <-event:
			if !ok {
				r.closed = true
				return 0, io.EOF
			}

			switch e := ev.(type) {
			case *types.SelectObjectContentEventStreamMemberRecords:
				// Write records to buffer
				r.buf.Write(e.Value.Payload)
				return r.buf.Read(p)

			case *types.SelectObjectContentEventStreamMemberStats:
				// Capture statistics
				if e.Value.Details != nil {
					r.stats.BytesScanned = aws.ToInt64(e.Value.Details.BytesScanned)
					r.stats.BytesProcessed = aws.ToInt64(e.Value.Details.BytesProcessed)
					r.stats.BytesReturned = aws.ToInt64(e.Value.Details.BytesReturned)
				}

			case *types.SelectObjectContentEventStreamMemberEnd:
				r.closed = true
				return 0, io.EOF
			}
		}
	}
}

func (r *selectReader) Close() error {
	if r.eventStream != nil {
		return r.eventStream.Close()
	}
	return nil
}

// Stats returns the query statistics.
func (r *selectReader) Stats() *SelectStats {
	return r.stats
}

// SelectStats contains S3 Select query statistics.
type SelectStats struct {
	BytesScanned   int64
	BytesProcessed int64
	BytesReturned  int64
}

// BytesSaved returns bytes saved by pushdown (scanned - returned).
func (s *SelectStats) BytesSaved() int64 {
	return s.BytesScanned - s.BytesReturned
}

// CompressionRatio returns the ratio of returned to scanned bytes.
func (s *SelectStats) CompressionRatio() float64 {
	if s.BytesScanned == 0 {
		return 0
	}
	return float64(s.BytesReturned) / float64(s.BytesScanned)
}

// --- Filter Building Utilities ---

// Filter represents a data filter for SQL generation.
type Filter struct {
	Column   string
	Operator string // =, !=, <, >, <=, >=, LIKE, IN, BETWEEN, IS NULL, IS NOT NULL
	Value    interface{}
	Values   []interface{} // For IN, BETWEEN
}

// BuildFilterSQL converts filters to S3 Select SQL WHERE clause.
func BuildFilterSQL(filters []Filter) string {
	if len(filters) == 0 {
		return ""
	}

	var conditions []string
	for _, f := range filters {
		cond := buildCondition(f)
		if cond != "" {
			conditions = append(conditions, cond)
		}
	}

	if len(conditions) == 0 {
		return ""
	}

	return "WHERE " + strings.Join(conditions, " AND ")
}

func buildCondition(f Filter) string {
	col := escapeIdentifier(f.Column)

	switch strings.ToUpper(f.Operator) {
	case "=", "!=", "<>", "<", ">", "<=", ">=":
		return fmt.Sprintf("%s %s %s", col, f.Operator, formatValue(f.Value))

	case "LIKE":
		return fmt.Sprintf("%s LIKE %s", col, formatValue(f.Value))

	case "IN":
		if len(f.Values) == 0 {
			return ""
		}
		vals := make([]string, len(f.Values))
		for i, v := range f.Values {
			vals[i] = formatValue(v)
		}
		return fmt.Sprintf("%s IN (%s)", col, strings.Join(vals, ", "))

	case "BETWEEN":
		if len(f.Values) < 2 {
			return ""
		}
		return fmt.Sprintf("%s BETWEEN %s AND %s", col, formatValue(f.Values[0]), formatValue(f.Values[1]))

	case "IS NULL":
		return fmt.Sprintf("%s IS NULL", col)

	case "IS NOT NULL":
		return fmt.Sprintf("%s IS NOT NULL", col)

	default:
		return ""
	}
}

func formatValue(v interface{}) string {
	switch val := v.(type) {
	case string:
		return fmt.Sprintf("'%s'", escapeString(val))
	case int, int32, int64, float32, float64:
		return fmt.Sprintf("%v", val)
	case bool:
		if val {
			return "true"
		}
		return "false"
	case time.Time:
		return fmt.Sprintf("'%s'", val.Format(time.RFC3339))
	default:
		return fmt.Sprintf("'%v'", v)
	}
}

func escapeString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

func escapeIdentifier(s string) string {
	// Use s."column" syntax for column access
	if strings.Contains(s, ".") {
		return s
	}
	return "s." + s
}

// BuildSelectSQL builds a complete SELECT statement.
func BuildSelectSQL(columns []string, filters []Filter, limit int) string {
	var sb strings.Builder

	// SELECT clause
	sb.WriteString("SELECT ")
	if len(columns) == 0 {
		sb.WriteString("*")
	} else {
		for i, col := range columns {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(escapeIdentifier(col))
		}
	}

	sb.WriteString(" FROM S3Object s")

	// WHERE clause
	whereClause := BuildFilterSQL(filters)
	if whereClause != "" {
		sb.WriteString(" ")
		sb.WriteString(whereClause)
	}

	// LIMIT clause
	if limit > 0 {
		sb.WriteString(fmt.Sprintf(" LIMIT %d", limit))
	}

	return sb.String()
}

// --- Convenience Query Builders ---

// TimeRangeQuery creates a query for events within a time range.
func TimeRangeQuery(bucket, key, timestampColumn string, start, end time.Time) SelectQuery {
	sql := BuildSelectSQL(nil, []Filter{
		{Column: timestampColumn, Operator: ">=", Value: start},
		{Column: timestampColumn, Operator: "<=", Value: end},
	}, 0)

	return DefaultSelectQuery(bucket, key, sql, SelectFormatCSV)
}

// ColumnFilterQuery creates a query with column value filtering.
func ColumnFilterQuery(bucket, key, column string, value interface{}) SelectQuery {
	sql := BuildSelectSQL(nil, []Filter{
		{Column: column, Operator: "=", Value: value},
	}, 0)

	return DefaultSelectQuery(bucket, key, sql, SelectFormatCSV)
}

// SampleQuery creates a query that returns a limited sample of rows.
func SampleQuery(bucket, key string, limit int) SelectQuery {
	sql := BuildSelectSQL(nil, nil, limit)
	return DefaultSelectQuery(bucket, key, sql, SelectFormatCSV)
}

// CountQuery creates a query that counts rows matching filters.
func CountQuery(bucket, key string, filters []Filter) SelectQuery {
	whereClause := BuildFilterSQL(filters)
	sql := "SELECT COUNT(*) FROM S3Object s"
	if whereClause != "" {
		sql += " " + whereClause
	}
	return DefaultSelectQuery(bucket, key, sql, SelectFormatCSV)
}

// --- Parquet-Specific Queries ---

// ParquetQuery creates a query optimized for Parquet files.
func ParquetQuery(bucket, key, expression string) SelectQuery {
	q := SelectQuery{
		Bucket:       bucket,
		Key:          key,
		Expression:   expression,
		InputFormat:  SelectFormatParquet,
		OutputFormat: SelectFormatJSON, // Parquet output not supported, use JSON
		OutputConfig: SelectOutputConfig{
			JSONRecordDelimiter: "\n",
		},
	}
	return q
}

// ParquetColumnQuery creates a query that selects specific columns from Parquet.
func ParquetColumnQuery(bucket, key string, columns []string, filters []Filter) SelectQuery {
	sql := BuildSelectSQL(columns, filters, 0)
	return ParquetQuery(bucket, key, sql)
}
