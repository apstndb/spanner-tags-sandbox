package spannerplus

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/dgryski/go-farm"
)

type Tag struct {
	Key   string
	Value string
}

type innerTransaction interface {
	QueryWithOptions(ctx context.Context, statement spanner.Statement, opts spanner.QueryOptions) *spanner.RowIterator
	ReadWithOptions(ctx context.Context, table string, keys spanner.KeySet, columns []string, opts *spanner.ReadOptions) (ri *spanner.RowIterator)
}

type innerReadOnlyTransaction interface {
	innerTransaction
	Close()
}
type innerReadWriteTransaction interface {
	innerTransaction
	BufferWrite(ms []*spanner.Mutation) error
	UpdateWithOptions(ctx context.Context, stmt spanner.Statement, opts spanner.QueryOptions) (rowCount int64, err error)
	BatchUpdateWithOptions(ctx context.Context, stmts []spanner.Statement, opts spanner.QueryOptions) (_ []int64, err error)
}

type transaction interface {
	QueryWithOptions(ctx context.Context, statement spanner.Statement, opts spanner.QueryOptions) *spanner.RowIterator
	QueryWithHash(ctx context.Context, statement spanner.Statement, tags ...Tag) *spanner.RowIterator
	QueryWithTags(ctx context.Context, statement spanner.Statement, tags ...Tag) *spanner.RowIterator
	ReadWithOptions(ctx context.Context, table string, keys spanner.KeySet, columns []string, opts *spanner.ReadOptions) (ri *spanner.RowIterator)
	Timestamp() (time.Time, error)
	WithTimestampBound(tb spanner.TimestampBound) transaction
}

type hashFunction = func(s string) Tag
type ClientWrapper struct {
	client *spanner.Client
	tags   []Tag
	hFunc  hashFunction
}

func WrapClient(client *spanner.Client, tags []Tag) *ClientWrapper {
	return &ClientWrapper{client, tags, hashFingerprint32Short}
}

func hashFingerprint64Long(s string) Tag {
	return Tag{"hash", fmt.Sprintf("%016x", farm.Fingerprint64([]byte(s)))}
}

func hashFingerprint32Short(s string) Tag {
	return Tag{"h", fmt.Sprintf("%08x", farm.Fingerprint32([]byte(s)))}
}

var tagRe = regexp.MustCompile("^.+=.+$")

func ValidateTagString(raw string) bool {
	ss := strings.Split(raw, ",")
	for _, s := range ss {
		if !tagRe.MatchString(s) {
			return false
		}
	}
	return true
}

func ParseTags(raw string) ([]Tag, error) {
	var result []Tag
	for _, s := range strings.Split(raw, ",") {
		before, after, found := strings.Cut(s, "=")
		if !found {
			return nil, fmt.Errorf("ParseTags: ill-formed tag: %s", raw)
		}
		result = append(result, Tag{before, after})
	}
	return result, nil
}

func WrapClientTagString(client *spanner.Client, tagString string) (*ClientWrapper, error) {
	tags, err := ParseTags(tagString)
	if err != nil {
		return nil, err
	}
	return WrapClient(client, tags), nil
}

func (cw *ClientWrapper) Close() {
	cw.client.Close()
}

func (cw *ClientWrapper) Single(tags []Tag) *ReadOnlyTransactionWrapper {
	return single(cw.client, flattenTags(cw.tags, tags), cw.hFunc)
}

func (cw *ClientWrapper) SingleWithTagString(tagString string) (*ReadOnlyTransactionWrapper, error) {
	tags, err := ParseTags(tagString)
	if err != nil {
		return nil, err
	}
	return cw.Single(tags), nil
}

func (cw *ClientWrapper) ReadOnlyTransaction(tags []Tag) *ReadOnlyTransactionWrapper {
	return readOnlyTransaction(cw.client, flattenTags(cw.tags, tags), cw.hFunc)
}

func (cw *ClientWrapper) ReadOnlyTransactionWithTagString(tagString string) (*ReadOnlyTransactionWrapper, error) {
	tags, err := ParseTags(tagString)
	if err != nil {
		return nil, err
	}
	return cw.ReadOnlyTransaction(tags), nil
}

func (cw *ClientWrapper) ReadWriteTransaction(ctx context.Context, tags []Tag, f func(ctx context.Context, transaction *ReadWriteTransactionWrapper) error) (spanner.CommitResponse, error) {
	return readWriteTransaction(ctx, cw.client, flattenTags(cw.tags, tags), cw.hFunc, f)
}

func (cw *ClientWrapper) ReadWriteTransactionTagString(ctx context.Context, tagString string, f func(ctx context.Context, transaction *ReadWriteTransactionWrapper) error) (spanner.CommitResponse, error) {
	tags, err := ParseTags(tagString)
	if err != nil {
		return spanner.CommitResponse{}, err
	}
	return cw.ReadWriteTransaction(ctx, tags, f)
}

func single(client *spanner.Client, tags []Tag, hFunc hashFunction) *ReadOnlyTransactionWrapper {
	return &ReadOnlyTransactionWrapper{tx: client.Single(), GlobalTags: tags, hFunc: hFunc}
}

func readOnlyTransaction(client *spanner.Client, tags []Tag, hFunc hashFunction) *ReadOnlyTransactionWrapper {
	return &ReadOnlyTransactionWrapper{tx: client.ReadOnlyTransaction(), GlobalTags: tags, hFunc: hFunc}
}

func readWriteTransaction(ctx context.Context, client *spanner.Client, tags []Tag, hFunc hashFunction, f func(ctx context.Context, transaction *ReadWriteTransactionWrapper) error) (spanner.CommitResponse, error) {
	return client.ReadWriteTransactionWithOptions(ctx, func(ctx context.Context, transaction *spanner.ReadWriteTransaction) error {
		return f(ctx, &ReadWriteTransactionWrapper{tx: transaction, GlobalTags: tags, hFunc: hFunc})
	}, spanner.TransactionOptions{
		TransactionTag: FormatTags(tags),
	})
}

type SpannerTransaction interface {
	QueryWithOptions(ctx context.Context, statement spanner.Statement, opts spanner.QueryOptions) *spanner.RowIterator
	Close()
}

type SpannerReadWriteTransaction interface {
	SpannerTransaction
	QueryWithOptions(ctx context.Context, statement spanner.Statement, opts spanner.QueryOptions) *spanner.RowIterator
	Close()
}

type ReadOnlyTransactionWrapper struct {
	tx *spanner.ReadOnlyTransaction
	// SpannerTransaction
	GlobalTags []Tag
	hFunc      hashFunction
}

type ReadWriteTransactionWrapper struct {
	tx *spanner.ReadWriteTransaction
	// SpannerTransaction
	GlobalTags []Tag
	hFunc      hashFunction
}

func flattenTags(tagSlices ...[]Tag) []Tag {
	var result []Tag
	for _, tags := range tagSlices {
		result = append(result, tags...)
	}
	return result
}

func (tx *ReadWriteTransactionWrapper) QueryWithHash(ctx context.Context, statement spanner.Statement, tags ...Tag) *spanner.RowIterator {
	return tx.QueryWithTags(ctx, statement, flattenTags(tags, []Tag{tx.hFunc(statement.SQL)})...)
}

func (tx *ReadWriteTransactionWrapper) QueryWithTags(ctx context.Context, statement spanner.Statement, tags ...Tag) *spanner.RowIterator {
	return queryWithTags(ctx, statement, flattenTags(tx.GlobalTags, tags), tx.tx)
}

func (tx *ReadOnlyTransactionWrapper) QueryWithHash(ctx context.Context, statement spanner.Statement, tags ...Tag) *spanner.RowIterator {
	return tx.QueryWithTags(ctx, statement, flattenTags(tags, []Tag{tx.hFunc(statement.SQL)})...)
}

func (tx *ReadOnlyTransactionWrapper) QueryWithTags(ctx context.Context, statement spanner.Statement, tags ...Tag) *spanner.RowIterator {
	return queryWithTags(ctx, statement, flattenTags(tx.GlobalTags, tags), tx.tx)
}

func (tx *ReadOnlyTransactionWrapper) Close() {
	tx.tx.Close()
}

func (tx *ReadOnlyTransactionWrapper) Timestamp() (time.Time, error) {
	return tx.tx.Timestamp()
}

func (tx *ReadOnlyTransactionWrapper) WithTimestampBound(tb spanner.TimestampBound) *ReadOnlyTransactionWrapper {
	return &ReadOnlyTransactionWrapper{tx: tx.tx.WithTimestampBound(tb), GlobalTags: tx.GlobalTags, hFunc: tx.hFunc}
}

func queryWithTags(ctx context.Context, statement spanner.Statement, tags []Tag, tx innerTransaction) *spanner.RowIterator {
	return tx.QueryWithOptions(ctx, statement, spanner.QueryOptions{
		RequestTag: FormatTags(tags),
	})
}

func FormatTags(tempTags []Tag) string {
	var tagStrings []string
	for _, tag := range tempTags {
		tagStrings = append(tagStrings, fmt.Sprintf("%s=%s", tag.Key, tag.Value))
	}
	return strings.Join(tagStrings, ",")
}
