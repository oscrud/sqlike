package sqlike

import (
	"context"
	"encoding/base64"
	"fmt"
	"reflect"

	"github.com/si3nloong/sqlike/sql/expr"
	"github.com/si3nloong/sqlike/sqlike"
	"github.com/si3nloong/sqlike/sqlike/actions"
	"github.com/si3nloong/sqlike/sqlike/options"
)

// Definition
var (
	OrderByDescending = "DESC"
)

// Paginator :
type Paginator struct {
	Cursor string
	Page   int
	Limit  int
	Order  map[string]string
	Select map[string]string
	Query  interface{}
}

// NewPaginator :
func NewPaginator() Paginator {
	return Paginator{}
}

// BuildMeta :
func (p Paginator) BuildMeta() map[string]interface{} {
	meta := make(map[string]interface{}, 0)

	if p.Cursor != "" {
		meta["cursor"] = p.Cursor
	}

	if p.Limit != 0 {
		meta["limit"] = p.Limit
	}

	if p.Page != 0 {
		meta["page"] = p.Page
	}
	return meta
}

// GetResult :
func (p *Paginator) GetResult(ctx context.Context, table *sqlike.Table, result interface{}) error {
	query := actions.Paginate().Limit(uint(p.Limit + 1))
	options := options.Paginate()
	selects := make([]interface{}, 0)

	if len(p.Select) > 0 {
		for key, value := range p.Select {
			if value != "" {
				selects = append(selects, expr.As(key, value))
			} else {
				selects = append(selects, expr.Column(key))
			}
		}
	} else {
		selects = append(selects, "*")
	}

	query = query.Select(selects...)
	query = query.Where(p.Query)
	for key, value := range p.Order {
		if value == OrderByDescending {
			query = query.OrderBy(expr.Desc(key))
		} else {
			query = query.OrderBy(expr.Asc(key))
		}
	}

	paginator, err := table.Paginate(ctx, query, options)
	if err != nil {
		return err
	}

	if p.Cursor != "" {
		cursor, err := base64.StdEncoding.DecodeString(p.Cursor)
		if err != nil {
			return err
		}

		if err := paginator.NextCursor(ctx, string(cursor)); err != nil {
			return err
		}
	}

	slice := reflect.ValueOf(result)
	if err := paginator.All(slice); err != nil {
		return err
	}

	slice = slice.Elem()
	if v := slice.Len(); v > p.Limit {
		key := slice.Index(v - 1).Elem().FieldByName("Key")
		if key.CanInterface() {
			cursorValue := fmt.Sprintf("%v", key.Interface())
			cursor := base64.StdEncoding.EncodeToString([]byte(cursorValue))
			p.Cursor = cursor
			slice.Set(slice.Slice(0, v-1))
		}
	} else {
		p.Cursor = ""
	}

	return nil
}
