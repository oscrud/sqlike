package sqlike

import (
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
	Offset int
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

	if p.Offset != 0 {
		meta["offset"] = p.Offset
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
func (p *Paginator) GetResult(table *sqlike.Table, result interface{}) error {
	query := actions.Paginate().Limit(uint(p.Limit + 1))
	options := options.Paginate().SetDebug(true)
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

	paginator, err := table.Paginate(query, options)
	if err != nil {
		return err
	}

	if p.Cursor != "" {
		if err := paginator.NextPage(p.Cursor); err != nil {
			return err
		}
	}

	slice := reflect.ValueOf(result)
	if err := paginator.All(slice); err != nil {
		return err
	}

	slice = slice.Elem()
	if v := slice.Len(); v > p.Limit {
		key := slice.Index(v - 2).Elem().FieldByName("Key")
		if key.CanInterface() {
			p.Cursor = fmt.Sprintf("%v", key.Interface())
			slice.Set(slice.Slice(0, v-1))
		}
	} else {
		p.Cursor = ""
	}

	return nil
}
