package main

import (
	"errors"
	"reflect"
	"strings"

	"github.com/oscrud/oscrud"
	sql "github.com/si3nloong/sqlike/sqlike"
	"github.com/si3nloong/sqlike/sqlike/options"
)

// Sqlike :
type Sqlike struct {
	client   *sql.Client
	database *sql.Database
}

// NewService :
func NewService(client *sql.Client) *Sqlike {
	return &Sqlike{client: client}
}

// Database :
func (service *Sqlike) Database(db string) *Sqlike {
	service.database = service.client.Database(db)
	return service
}

// ToService :
func (service *Sqlike) ToService(table string, model oscrud.DataModel) Service {
	if service.database == nil {
		panic("You set database by `Database()` before transform to service.")
	}

	return Service{
		service.client,
		service.database,
		service.database.Table(table),
		model,
	}
}

// Service :
type Service struct {
	client   *sql.Client
	database *sql.Database
	table    *sql.Table
	model    oscrud.DataModel
}

// internal construct new reflect model
func (service Service) newModel() reflect.Value {
	return reflect.New(reflect.TypeOf(service.model).Elem())
}

// internal construct new reflect slice model
func (service Service) newModels() reflect.Value {
	return reflect.New(reflect.SliceOf(reflect.TypeOf(service.model)))
}

// Create :
func (service Service) Create(ctx oscrud.Context) oscrud.Context {
	qm := service.newModel()
	if err := ctx.BindAll(qm.Interface()); err != nil {
		return ctx.Stack(500, err).End()
	}

	model := qm.Interface().(oscrud.DataModel)
	data := model.ToCreate()
	_, err := service.table.InsertOne(data)
	if err != nil {
		return ctx.Stack(500, err).End()
	}

	return ctx.JSON(200, data).End()
}

// Delete :
func (service Service) Delete(ctx oscrud.Context) oscrud.Context {
	qm := service.newModel()
	if err := ctx.BindAll(qm.Interface()); err != nil {
		return ctx.Stack(500, err).End()
	}

	model := qm.Interface().(oscrud.DataModel)
	if err := service.table.DestroyOne(model); err != nil {
		return ctx.Stack(500, err).End()
	}
	return ctx.JSON(200, model).End()
}

// Patch :
func (service Service) Patch(ctx oscrud.Context) oscrud.Context {
	qm := service.newModel()
	if err := ctx.BindAll(qm.Interface()); err != nil {
		return ctx.Stack(500, err).End()
	}

	model := qm.Interface().(oscrud.DataModel)
	data := model.ToUpdate()
	if _, err := service.table.InsertOne(data, options.InsertOne().SetMode(options.InsertOnDuplicate)); err != nil {
		return ctx.Stack(500, err).End()
	}
	return ctx.JSON(200, data).End()
}

// Update :
func (service Service) Update(ctx oscrud.Context) oscrud.Context {
	qm := service.newModel()
	if err := ctx.BindAll(qm.Interface()); err != nil {
		return ctx.Stack(500, err).End()
	}

	model := qm.Interface().(oscrud.DataModel)
	data := model.ToUpdate()
	if err := service.table.ModifyOne(data); err != nil {
		return ctx.Stack(500, err).End()
	}
	return ctx.JSON(200, data).End()
}

// Get :
func (service Service) Get(ctx oscrud.Context) oscrud.Context {
	query := new(oscrud.QueryOne)
	if err := ctx.Bind(query); err != nil {
		return ctx.Stack(500, err).End()
	}

	qm := service.newModel()
	if err := ctx.BindAll(qm.Interface()); err != nil {
		return ctx.Stack(500, err).End()
	}

	model := qm.Interface().(oscrud.DataModel)
	fields := make(map[string]string)
	if query.Select != "" {
		keys := strings.Split(query.Select, ",")
		for _, key := range keys {
			fields[key] = ""
		}
	}

	paginate := Paginator{
		Limit:  1,
		Select: fields,
		Query:  model.ToQuery(),
	}

	slice := service.newModels()
	if err := paginate.GetResult(service.table, slice.Interface()); err != nil {
		return ctx.Stack(500, err).End()
	}

	if slice.Elem().Len() == 1 {
		data := slice.Elem().Index(0).Interface().(oscrud.DataModel)
		return ctx.JSON(200, data.ToResult()).End()
	}
	return ctx.Error(404, errors.New("entity not found")).End()
}

// Find :
func (service Service) Find(ctx oscrud.Context) oscrud.Context {
	query := new(oscrud.Query)
	if err := ctx.Bind(query); err != nil {
		return ctx.Stack(500, err).End()
	}

	qm := service.newModel()
	if err := ctx.BindAll(qm.Interface()); err != nil {
		return ctx.Stack(500, err).End()
	}

	model := qm.Interface().(oscrud.DataModel)
	order := make(map[string]string)
	if query.Order != "" {
		orders := strings.Split(query.Order, ",")
		lastKey := ""
		for _, key := range orders {
			if strings.ToLower(key) == "desc" {
				order[lastKey] = OrderByDescending
				lastKey = ""
				continue
			}
			order[key] = ""
			lastKey = key
		}
	}

	fields := make(map[string]string)
	if query.Select != "" {
		keys := strings.Split(query.Select, ",")
		for _, key := range keys {
			fields[key] = ""
		}
	}

	paginate := Paginator{
		Cursor: query.Cursor,
		Offset: query.Offset,
		Page:   query.Page,
		Limit:  query.Limit,
		Order:  order,
		Select: fields,
		Query:  model.ToQuery(),
	}

	slice := service.newModels()
	if err := paginate.GetResult(service.table, slice.Interface()); err != nil {
		return ctx.Stack(500, err).End()
	}

	data := slice.Elem()
	result := make([]interface{}, data.Len())
	for i := 0; i < data.Len(); i++ {
		result[i] = data.Index(i).Interface().(oscrud.DataModel).ToResult()
	}

	response := map[string]interface{}{
		"meta":   paginate.BuildMeta(),
		"result": result,
	}
	return ctx.JSON(200, response).End()
}
