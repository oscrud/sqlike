package sqlike

import (
	"errors"
	"reflect"
	"strings"

	"github.com/oscrud/oscrud"
	sql "github.com/si3nloong/sqlike/sqlike"
	"github.com/si3nloong/sqlike/sqlike/actions"
)

// Sqlike :
type Sqlike struct {
	client   *sql.Client
	database *sql.Database
	table    string
	model    oscrud.ServiceModel
}

// NewService :
func NewService(client *sql.Client) *Sqlike {
	return &Sqlike{client: client}
}

// Database :
func (service *Sqlike) Database(db string, table string) *Sqlike {
	service.database = service.client.Database(db)
	service.table = table
	return service
}

// Model :
func (service *Sqlike) Model(model oscrud.ServiceModel) *Sqlike {
	service.model = model
	return service
}

// ToService :
func (service *Sqlike) ToService() Service {
	if service.database == nil || service.table == "" {
		panic("You haven't set database or table by `Database(database_name, table_name)` before transform to service.")
	}

	if service.model == nil {
		panic("You haven't set model by `Model(model_instance)` before transform to service.")
	}

	return Service{
		service.client,
		service.database,
		service.database.Table(service.table),
		service.model,
	}
}

// Service :
type Service struct {
	client   *sql.Client
	database *sql.Database
	table    *sql.Table
	model    oscrud.ServiceModel
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

	model := qm.Interface().(oscrud.ServiceModel)
	data, err := model.ToCreate()
	if err != nil {
		return ctx.Error(400, err).End()
	}

	_, err = service.table.InsertOne(ctx.Context(), data)
	if err != nil {
		return ctx.Stack(500, err).End()
	}

	res, err := model.ToResult()
	if err != nil {
		return ctx.Error(400, err).End()
	}
	return ctx.JSON(200, res).End()
}

// Delete :
func (service Service) Delete(ctx oscrud.Context) oscrud.Context {
	qm := service.newModel()
	if err := ctx.BindAll(qm.Interface()); err != nil {
		return ctx.Stack(500, err).End()
	}

	model := qm.Interface().(oscrud.ServiceModel)
	query, err := model.ToQuery()
	if err != nil {
		return ctx.Error(400, err).End()
	}

	action := actions.FindOne().Where(query)
	result := service.table.FindOne(ctx.Context(), action)
	if err := result.Decode(model); err != nil {
		return ctx.Error(404, err).End()
	}

	delete, err := model.ToDelete()
	if err != nil {
		return ctx.Error(400, err).End()
	}

	if err := service.table.DestroyOne(ctx.Context(), delete); err != nil {
		return ctx.Stack(500, err).End()
	}

	res, err := delete.(oscrud.ServiceModel).ToResult()
	if err != nil {
		return ctx.Error(400, err).End()
	}
	return ctx.JSON(200, res).End()
}

// Patch :
func (service Service) Patch(ctx oscrud.Context) oscrud.Context {
	qm := service.newModel()
	if err := ctx.BindAll(qm.Interface()); err != nil {
		return ctx.Stack(500, err).End()
	}

	incoming := qm.Interface().(oscrud.ServiceModel)
	query, err := incoming.ToQuery()
	if err != nil {
		return ctx.Error(400, err).End()
	}

	model := service.newModel().Interface().(oscrud.ServiceModel)
	action := actions.FindOne().Where(query)
	result := service.table.FindOne(ctx.Context(), action)
	if err := result.Decode(model); err != nil {
		return ctx.Error(404, err).End()
	}

	patch, err := model.ToPatch(incoming)
	if err != nil {
		return ctx.Error(400, err).End()
	}

	if err := service.table.ModifyOne(ctx.Context(), patch); err != nil {
		return ctx.Stack(500, err).End()
	}

	res, err := patch.(oscrud.ServiceModel).ToResult()
	if err != nil {
		return ctx.Error(400, err).End()
	}
	return ctx.JSON(200, res).End()
}

// Update :
func (service Service) Update(ctx oscrud.Context) oscrud.Context {
	qm := service.newModel()
	if err := ctx.BindAll(qm.Interface()); err != nil {
		return ctx.Stack(500, err).End()
	}

	incoming := qm.Interface().(oscrud.ServiceModel)
	query, err := incoming.ToQuery()
	if err != nil {
		return ctx.Error(400, err).End()
	}

	model := service.newModel().Interface().(oscrud.ServiceModel)
	action := actions.FindOne().Where(query)
	result := service.table.FindOne(ctx.Context(), action)
	if err := result.Decode(model); err != nil {
		return ctx.Error(404, err).End()
	}

	update, err := model.ToUpdate(incoming)
	if err != nil {
		return ctx.Error(400, err).End()
	}

	if err := service.table.ModifyOne(ctx.Context(), update); err != nil {
		return ctx.Stack(500, err).End()
	}

	res, err := update.(oscrud.ServiceModel).ToResult()
	if err != nil {
		return ctx.Error(400, err).End()
	}
	return ctx.JSON(200, res).End()
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

	model := qm.Interface().(oscrud.ServiceModel)
	fields := make(map[string]string)
	if query.Select != "" {
		keys := strings.Split(query.Select, ",")
		for _, key := range keys {
			fields[key] = ""
		}
	}

	queries, err := model.ToQuery()
	if err != nil {
		return ctx.Error(400, err).End()
	}

	paginate := Paginator{
		Limit:  1,
		Select: fields,
		Query:  queries,
	}

	slice := service.newModels()
	if err := paginate.GetResult(ctx.Context(), service.table, slice.Interface()); err != nil {
		return ctx.Stack(500, err).End()
	}

	if slice.Elem().Len() == 1 {
		data := slice.Elem().Index(0).Interface().(oscrud.ServiceModel)
		result, err := data.ToResult()
		if err != nil {
			return ctx.Error(400, err).End()
		}
		return ctx.JSON(200, result).End()
	}
	return ctx.Error(404, errors.New("entity not found")).End()
}

// Find :
func (service Service) Find(ctx oscrud.Context) oscrud.Context {
	query := new(oscrud.Query)
	if err := ctx.Bind(query); err != nil {
		return ctx.Error(400, err).End()
	}

	qm := service.newModel()
	if err := ctx.BindAll(qm.Interface()); err != nil {
		return ctx.Error(400, err).End()
	}

	model := qm.Interface().(oscrud.ServiceModel)
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

	queries, err := model.ToQuery()
	if err != nil {
		return ctx.Error(400, err).End()
	}

	paginate := Paginator{
		Cursor: query.Cursor,
		Page:   query.Page,
		Limit:  query.Limit,
		Order:  order,
		Select: fields,
		Query:  queries,
	}

	slice := service.newModels()
	if err := paginate.GetResult(ctx.Context(), service.table, slice.Interface()); err != nil {
		return ctx.Stack(500, err).End()
	}

	data := slice.Elem()
	result := make([]interface{}, data.Len())
	for i := 0; i < data.Len(); i++ {
		result[i], err = data.Index(i).Interface().(oscrud.ServiceModel).ToResult()
		if err != nil {
			return ctx.Error(400, err).End()
		}
	}

	response := map[string]interface{}{
		"meta":   paginate.BuildMeta(),
		"result": result,
	}
	return ctx.JSON(200, response).End()
}
