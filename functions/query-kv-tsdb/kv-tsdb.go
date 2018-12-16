package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/nuclio/nuclio-sdk-go"
	"github.com/pkg/errors"
	"github.com/v3io/v3io-go-http"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/formatter"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
)

type TableOutput struct {
	Columns []map[string]string `json:"columns"`
	Rows    [][]interface{}     `json:"rows"`
	Type    string              `json:"type"`
}

type RequestSimpleJson struct {
	PanelId       int                      `json:"panelId"`
	Range         map[string]interface{}   `json:"range"`
	RangeRaw      map[string]string        `json:"rangeRaw"`
	Interval      string                   `json:"interval"`
	IntervalMs    int                      `json:"intervalMs"`
	Targets       []map[string]interface{} `json:"targets"`
	Target        string                   `json:"target"`
	Format        string                   `json:"format"`
	MaxDataPoints int                      `json:"maxDataPoints"`
	Path          string                   `json:"path"`
}

type RequestHandler struct {
	request *RequestSimpleJson
	context *nuclio.Context
}

type tsdbQuery struct {
	Name        string
	Aggregators []string
	Step        int64
	Filter      string
	From        int64
	To          int64
}

type outputFormatter interface {
	Write(set []map[string]interface{}) ([]byte, error)
}

type simpleJsonOutputFormatter struct {
	format string
	filter *InputFilter
}

type tableOutputFormatter struct {
	format string
	filter *InputFilter
}

func QueryKvTsdb(context *nuclio.Context, event nuclio.Event) (interface{}, error) {
	var request RequestSimpleJson
	if len(event.GetBody()) > 0 {
		err := json.Unmarshal(event.GetBody(), &request)
		context.Logger.Info(request)
		if err != nil {
			return nil, err
		}
	}
	request.Path = event.GetPath()
	// TODO: do something with the body to build the path, attrs list & filter string

	requestHandler := RequestHandler{&request, context}
	retval, err := requestHandler.Invoke()
	if err != nil {
		return nil, err
	}
	return []byte(retval), nil
}

func (requestHandler *RequestHandler) Invoke() ([]byte, error) {
	path := strings.Trim(requestHandler.request.Path, "/")
	pathChuncks := strings.Split(path, "/")
	interf := pathChuncks[0]
	method := ""
	methodIndex := len(pathChuncks) - 1
	if methodIndex > 0 {
		method = pathChuncks[methodIndex]
		if !strInArr([]string{"query", "search"}, method) {
			method = ""
			methodIndex = len(pathChuncks)
		}
	}
	if method == "query" {
		return requestHandler.HandleQuery(interf)
	}
	if method == "search" {
		return requestHandler.HandleSearch(interf)
	}
	if len(method) > 0 {
		panic(errors.New("Unknown method: " + method))
	}
	return []byte(""), nil
}

func (requestHandler *RequestHandler) HandleQuery(iterf string) ([]byte, error) {
	filters := *parseQueryFilters(&requestHandler.request.Targets)
	// allow, for now, only one series type
	timeseriesFilter := filters["timeserie"]
	if timeseriesFilter != nil {
		return requestHandler.getTimeseriesData(timeseriesFilter, iterf)
	}
	tableFilter := filters["table"]
	if tableFilter != nil {
		return requestHandler.getTableData(tableFilter, iterf)
	}
	return nil, errors.New("Unknown output format")
}

func (requestHandler *RequestHandler) HandleSearch(interf string) ([]byte, error) {
	// TODO: use supplied range to filter out results
	//placeholder for support other interfaces
	if interf == "tsdb" {
		return requestHandler.getTsdbSearchData()
	}
	return requestHandler.getV3ioSearchData()
}

func (requestHandler *RequestHandler) getTimeseriesData(filter *InputFilter, dataType string) ([]byte, error) {
	//timeseries format
	dbName := filter.tableName
	if dataType == "tsdb" {
		return requestHandler.getTsdbTimeseriesData(filter, dbName)
	}
	if dataType == "v3io" {
		return requestHandler.getV3ioTimeseriesData(filter, dbName)
	}
	if dataType == "table" {
		return requestHandler.getTableTimeseriesData(filter, dbName)
	}
	return nil, errors.New("Unknown method: " + dataType)
}

func (requestHandler *RequestHandler) getV3ioTimeseriesData(filter *InputFilter, dbName string) ([]byte, error) {
	rangeMs, _ := requestHandler.getRangeMs()
	attrNames := append(filter.fields, "timestamp")
	query := fmt.Sprintf("((`timestamp`>=%v AND `timestamp`<=%v) OR NOT exists(timestamp))", rangeMs[0], rangeMs[1])
	if filter != nil && len(filter.filter) > 0 {
		query += " AND " + filter.filter
	}
	iter, err := initV3ioCursor(requestHandler.context, dbName, query, attrNames)
	if err != nil {
		return nil, errors.Wrap(err, "failed to init v3io iterator")
	}
	outputData := []map[string]interface{}{}
	outputDataMap := map[string][][]interface{}{}
	datapoints := [][]interface{}{}
	filtersFields := filter.fields
	for iter.Next() {
		timestamp := iter.GetField("timestamp")
		data := iter.GetFields()
		if data != nil {
			for field, val := range data {
				if strInArr(filtersFields, field) {
					if outputDataMap[field] == nil {
						outputDataMap[field] = datapoints
					}
					outputDataMap[field] = append(outputDataMap[field], []interface{}{val, timestamp})
				}
			}
		}
	}
	for k, v := range outputDataMap {
		sort.Slice(v, func(i, j int) bool {
			return v[i][1].(int) < v[j][1].(int)
		})
		outputData = append(outputData, map[string]interface{}{"target": k, "datapoints": v})
	}
	return json.Marshal(outputData)
}

func (requestHandler *RequestHandler) getTableTimeseriesData(filter *InputFilter, dbName string) ([]byte, error) {
	return nil, nil
}

func (requestHandler *RequestHandler) getTsdbTimeseriesData(filter *InputFilter, dbName string) ([]byte, error) {
	tsdbQueries, err := requestHandler.prepTsdbQueries(filter)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create tsdb queries")
	}
	for _, query := range tsdbQueries {
		// Create TSDB Querier
		requestHandler.context.Logger.DebugWith("Query", "params", query)
		adapter, err := initTsdbAdapter(os.Getenv("V3IO_URL"), os.Getenv("CONTAINER_ID"), dbName)
		if err != nil {
			return nil, errors.Wrap(err, "Failed to initialize Adapter")
		}
		qry, err := adapter.Querier(nil, query.From, query.To)
		if err != nil {
			return nil, errors.Wrap(err, "Failed to initialize Querier")
		}
		// Select Query to get back a series set iterator
		set, err := qry.Select(query.Name, strings.Join(query.Aggregators, ","), query.Step, query.Filter)
		if err != nil {
			return nil, errors.Wrap(err, "Select Failed")
		}
		// convert SeriesSet to Json (Grafana simpleJson format)
		f, err := formatter.NewFormatter("json", nil)
		if err != nil {
			return nil, errors.Wrap(err, "failed to start json formatter")
		}
		var b bytes.Buffer
		err = f.Write(&b, set)
		return b.Bytes(), err
	}
	return nil, nil
}

func (requestHandler *RequestHandler) getTableData(filter *InputFilter, interf string) ([]byte, error) {
	rangeMs, _ := requestHandler.getRangeMs()
	dbName := filter.tableName
	query := fmt.Sprintf("((`timestamp`>=%v AND `timestamp`<=%v) OR NOT exists(timestamp))", rangeMs[0], rangeMs[1])
	if filter != nil && len(filter.filter) > 0 {
		query += " AND " + filter.filter
	}
	iter, err := initV3ioCursor(requestHandler.context, dbName, query, filter.fields)
	if err != nil {
		return nil, errors.Wrap(err, "failed to init v3io iterator")
	}
	outputDataArr := []map[string]interface{}{}
	for iter.Next() {
		data := iter.GetFields()
		if data != nil {
			outputDataArr = append(outputDataArr, data)
		}
	}

	formatter, err := newOutFormatter(interf, filter, "table")
	if err != nil {
		return nil, err
	}
	return formatter.Write(outputDataArr)
}

func (requestHandler *RequestHandler) getV3ioSearchData() ([]byte, error) {
	// TODO: use supplied range to filter out results
	filter := *parseSearchFilter(&requestHandler.request.Target)
	dbName := filter.tableName
	fields := []string{}
	if dbName != "" {
		iter, err := initV3ioCursor(requestHandler.context, dbName, filter.filter, filter.fields)
		if err != nil {
			return nil, errors.Wrap(err, "failed to init v3io iterator")
		}
		if len(filter.fields) > 0 {
			for iter.Next() {
				for _, field := range filter.fields {
					fields = append(fields, fmt.Sprintf("%v", iter.GetFields()[field]))
				}
			}
		} else {
			if iter.Next() {
				for field := range iter.GetFields() {
					fields = append(fields, field)
				}
			}
		}
	}
	retval, err := json.Marshal(fields)
	return []byte(retval), err
}

func (requestHandler *RequestHandler) getTsdbSearchData() ([]byte, error) {
	// TODO: use supplied range to filter out results
	filter := *parseSearchFilter(&requestHandler.request.Target)
	adapter, err := initTsdbAdapter(os.Getenv("V3IO_URL"), os.Getenv("CONTAINER_ID"), filter.tableName)
	qry, err := adapter.Querier(nil, 0, 0)
	if err != nil {
		return nil, err
	}
	fields, err := qry.LabelValues("__name__")
	if err != nil {
		return nil, err
	}
	requestHandler.context.Logger.Info(fields)
	retval, err := json.Marshal(fields)
	return []byte(retval), err
}

func (requestHandler *RequestHandler) prepTsdbQueries(filter *InputFilter) ([]tsdbQuery, error) {
	var retval []tsdbQuery
	var err error
	rangeMs, _ := requestHandler.getRangeMs()
	for _, field := range filter.fields {
		newQuery := tsdbQuery{Name: field, Filter: filter.filter, From: rangeMs[0], To: rangeMs[1]}
		retval = append(retval, newQuery)
	}
	return retval, err
}

func initTsdbAdapter(url string, containerId string, path string) (*tsdb.V3ioAdapter, error) {
	tsdbConfig, _ := json.Marshal(map[string]string{"webApiEndpoint": url, "container": containerId, "tablePath": path, "username": os.Getenv("USERNAME"), "password": os.Getenv("PASSWORD")})
	cfg, _ := config.GetOrLoadFromData([]byte(tsdbConfig))
	return tsdb.NewV3ioAdapter(cfg, nil, nil)
}

func initV3ioCursor(context *nuclio.Context, dbName string, filter string, attrNames []string) (*v3io.SyncItemsCursor, error) {
	container, err := createContainer(context, os.Getenv("V3IO_URL"), os.Getenv("CONTAINER_ID"))
	if err != nil {
		context.Logger.ErrorWith("Failed to createContainer", "err", err)
		return nil, errors.Wrap(err, "Failed to createContainer")
	}
	if len(attrNames) == 0 {
		attrNames = []string{"*"}
	}
	input := v3io.GetItemsInput{Path: dbName + "/", AttributeNames: attrNames, Filter: filter}
	return container.Sync.GetItemsCursor(&input)
}

func createContainer(nuclioContext *nuclio.Context, addr, cont string) (*v3io.Container, error) {
	// create context
	context, err := v3io.NewContext(nuclioContext.Logger, addr, 8)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create client")
	}
	// create session
	session, err := context.NewSession(os.Getenv("USERNAME"), os.Getenv("PASSWORD"), "v3test")
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create session")
	}
	// create the container
	container, err := session.NewContainer(cont)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create container")
	}
	return container, nil
}

func (requestHandler *RequestHandler) getRangeTime() ([2]time.Time, error) {
	var retval [2]time.Time
	var err error
	timestampStrRange := []string{requestHandler.request.Range["from"].(string), requestHandler.request.Range["to"].(string)}
	for i, timestamStr := range timestampStrRange {
		retval[i], err = time.Parse("2006-01-02T15:04:05.999999999Z07:00", timestamStr)
		if err != nil {
			return retval, err
		}
	}
	return retval, nil
}

func (requestHandler *RequestHandler) getRangeMs() ([2]int64, error) {
	var retval [2]int64
	rangeTime, err := requestHandler.getRangeTime()
	if err != nil {
		return retval, err
	}
	for i, timestamp := range rangeTime {
		retval[i] = timestamp.Round(time.Millisecond).UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond))
	}
	return retval, nil
}

func (requestHandler *RequestHandler) isInRange(timestamp int64) bool {
	rangeMs, _ := requestHandler.getRangeMs()
	return rangeMs[0] <= timestamp && timestamp <= rangeMs[1]
}

type InputFilter struct {
	filter    string
	fields    []string
	tableName string
}

func parseQueryFilters(filtersStr *[]map[string]interface{}) *map[string]*InputFilter {
	filters := map[string]*InputFilter{}
	for _, target := range *filtersStr {
		filterType := target["type"].(string)
		if filters[filterType] == nil {
			filters[filterType] = &InputFilter{}
		}
		fieldInput := target["target"].(string)
		populateFilter(filters[filterType], fieldInput)
	}
	return &filters
}

func populateFilter(filter *InputFilter, fieldInput string) {
	toReplace := "()[]\\"
	args := []string{}
	for _, arg := range toReplace {
		args = append(args, []string{fmt.Sprintf("%c", arg), ""}...)
	}
	replacer := strings.NewReplacer(args...)
	fieldInput = replacer.Replace(fieldInput)
	re := regexp.MustCompile(`^\s*(filter|fields|table_name)\s*=\s*(.*)\s*$`)
	match := re.FindStringSubmatch(fieldInput)
	if len(match) > 2 {
		switch match[1] {
		case "filter":
			filter.filter = match[2]
			break
		case "fields":
			for _, field := range strings.Split(match[2], ",") {
				filter.fields = append(filter.fields, strings.TrimSpace(field))
			}
			break
		case "table_name":
			filter.tableName = match[2]
			break
		}

	} else {
		filter.fields = append(filter.fields, strings.TrimSpace(fieldInput))
	}
}

func parseSearchFilter(filtersStr *string) *InputFilter {
	filter := InputFilter{}
	for _, target := range strings.Split(*filtersStr, ",") {
		fmt.Println(target)
		populateFilter(&filter, strings.TrimSpace(target))
	}
	return &filter
}

func strInArr(arr []string, strSearch string) bool {
	for _, strInArr := range arr {
		if strInArr == strSearch {
			return true
		}
	}
	return false
}

func newOutFormatter(protocol string, filter *InputFilter, format string) (outputFormatter, error) {
	switch protocol {
	case "table":
		return tableOutputFormatter{format: format, filter: filter}, nil
	case "json", "v3io":
		return simpleJsonOutputFormatter{format: format, filter: filter}, nil

	default:
		return nil, fmt.Errorf("unknown formatter type %s", format)
	}
}

func (formatter tableOutputFormatter) Write(set []map[string]interface{}) ([]byte, error) {
	return json.Marshal(set)
}

func (formatter simpleJsonOutputFormatter) Write(set []map[string]interface{}) ([]byte, error) {
	if formatter.format == "table" {
		var outputData TableOutput
		columns := []map[string]string{}
		// columnsKeys := []string{}
		columnsArr := map[string]string{}
		//assume all rows have the same colums
		if len(set) > 0 {
			firstRow := set[0]
			for field, val := range firstRow {
				valTypeStr, _ := parseValue(val)
				columnsArr[field] = valTypeStr
				// columnsKeys = append(columnsKeys, field)
			}
		}
		// sort.Strings(columnsKeys)
		for _, column := range formatter.filter.fields {
			columns = append(columns, map[string]string{"text": column, "type": columnsArr[column]})
		}
		outputData.Columns = columns
		// populate the map using the right format (table)
		rows := [][]interface{}{}
		for _, entry := range set {
			row := []interface{}{}
			for _, field := range columns {
				row = append(row, entry[field["text"]])
			}
			rows = append(rows, row)
		}
		outputData.Rows = rows
		outputData.Type = "table"
		return json.Marshal([]TableOutput{outputData})
	}
	// TODO: implement timeserie format
	return nil, nil
}

func parseValue(val interface{}) (string, interface{}) {
	valType := reflect.TypeOf(val).String()
	v := val
	valTypeStr := valType
	switch valType {
	case "int":
		valTypeStr = "number"
		v = val.(int)
	case "string":
		v, err := strconv.ParseFloat(val.(string), 64)
		if err == nil {
			fmt.Println(v)
			valTypeStr = "number"
		}
	}
	return valTypeStr, v
}

func pop(m *map[string]interface{}, key string) (interface{}, bool) {
	v, ok := (*m)[key]
	if ok {
		delete(*m, key)
	}
	return v, ok
}
