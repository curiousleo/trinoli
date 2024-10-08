package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"trinoli/internal"

	"github.com/labstack/echo/v4"
	_ "github.com/marcboeker/go-duckdb"
)

const (
	initialLimit = 1024
	limitFactor  = 2
)

func queryResultsOfError(err error) internal.QueryResults {
	message := err.Error()
	return internal.QueryResults{
		Id:               "",
		InfoUri:          "",
		PartialCancelUri: nil,
		NextUri:          nil,
		Columns:          nil,
		Data:             nil,
		Stats:            internal.StatementStats{},
		Error: &internal.QueryError{
			Message:       &message,
			SqlState:      nil,
			ErrorCode:     http.StatusBadRequest,
			ErrorName:     nil,
			ErrorType:     nil,
			ErrorLocation: nil,
			FailureInfo:   nil,
		},
		Warnings:    nil,
		UpdateType:  nil,
		UpdateCount: nil,
	}
}

func queryResultsOfSuccess(columns []internal.Column, data [][]any) internal.QueryResults {
	return internal.QueryResults{
		Id:               "",
		InfoUri:          "",
		PartialCancelUri: nil,
		NextUri:          nil, // TODO: Generate nextUri
		Columns:          columns,
		Data:             data,
		Stats:            internal.StatementStats{},
		Error:            nil,
		Warnings:         nil,
		UpdateType:       nil,
		UpdateCount:      nil,
	}
}

func doQuery(db *sql.DB, c echo.Context, query string, limit int, offset int) error {
	// TODO: Retrieve profiling info
	// https://pkg.go.dev/github.com/marcboeker/go-duckdb@v1.8.1#ProfilingInfo
	// duckdb.GetProfilingInfo(db)
	// TODO: Is there a nicer way to do this?
	query = fmt.Sprintf("SELECT * FROM (%s) LIMIT %d OFFSET %d", query, limit, offset)
	println("query", query)
	rows, err := db.QueryContext(c.Request().Context(), query)
	if err != nil {
		return c.JSON(http.StatusBadRequest, queryResultsOfError(err))
	}
	columns, data, err := internal.RowsToJson(rows)
	if err != nil {
		return c.JSON(http.StatusBadRequest, queryResultsOfError(err))
	}
	return c.JSON(http.StatusOK, queryResultsOfSuccess(columns, data))
}

func main() {
	duckdbConfig := map[string]string{
		"access_mode":                  "READ_ONLY",
		"autoinstall_known_extensions": "false",
		"autoload_known_extensions":    "false",
		"enable_external_access":       "false",
		"lock_configuration":           "true",
	}
	configQuery := []string{}
	for k, v := range duckdbConfig {
		configQuery = append(configQuery, k+"="+v)
	}
	duckdbFile := "/mnt/c/Users/Leo/Code/mastr-export/bnetza_mastr_2024-09.duckdb1"
	dataSourceName := duckdbFile + "?" + strings.Join(configQuery, "&")
	db, err := sql.Open("duckdb", dataSourceName)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	e := echo.New()
	e.POST("/v1/statement", func(c echo.Context) error {
		return doQuery(db, c, c.FormValue("query"), initialLimit, 0)
	})
	e.GET("/fetch", func(c echo.Context) error {
		limit, err := strconv.Atoi(c.QueryParam("limit"))
		if err != nil {
			return c.JSON(http.StatusBadRequest, queryResultsOfError(err))
		}

		offset, err := strconv.Atoi(c.QueryParam("offset"))
		if err != nil {
			return c.JSON(http.StatusBadRequest, queryResultsOfError(err))
		}

		query := c.QueryParam("query")
		return doQuery(db, c, query, limit, offset)
	})
	e.Logger.Fatal(e.Start(":1323"))
}
