package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"trinoli/internal"

	"github.com/labstack/echo/v4"
	"github.com/marcboeker/go-duckdb"
)

const (
	initialLimit = 1024
	limitFactor  = 2
	host         = "localhost"
	port         = 1323
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

func queryResultsOfSuccess(columns []internal.Column, data [][]any, nextUri *url.URL) internal.QueryResults {
	var nextUriStr string
	if nextUri != nil {
		nextUriStr = nextUri.String()
	}
	return internal.QueryResults{
		Id:               "",
		InfoUri:          "",
		PartialCancelUri: nil,
		NextUri:          &nextUriStr,
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
	// TODO: Is there a nicer way to LIMIT and OFFSET?
	query = fmt.Sprintf("SELECT * FROM (%s) LIMIT %d OFFSET %d", query, limit, offset)
	conn, err := db.Conn(c.Request().Context())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, queryResultsOfError(err))
	}
	defer conn.Close()

	rows, err := conn.QueryContext(c.Request().Context(), query)
	if err != nil {
		return c.JSON(http.StatusBadRequest, queryResultsOfError(err))
	}
	defer rows.Close()

	columns, data, err := internal.RowsToJson(rows)
	if err != nil {
		return c.JSON(http.StatusBadRequest, queryResultsOfError(err))
	}

	nextUri := c.Request().URL
	if len(data) == limit {
		query := nextUri.Query()
		query.Set("offset", strconv.Itoa(offset+limit))
		query.Set("limit", strconv.Itoa(limit))
		nextUri.RawQuery = query.Encode()
		// TODO
		nextUri.Scheme = "http"
		nextUri.Host = fmt.Sprintf("%s:%d", host, port)
	} else {
		nextUri = nil
	}

	// TODO: Add this profiling info to `StatementStats`
	// info, err := duckdb.GetProfilingInfo(conn)
	// if err != nil {
	// 	return c.JSON(http.StatusInternalServerError, queryResultsOfError(err))
	// }
	// fmt.Printf("CPU time: %s\n", info.Metrics["CPU_TIME"])
	// fmt.Printf("info.Metrics: %v\n", info.Metrics)

	return c.JSON(http.StatusOK, queryResultsOfSuccess(columns, data, nextUri))
}

func openDB(file string) (*sql.DB, error) {
	config := map[string]string{
		"access_mode":                  "READ_ONLY",
		"autoinstall_known_extensions": "false",
		"autoload_known_extensions":    "false",
		"enable_external_access":       "false",
	}
	params := []string{}
	for k, v := range config {
		params = append(params, k+"="+v)
	}
	dsn := file + "?" + strings.Join(params, "&")
	connector, err := duckdb.NewConnector(dsn, func(execer driver.ExecerContext) error {
		queries := []string{
			`PRAGMA enable_profiling = 'no_output'`,
			// `PRAGMA profiling_mode = 'detailed'`,
			// `SET lock_configuration = 'true'`,
		}
		for _, query := range queries {
			_, err := execer.ExecContext(context.Background(), query, nil)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(connector)
	return db, nil
}

func main() {
	db, err := openDB("/mnt/c/Users/Leo/Code/mastr-export/bnetza_mastr_2024-09.duckdb1")
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
	e.Logger.Fatal(e.Start(fmt.Sprintf("%s:%d", host, port)))
}
