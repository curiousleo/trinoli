package main

import (
	"database/sql"
	"log"
	"net/http"
	"trinoli/internal"

	"github.com/labstack/echo/v4"
	_ "github.com/marcboeker/go-duckdb"
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

func query(db *sql.DB, c echo.Context, query string) error {
	rows, err := db.QueryContext(c.Request().Context(), query)
	if err != nil {
		return c.JSON(http.StatusBadRequest, queryResultsOfError(err))
	}
	json, err := internal.RowsToJson(rows)
	if err != nil {
		return c.JSON(http.StatusBadRequest, queryResultsOfError(err))
	}
	return c.JSONBlob(http.StatusOK, json)
}

func main() {
	db, err := sql.Open("duckdb", "?access_mode=READ_ONLY")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	e := echo.New()
	e.POST("/v1/statement", func(c echo.Context) error {
		return query(db, c, c.QueryParam("query"))
	})
	e.Logger.Fatal(e.Start(":1323"))
}
