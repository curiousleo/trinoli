package internal

type Code struct {
	Code int    `json:"code"`
	Name string `json:"name"`
}

// https://github.com/trinodb/trino/blob/f7a25a1d5c997de24246b17c4269c7f06fd6136e/client/trino-client/src/main/java/io/trino/client/Warning.java//L29-L31
type Warning_ struct {
	WarningCode Code   `json:"warningCode"`
	Message     string `json:"message"`
}

// https://github.com/trinodb/trino/blob/2c3cef5d6f1079c1d9cb03e4626b5b0791600887/client/trino-client/src/main/java/io/trino/client/ErrorLocation.java//L30-L32
type ErrorLocation struct {
	LineNumber   int `json:"lineNumber"`
	ColumnNumber int `json:"columnNumber"`
}

// https://github.com/trinodb/trino/blob/2c3cef5d6f1079c1d9cb03e4626b5b0791600887/client/trino-client/src/main/java/io/trino/client/ErrorInfo.java//L30-L33
type ErrorInfo struct {
	Code int    `json:"code"`
	Name string `json:"name"`
	Type string `json:"type"`
}

// https://github.com/trinodb/trino/blob/2c3cef5d6f1079c1d9cb03e4626b5b0791600887/client/trino-client/src/main/java/io/trino/client/FailureInfo.java#L39-L46
type FailureInfo struct {
	Type          string         `json:"type"`
	Message       *string        `json:"message"`
	Cause         *FailureInfo   `json:"cause"`
	Suppressed    []FailureInfo  `json:"suppressed"`
	Stack         []string       `json:"stack"`
	ErrorInfo     *ErrorInfo     `json:"errorInfo"`
	ErrorLocation *ErrorLocation `json:"errorLocation"`
}

// https://github.com/trinodb/trino/blob/2c3cef5d6f1079c1d9cb03e4626b5b0791600887/client/trino-client/src/main/java/io/trino/client/QueryError.java#L35-L42
type QueryError struct {
	Message       *string        `json:"message"`
	SqlState      *string        `json:"sqlState"`
	ErrorCode     int            `json:"errorCode"`
	ErrorName     *string        `json:"errorName"`
	ErrorType     *string        `json:"errorType"`
	ErrorLocation *ErrorLocation `json:"errorLocation"`
	FailureInfo   *FailureInfo   `json:"failureInfo"`
}

// https://github.com/trinodb/trino/blob/2c3cef5d6f1079c1d9cb03e4626b5b0791600887/core/trino-main/src/main/java/io/trino/execution/Column.java#L30-L32
type Column struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// https://github.com/trinodb/trino/blob/2c3cef5d6f1079c1d9cb03e4626b5b0791600887/client/trino-client/src/main/java/io/trino/client/StageStats.java#L47-L63
type StageStats struct {
	StageId            *string      `json:"stageId"`
	State              string       `json:"state"`
	Done               bool         `json:"done"`
	Nodes              int          `json:"nodes"`
	TotalSplits        int          `json:"totalSplits"`
	QueuedSplits       int          `json:"queuedSplits"`
	RunningSplits      int          `json:"runningSplits"`
	CompletedSplits    int          `json:"completedSplits"`
	CpuTimeMillis      int          `json:"cpuTimeMillis"`
	WallTimeMillis     int          `json:"wallTimeMillis"`
	ProcessedRows      int          `json:"processedRows"`
	ProcessedBytes     int          `json:"processedBytes"`
	PhysicalInputBytes int          `json:"physicalInputBytes"`
	FailedTasks        int          `json:"failedTasks"`
	CoordinatorOnly    bool         `json:"coordinatorOnly"`
	SubStages          []StageStats `json:"subStages"`
}

// https://github.com/trinodb/trino/blob/2c3cef5d6f1079c1d9cb03e4626b5b0791600887/client/trino-client/src/main/java/io/trino/client/StatementStats.java#L52-L73
type StatementStats struct {
	State                string      `json:"state"`
	Queued               bool        `json:"queued"`
	Scheduled            bool        `json:"scheduled"`
	ProgressPercentage   float32     `json:"progressPercentage"`
	RunningPercentage    float32     `json:"runningPercentage"`
	Nodes                int         `json:"nodes"`
	TotalSplits          int         `json:"totalSplits"`
	QueuedSplits         int         `json:"queuedSplits"`
	RunningSplits        int         `json:"runningSplits"`
	CompletedSplits      int         `json:"completedSplits"`
	CpuTimeMillis        int         `json:"cpuTimeMillis"`
	WallTimeMillis       int         `json:"wallTimeMillis"`
	QueuedTimeMillis     int         `json:"queuedTimeMillis"`
	ElapsedTimeMillis    int         `json:"elapsedTimeMillis"`
	ProcessedRows        int         `json:"processedRows"`
	ProcessedBytes       int         `json:"processedBytes"`
	PhysicalInputBytes   int         `json:"physicalInputBytes"`
	PhysicalWrittenBytes int         `json:"physicalWrittenBytes"`
	PeakMemoryBytes      int         `json:"peakMemoryBytes"`
	SpilledBytes         int         `json:"spilledBytes"`
	RootStage            *StageStats `json:"rootStage"`
}

// // TODO: Consider also: UUID, time, timedelta, Decimal
// type Value

// https://github.com/trinodb/trino/blob/2c3cef5d6f1079c1d9cb03e4626b5b0791600887/client/trino-client/src/main/java/io/trino/client/QueryResults.java#L49-L61
type QueryResults struct {
	Id               string   `json:"id"`
	InfoUri          string   `json:"infoUri"`
	PartialCancelUri *string  `json:"partialCancelUri"`
	NextUri          *string  `json:"nextUri"`
	Columns          []Column `json:"columns"`

	// https://github.com/trinodb/trino/blob/2c3cef5d6f1079c1d9cb03e4626b5b0791600887/client/trino-client/src/main/java/io/trino/client/QueryData.java#L20-L23
	Data        [][]any        `json:"data"`
	Stats       StatementStats `json:"stats"`
	Error       *QueryError    `json:"error"`
	Warnings    []Warning_     `json:"warnings"`
	UpdateType  *string        `json:"updateType"`
	UpdateCount *int           `json:"updateCount"`
}
