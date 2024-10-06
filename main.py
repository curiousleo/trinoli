from datetime import date, datetime
from typing import List, Optional
import duckdb
import duckdb.typing
from fastapi import FastAPI, status
from fastapi.responses import ORJSONResponse
from pydantic import BaseModel
from urllib.parse import urlencode, urlunparse

app = FastAPI(debug=True)


# https://github.com/trinodb/trino/blob/f7a25a1d5c997de24246b17c4269c7f06fd6136e/client/trino-client/src/main/java/io/trino/client/Warning.java#L81-L83
class Code(BaseModel):
    code: int
    name: str


# https://github.com/trinodb/trino/blob/f7a25a1d5c997de24246b17c4269c7f06fd6136e/client/trino-client/src/main/java/io/trino/client/Warning.java#L29-L31
class Warning_(BaseModel):
    warningCode: Code
    message: str


# https://github.com/trinodb/trino/blob/2c3cef5d6f1079c1d9cb03e4626b5b0791600887/client/trino-client/src/main/java/io/trino/client/ErrorLocation.java#L30-L32
class ErrorLocation(BaseModel):
    lineNumber: int
    columnNumber: int


# https://github.com/trinodb/trino/blob/2c3cef5d6f1079c1d9cb03e4626b5b0791600887/client/trino-client/src/main/java/io/trino/client/ErrorInfo.java#L30-L33
class ErrorInfo(BaseModel):
    code: int
    name: str
    type: str


# https://github.com/trinodb/trino/blob/2c3cef5d6f1079c1d9cb03e4626b5b0791600887/client/trino-client/src/main/java/io/trino/client/FailureInfo.java#L39-L46
class FailureInfo(BaseModel):
    type: str
    message: Optional[str]
    cause: Optional["FailureInfo"]
    suppressed: List["FailureInfo"]
    stack: List[str]
    errorInfo: Optional[ErrorInfo]
    errorLocation: Optional[ErrorLocation]


# https://github.com/trinodb/trino/blob/2c3cef5d6f1079c1d9cb03e4626b5b0791600887/client/trino-client/src/main/java/io/trino/client/QueryError.java#L35-L42
class QueryError(BaseModel):
    message: Optional[str]
    sqlState: Optional[str] = None
    errorCode: int
    errorName: Optional[str] = None
    errorType: Optional[str] = None
    errorLocation: Optional[ErrorLocation] = None
    failureInfo: Optional[FailureInfo] = None


# https://github.com/trinodb/trino/blob/2c3cef5d6f1079c1d9cb03e4626b5b0791600887/core/trino-main/src/main/java/io/trino/execution/Column.java#L30-L32
class Column(BaseModel):
    name: str
    type: str


# https://github.com/trinodb/trino/blob/2c3cef5d6f1079c1d9cb03e4626b5b0791600887/client/trino-client/src/main/java/io/trino/client/StageStats.java#L47-L63
class StageStats(BaseModel):
    stageId: Optional[str]
    state: str
    done: bool
    nodes: int
    totalSplits: int
    queuedSplits: int
    runningSplits: int
    completedSplits: int
    cpuTimeMillis: int
    wallTimeMillis: int
    processedRows: int
    processedBytes: int
    physicalInputBytes: int
    failedTasks: int
    coordinatorOnly: bool
    subStages: List["StageStats"]


# https://github.com/trinodb/trino/blob/2c3cef5d6f1079c1d9cb03e4626b5b0791600887/client/trino-client/src/main/java/io/trino/client/StatementStats.java#L52-L73
class StatementStats(BaseModel):
    state: str = ""
    queued: bool = False
    scheduled: bool = False
    progressPercentage: float = 0.0
    runningPercentage: float = 0.0
    nodes: int = 0
    totalSplits: int = 0
    queuedSplits: int = 0
    runningSplits: int = 0
    completedSplits: int = 0
    cpuTimeMillis: int = 0
    wallTimeMillis: int = 0
    queuedTimeMillis: int = 0
    elapsedTimeMillis: int = 0
    processedRows: int = 0
    processedBytes: int = 0
    physicalInputBytes: int = 0
    physicalWrittenBytes: int = 0
    peakMemoryBytes: int = 0
    spilledBytes: int = 0
    rootStage: Optional[StageStats] = None


# TODO: Consider also: UUID, time, timedelta, Decimal
type Value = bool | bytes | date | datetime | float | int | str | None


# https://github.com/trinodb/trino/blob/2c3cef5d6f1079c1d9cb03e4626b5b0791600887/client/trino-client/src/main/java/io/trino/client/QueryResults.java#L49-L61
class QueryResults(BaseModel):
    id: str = ""
    infoUri: str = ""  # really a URI
    partialCancelUri: Optional[str] = None  # really a URI
    nextUri: Optional[str] = None  # really a URI
    columns: Optional[List[Column]] = None

    # https://github.com/trinodb/trino/blob/2c3cef5d6f1079c1d9cb03e4626b5b0791600887/client/trino-client/src/main/java/io/trino/client/QueryData.java#L20-L23
    data: Optional[List[List[Value]]] = None
    stats: StatementStats = StatementStats()
    error: Optional[QueryError] = None
    warnings: Optional[List[Warning_]] = None
    updateType: Optional[str] = None
    updateCount: Optional[int] = None


LIMIT = 10

con = duckdb.connect(
    "C:\\Users\\Leo\\Code\\mastr-export\\bnetza_mastr_2024-09.duckdb1",
    read_only=True,
    config={
        "autoinstall_known_extensions": False,
        "autoload_known_extensions": False,
        "enable_external_access": False,
        "lock_configuration": True,
    },
)
# con.execute("SET enable_external_access = false")


def nextUri(query: str, limit: int, offset: int):
    return urlunparse(
        [
            "http",
            "localhost:8000",
            "/fetch",
            "",
            urlencode({"query": query, "limit": limit, "offset": offset}),
            "",
        ]
    )


def do_query(query: str, offset: int, limit: int):
    global con
    with con.cursor() as cur:
        try:
            rel = cur.sql(query)
        except (duckdb.ParserException, duckdb.InvalidInputException) as e:
            return ORJSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content=QueryResults(
                    error=QueryError(
                        errorCode=status.HTTP_400_BAD_REQUEST, message=str(e)
                    )
                ).model_dump_json(),
            )
        rel = rel.limit(n=limit, offset=offset)
        columns = [
            Column(name=name, type=str(type))
            for name, type in zip(rel.columns, rel.dtypes)
        ]
        data = rel.fetchall()
        nextUriOrNone = (
            nextUri(query, limit=limit * 2, offset=offset + limit)
            if len(data) == limit
            else None
        )
        return ORJSONResponse(
            content=QueryResults(
                columns=columns, data=data, nextUri=nextUriOrNone
            ).model_dump_json()
        )


# TODO: Respect headers
# https://trino.io/docs/current/develop/client-protocol.html#client-request-headers
@app.post("/v1/statement", response_model=QueryResults, response_class=QueryResults)
def run_query(query: str) -> QueryResults:
    return do_query(query, offset=0, limit=LIMIT)


@app.get("/fetch", response_model=QueryResults, response_class=QueryResults)
def fetch(query: str, offset: int, limit: int):
    return do_query(query, offset=offset, limit=limit)
