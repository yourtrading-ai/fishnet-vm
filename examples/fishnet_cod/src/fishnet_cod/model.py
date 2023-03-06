from enum import Enum
from typing import List, Tuple, Optional, Dict

from aars import Record, Index


class UserInfo(Record):
    datasetIDs: List[str]
    executionIDs: List[str]
    algorithmIDs: List[str]
    username: str
    bio: str


class Timeseries(Record):
    name: str
    owner: str
    desc: Optional[str]
    available: bool = True
    data: List[Tuple[int, float]]


# Check coinmarketcap.com for the exact granularity/aggregation timeframes
class Granularity(str, Enum):
    DAY = "DAY"
    WEEK = "WEEK"
    MONTH = "MONTH"
    THREE_MONTHS = "THREE_MONTHS"
    YEAR = "YEAR"


class View(Record):
    startTime: int
    endTime: int
    granularity: Granularity
    values: Dict[str, List[Tuple[int, float]]]  # timeseriesID -> data


class Dataset(Record):
    name: str
    owner: str
    desc: Optional[str]
    available: bool = True
    ownsAllTimeseries: bool
    timeseriesIDs: List[str]
    views: Optional[List[str]]


class Algorithm(Record):
    name: str
    desc: str
    owner: str
    code: str
    executionIDs: List[str] = []


class ExecutionStatus(str, Enum):
    REQUESTED = "REQUESTED"
    PENDING = "PENDING"
    DENIED = "DENIED"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class Execution(Record):
    algorithmID: str
    datasetID: str
    owner: str
    status: ExecutionStatus = ExecutionStatus.REQUESTED
    resultID: Optional[str]
    params: Optional[dict]


class PermissionStatus(str, Enum):
    REQUESTED = "REQUESTED"
    GRANTED = "GRANTED"
    DENIED = "DENIED"


class DatasetPermissionStatus(str, Enum):
    NOT_REQUESTED = "NOT REQUESTED"
    REQUESTED = "REQUESTED"
    GRANTED = "GRANTED"
    DENIED = "DENIED"


class Permission(Record):
    timeseriesID: str
    algorithmID: Optional[str]
    authorizer: str
    status: PermissionStatus
    executionCount: int
    maxExecutionCount: Optional[int]
    requestor: str


class Result(Record):
    executionID: str
    data: str


# indexes to fetch data for permissions
Index(Permission, "owner")
Index(Permission, ["requestor", "timeseriesID"])
Index(Permission, "timeseriesID")
Index(Permission, ["timeseriesID", "requestor"])
Index(Permission, "requestor")


# indexes to fetch data for Executions
Index(Execution, "owner")
Index(Execution, "datasetID")
Index(Execution, "status")

# indexes to fetch data for Timeseries
Index(Timeseries, "owner")

# indexes to fetch data for Algorithms
Index(Algorithm, "owner")

# indexes to fetch data for Datasets
Index(Dataset, "owner")
Index(Dataset, "timeseriesIDs")

