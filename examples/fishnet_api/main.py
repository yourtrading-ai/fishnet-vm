import asyncio
import logging
import os
from os import listdir, getenv

from aleph_message.models import PostMessage

logger = logging.getLogger(__name__)

logger.debug("import aleph_client")
from aleph.sdk.vm.cache import VmCache, TestVmCache
from aleph.sdk.vm.app import AlephApp

logger.debug("import aars")
from aars import AARS

logger.debug("import fastapi")
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

logger.debug("import project modules")
from fishnet_cod import *
from .requests import *
import numpy as np

logger.debug("imports done")

http_app = FastAPI()

origins = ["*"]

http_app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if getenv("TEST_CACHE") is not None and getenv("TEST_CACHE").lower() == "true":
    cache = TestVmCache()
else:
    cache = VmCache()
app = AlephApp(http_app=http_app)
aars = AARS(channel="FISHNET_TEST", cache=cache)


async def re_index():
    logger.info("API re-indexing")
    await asyncio.wait_for(AARS.sync_indices(), timeout=None)
    logger.info("API re-indexing done")


@http_app.on_event("startup")
async def startup():
    await re_index()


@app.get("/")
async def index():
    if os.path.exists("/opt/venv"):
        opt_venv = list(listdir("/opt/venv"))
    else:
        opt_venv = []
    return {
        "vm_name": "fishnet_api",
        "endpoints": [
            "/timeseries/upload",
            "/datasets",
            "/user/{address}/datasets",
            "/datasets/upload",
            "/algorithms",
            "/user/{address}/algorithms",
            "/algorithms/upload",
            "/executions",
            "/user/{address}/executions",
        ],
        "files_in_volumes": {
            "/opt/venv": opt_venv,
        },
    }


@app.get("/indices")
async def index():
    ts = [list(index.hashmap.items()) for index in Timeseries.get_indices()]
    ui = [list(index.hashmap.items()) for index in UserInfo.get_indices()]
    ds = [list(index.hashmap.items()) for index in Dataset.get_indices()]
    al = [list(index.hashmap.items()) for index in Algorithm.get_indices()]
    ex = [list(index.hashmap.items()) for index in Execution.get_indices()]
    pe = [list(index.hashmap.items()) for index in Permission.get_indices()]
    return ts, ui, ds, al, ex, pe


@app.get("/indices/reindex")
async def index():
    await re_index()


@app.get("/datasets")
async def datasets(
        view_as: Optional[str] = None,
        by: Optional[str] = None,
        page: Optional[int] = None,
        page_size: Optional[int] = None,
) -> List[Tuple[Dataset, Optional[DatasetPermissionStatus]]]:
    """
    Get all datasets. Returns a list of tuples of datasets and their permission status for the given `view_as` user.
    If `view_as` is not given, the permission status will be `none` for all datasets.
    :param `view_as`: address of the user to view the datasets as and give additional permission information
    :param `by`: address of the dataset owner to filter by
    :param `page_size´: size of the pages to fetch
    :param `page`: page number to fetch
    """
    if by:
        datasets = await Dataset.where_eq(owner=by).all()

    else:
        datasets = await Dataset.fetch_objects().page(page=page, page_size=page_size)

    datasets = await Dataset.fetch_objects().page(page=page, page_size=page_size)
    ts_ids = [rec.timeseriesIDs for rec in datasets]

    ts_ids_np = np.array(ts_ids)
    ts_ids_lists = np.hstack(ts_ids_np)
    ts_ids_unique = np.unique(ts_ids_lists)

    ts_ids_lst = list(ts_ids_unique)

    dataset_by_requestor = await Dataset.where_eq(timeseriesIDs=ts_ids_lst).all()

    returned_datasets = []

    for rec in dataset_by_requestor:
        permission_records = await Permission.where_eq(
            timeseriesID=rec.timeseriesIDs, requestor=view_as
        ).page(page=page, page_size=page_size)

        if not permission_records:
            returned_datasets.append((rec, DatasetPermissionStatus.NOT_REQUESTED))
            continue

        permission_status = [perm_rec.status for perm_rec in permission_records]
        if all(status == PermissionStatus.GRANTED for status in permission_status):
            returned_datasets.append((rec, DatasetPermissionStatus.GRANTED))
        elif PermissionStatus.DENIED in permission_status:
            returned_datasets.append((rec, DatasetPermissionStatus.DENIED))
        elif PermissionStatus.REQUESTED in permission_status:
            returned_datasets.append((rec, DatasetPermissionStatus.REQUESTED))
    return returned_datasets


@app.get("/user/{userAddress}/permissions/incoming")
async def in_permission_requests(
        userAddress: str, page: Optional[int] = None, page_size: Optional[int] = None
) -> List[Permission]:
    permission_records = await Permission.where_eq(owner=userAddress).page(
        page=page, page_size=page_size
    )
    return permission_records


@app.get("/user/{userAddress}/permissions/outgoing")
async def out_permission_requests(
        userAddress: str, page: Optional[int] = None, page_size: Optional[int] = None
) -> List[Permission]:
    permission_records = await Permission.where_eq(requestor=userAddress).page(
        page=page, page_size=page_size
    )
    return permission_records


@app.get("/algorithms")
async def query_algorithms(
        id: Optional[str] = None,
        name: Optional[str] = None,
        by: Optional[str] = None,
        page: Optional[int] = None,
        page_size: Optional[int] = None,
) -> List[Algorithm]:
    """
    - query for own algos
    - query other algos
    - page, page_size and by
    """

    if id:
        algo_id = await Algorithm.fetch(id).page(page=page, page_size=page_size)
        if not algo_id:
            raise HTTPException(status_code=404, detail="No Algorithms found")
        return algo_id

    if name:
        algo_name = await Algorithm.where_eq(name=name).page(
            page=page, page_size=page_size
        )
        if not algo_name:
            raise HTTPException(status_code=404, detail="No Algorithms found")
        return algo_name

    elif by:
        algo_owner = await Algorithm.where_eq(owner=by).page(
            page=page, page_size=page_size
        )
        if not algo_owner:
            raise HTTPException(status_code=404, detail="No Algorithm found")
        return algo_owner

    elif page or page_size:
        return await Algorithm.fetch_objects().page(page=page, page_size=page_size)

    else:
        return await Algorithm.fetch_objects().page(page=1)


@app.get("/user/{address}/algorithms")
async def get_user_algorithms(
        address: str, page: Optional[int] = None, page_size: Optional[int] = None
) -> List[Algorithm]:
    return await Algorithm.where_eq(owner=address).page(page=page, page_size=page_size)


@app.get("/executions")
async def get_executions(
        dataset_id: Optional[str],
        by: Optional[str] = None,
        status: Optional[ExecutionStatus] = None,
        page: Optional[int] = None,
        page_size: Optional[int] = None,
) -> List[Execution]:
    if dataset_id or by or status:
        execution_requests = Execution.where_eq(
            datasetID=dataset_id, owner=by, status=status
        )
    else:
        execution_requests = Execution.fetch_objects().page(
            page=page, page_size=page_size
        )
    if not execution_requests:
        raise HTTPException(status_code=404, detail="No Execution found")
    return await execution_requests


@app.get("/user/{address}/results")
async def get_user_results(
        address: str, page: Optional[int] = None, page_size: Optional[int] = None
) -> List[Result]:
    return await Result.where_eq(owner=address).page(page=page, page_size=page_size)


@app.get("/executions/{execution_id}/possible_execution_count")
async def get_possible_execution_count(execution_id: str) -> int:
    """
    THIS IS AN OPTIONAL ENDPOINT. It is a nice challenge to implement this endpoint, as the code is not trivial, and
    it might be still good to have this code in the future.

    This endpoint returns the number of times the execution can be executed.
    This is the maximum number of times
    the algorithm can be executed on the dataset, given the permissions of each timeseries.
    It can only be executed
    as many times as the least available timeseries can be executed.
    """

    return -1


@app.put("/timeseries/upload")
async def upload_timeseries(req: UploadTimeseriesRequest) -> List[Timeseries]:
    """
    Upload a list of timeseries. If the passed timeseries has an `id_hash` and it already exists,
    it will be overwritten. If the timeseries does not exist, it will be created.
    A list of the created/updated timeseries is returned. If the list is shorter than the passed list, then
    it might be that a passed timeseries contained illegal data.
    """
    ids_to_fetch = [ts.id_hash for ts in req.timeseries if ts.id_hash is not None]
    requests = []
    old_time_series = (
        {ts.id_hash: ts for ts in await Timeseries.fetch(ids_to_fetch).all()}
        if ids_to_fetch
        else {}
    )
    for ts in req.timeseries:
        if old_time_series.get(ts.id_hash) is None:
            requests.append(Timeseries(**dict(ts)).save())
            continue
        old_ts: Timeseries = old_time_series[ts.id_hash]
        if ts.owner != old_ts.owner:
            raise HTTPException(
                status_code=403,
                detail="Cannot overwrite timeseries that is not owned by you",
            )
        old_ts.name = ts.name
        old_ts.data = ts.data
        old_ts.desc = ts.desc
        requests.append(old_ts.save())
    upserted_timeseries = await asyncio.gather(*requests)
    return [ts for ts in upserted_timeseries if not isinstance(ts, BaseException)]


@app.put("/datasets/upload")
async def upload_dataset(dataset: UploadDatasetRequest) -> Dataset:
    """
    Upload a dataset.
    If an `id_hash` is provided, it will update the dataset with that id.
    """
    if dataset.ownsAllTimeseries:
        timeseries = await Timeseries.fetch(dataset.timeseriesIDs).all()
        dataset.ownsAllTimeseries = all(
            [ts.owner == dataset.owner for ts in timeseries]
        )
    if dataset.id_hash is not None:
        old_dataset = await Dataset.fetch(dataset.id_hash).first()
        if old_dataset is not None:
            if old_dataset.owner != dataset.owner:
                raise HTTPException(
                    status_code=403,
                    detail="Cannot overwrite dataset that is not owned by you",
                )
            old_dataset.name = dataset.name
            old_dataset.desc = dataset.desc
            old_dataset.timeseriesIDs = dataset.timeseriesIDs
            old_dataset.ownsAllTimeseries = dataset.ownsAllTimeseries
            return await old_dataset.save()
    return await Dataset(**dataset.dict()).save()


@app.put("/algorithms/upload")
async def upload_algorithm(algorithm: UploadAlgorithmRequest) -> Algorithm:
    """
    Upload an algorithm.
    If an `id_hash` is provided, it will update the algorithm with that id.
    """
    if algorithm.id_hash is not None:
        old_algorithm = await Algorithm.fetch(algorithm.id_hash).first()
        if old_algorithm is not None:
            if old_algorithm.owner != algorithm.owner:
                raise HTTPException(
                    status_code=403,
                    detail="Cannot overwrite algorithm that is not owned by you",
                )
            old_algorithm.name = algorithm.name
            old_algorithm.desc = algorithm.desc
            old_algorithm.code = algorithm.code
            return await old_algorithm.save()
    return await Algorithm(**algorithm.dict()).save()


@app.post("/executions/request")
async def request_execution(
        execution: RequestExecutionRequest,
) -> RequestExecutionResponse:
    """
    This endpoint is used to request an execution.
    If the user needs some permissions, the timeseries for which the user needs permissions are returned and
    the execution status is set to "requested". The needed permissions are also being requested. As soon as the
    permissions are granted, the execution is automatically executed.
    If some timeseries are not available, the execution is "denied" and the execution as well as the
    unavailable timeseries are returned.
    If the user has all permissions, the execution is started and the execution is returned.
    """
    dataset = await Dataset.fetch(execution.datasetID).first()

    if dataset.owner == execution.owner and dataset.ownsAllTimeseries:
        execution.status = ExecutionStatus.PENDING
        return RequestExecutionResponse(
            execution=await Execution(**execution.dict()).save()
        )

    requested_timeseries = await Timeseries.fetch(dataset.timeseriesIDs).all()
    permissions = {
        permission.timeseriesID: permission
        for permission in await Permission.where_eq(
            timeseriesID=dataset.timeseriesIDs, requestor=execution.owner
        ).all()
    }
    requests = []
    unavailable_timeseries = []
    for ts in requested_timeseries:
        if ts.owner == execution.owner:
            continue
        if not ts.available:
            unavailable_timeseries.append(ts)
        if requested_timeseries:
            continue
        if ts.id_hash not in permissions:
            requests.append(
                Permission(
                    timeseriesID=ts.id_hash,
                    algorithmID=execution.algorithmID,
                    owner=ts.owner,
                    requestor=execution.owner,
                    status=PermissionStatus.REQUESTED,
                    executionCount=0,
                    maxExecutionCount=1,
                ).save()
            )
        else:
            permission = permissions[ts.id_hash]
            needs_update = False
            if permission.status == PermissionStatus.DENIED:
                permission.status = PermissionStatus.REQUESTED
                needs_update = True
            if permission.maxExecutionCount <= permission.executionCount:
                permission.maxExecutionCount = permission.executionCount + 1
                permission.status = PermissionStatus.REQUESTED
                needs_update = True
            if needs_update:
                requests.append(permission.save())
    if unavailable_timeseries:
        execution.status = ExecutionStatus.DENIED
        return RequestExecutionResponse(
            execution=await Execution(**execution.dict()).save(),
            unavailableTimeseries=unavailable_timeseries,
        )
    if requests:
        new_permission_requests = await asyncio.gather(*requests)
        execution.status = ExecutionStatus.REQUESTED
        return RequestExecutionResponse(
            execution=await Execution(**execution.dict()).save(),
            permissionRequests=new_permission_requests,
        )
    else:
        execution.status = ExecutionStatus.PENDING
        return RequestExecutionResponse(
            execution=await Execution(**execution.dict()).save()
        )


@app.put("/permissions/approve")
async def approve_permissions(permission_hashes: List[str]) -> List[Permission]:
    """
    Approve permission.
    This EndPoint will approve a list of permissions by their item hashes
    If an 'id_hashes' is provided, it will change all the Permission status
    to 'Granted'.
    """

    ts_ids = []
    requests = []

    permission_records = await Permission.fetch(permission_hashes).all()
    if not permission_records:
        raise HTTPException(
            status_code=404, detail="No Permission Found with this Hashes"
        )

    for rec in permission_records:
        rec.status = PermissionStatus.GRANTED
        ts_ids.append(rec.timeseriesID)
        requests.append(rec.save())

    ds_ids = []
    dataset_records = await Dataset.where_eq(timeseriesIDs=ts_ids).all()
    if not dataset_records:
        raise HTTPException(status_code=404, detail="No Dataset found")
    for rec in dataset_records:
        if rec.id_hash in ds_ids:
            ds_ids.append(rec.id_hash)

    executions_records = await Execution.where_eq(datasetID=ds_ids).all()
    for rec in executions_records:
        if ds_ids and rec.datasetID in ds_ids:
            rec.status = ExecutionStatus.PENDING
            requests.append(rec.save())
    await asyncio.gather(*requests)
    return permission_records


@app.put("/permissions/deny")
async def deny_permissions(permission_hashes: List[str]) -> List[Permission]:
    """
    Deny permission.
    This EndPoint will deny a list of permissions by their item hashes
    If an `id_hashes` is provided, it will change all the Permission status
    to 'Denied'.
    """
    permission_records = await Permission.fetch(permission_hashes).all()
    if not permission_records:
        raise HTTPException(
            status_code=404, detail="No Permission found with this Hashes"
        )

    ts_ids = []
    requests = []
    for rec in permission_records:
        rec.status = PermissionStatus.DENIED
        ts_ids.append(rec.timeseriesID)
        requests.append(rec.save())
    dataset_records = await Dataset.where_eq(timeseriesIDs=ts_ids).all()
    ds_ids = []
    if not dataset_records:
        raise HTTPException(status_code=424, detail="No Timeseries found")
    for rec in dataset_records:
        ds_ids.append(rec.id_hash)
    executions_records = await Execution.where_eq(datasetID=ds_ids).all()
    for rec in executions_records:
        if rec.datasetID in ds_ids and rec.status == ExecutionStatus.PENDING:
            rec.status = ExecutionStatus.DENIED
            requests.append(rec.save())

    await asyncio.gather(*requests)
    return permission_records


@app.put("/datasets/{dataset_id}/available/{available}")
async def set_dataset_available(dataset_id: str, available: bool) -> Dataset:
    """
    Set a dataset to be available or not. This will also update the status of all
    executions that are waiting for permission on this dataset.
    param `dataset_id':put the dataset hash here
    param 'available':put the Boolean value
    """

    requests = []
    dataset = await Dataset.fetch(dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="No Dataset found")
    dataset.available = available
    requests.append(dataset.save())

    ts_list = await Timeseries.fetch(dataset.timeseriesIDs).all()
    if not ts_list:
        raise HTTPException(status_code=424, detail="No Timeseries found")

    for rec in ts_list:
        if rec.available != available:
            rec.available = available
            requests.append(rec.save())
    executions_records = await Execution.fetch(dataset_id).all()
    for rec in executions_records:
        if rec.status == ExecutionStatus.PENDING:
            rec.status = ExecutionStatus.DENIED
            requests.append(rec.save())

    await asyncio.gather(*requests)
    return dataset


filters = [
    {
        "channel": aars.channel,
        "type": "POST",
        "post_type": [
            "Execution",
            "Permission",
            "Dataset",
            "Timeseries",
            "Algorithm",
            "amend",
        ],
    }
]


@app.event(filters=filters)
async def fishnet_event(event: PostMessage):
    print("fishnet_event", event)
    if event.content.type in [
        "Execution",
        "Permission",
        "Dataset",
        "Timeseries",
        "Algorithm",
    ]:
        cls: Record = globals()[event.content.type]
        record = await cls.from_post(event)
    else:
        record = Record.fetch(event.content.ref)
    [index.add_record(record) for index in record.get_indices()]
