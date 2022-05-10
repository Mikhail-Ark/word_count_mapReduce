import wordcount_mr_pb2


MAP_DEFAULT_IGNORE_CASE = False
MAP_DEFAULT_N_BUCKETS = 1
MAP_DEFAULT_SORT = False
REDUCE_DEFAULT_MERGE_JOIN = MAP_DEFAULT_SORT
MAP_DEFAULT_OUTPUT_NAME = "mr"
REDUCE_DEFAULT_OUTPUT_NAME = "out"
WAIT_DEFAULT_MILLISECONDS = 500


def request_to_string(request):
    request_list = [
            f"status: {('INIT', 'SUCCESS', 'FAILURE')[request.status]}",
            f"worker_id: {str(request.worker_id)}",
        ]
    if request.output_file_names:
        request_list.append(
            f"output_file_names: {' '.join(request.output_file_names)}"
        )
    if request.warnings:
        request_list.append(
            f"warnings: {' '.join(request.warnings)}"
        )
    return " | ".join(request_list)


def make_map_task(
    input_path, output_path, job_id, input_file_names, n_buckets
):
    return wordcount_mr_pb2.Task(
        type=wordcount_mr_pb2.Task.MAP,
        input_path=input_path,
        output_path=output_path,
        input_file_names=input_file_names,
        job_id=job_id,
        n_buckets=n_buckets
    )


def make_reduce_task(job_id, input_path, output_path):
    return wordcount_mr_pb2.Task(
        type=wordcount_mr_pb2.Task.REDUCE,
        input_path=input_path,
        output_path=output_path,
        job_id=job_id
    )


def make_terminate_task():
    return wordcount_mr_pb2.Task(
        type=wordcount_mr_pb2.Task.TERMINATE,
    )


def make_wait_task(milliseconds=WAIT_DEFAULT_MILLISECONDS):
    return wordcount_mr_pb2.Task(
        type=wordcount_mr_pb2.Task.WAIT, wait_milliseconds=milliseconds
    )


def task_type_to_string(task_type):
    return ('map', 'reduce', 'wait', 'terminate')[task_type]


def unpack_params(request):
    task_type = request.type
    params = dict()
    if request.input_path:
        params["input_path"] = request.input_path
    if request.output_path:
        params["output_path"] = request.output_path
    if request.job_id:
        params["job_id"] = request.job_id
    if request.input_file_names:
        params["input_file_names"] = request.input_file_names
    if request.HasField("output_file_name"):
        params["output_file_name"] = request.output_file_name
    if task_type == wordcount_mr_pb2.Task.MAP:
        if request.HasField("ignore_case"):
            params["ignore_case"] = request.ignore_case
        else:
            params["ignore_case"] = MAP_DEFAULT_IGNORE_CASE
        if request.HasField("sort"):
            params["sort"] = request.sort
        else:
            params["sort"] = MAP_DEFAULT_SORT
        if request.HasField("n_buckets"):
            params["n_buckets"] = request.n_buckets
        else:
            params["n_buckets"] = MAP_DEFAULT_N_BUCKETS
        if "output_file_name" not in params:
            params["output_file_name"] = MAP_DEFAULT_OUTPUT_NAME
    elif task_type == wordcount_mr_pb2.Task.REDUCE:
        if request.HasField("n_buckets"):
            params["merge_join"] = request.merge_join
        else:
            params["merge_join"] = REDUCE_DEFAULT_MERGE_JOIN
        if "output_file_name" not in params:
            params["output_file_name"] = REDUCE_DEFAULT_OUTPUT_NAME
    elif task_type == wordcount_mr_pb2.Task.WAIT:
        if request.HasField("wait_milliseconds"):
            params["wait_milliseconds"] = request.wait_milliseconds
        else:
            params["wait_milliseconds"] = WAIT_DEFAULT_MILLISECONDS
    elif task_type == wordcount_mr_pb2.Task.TERMINATE:
        return task_type, None
    else:
        raise AssertionError("Unknown task type")
    return task_type, params


def make_init_task_request(worker_id):
    return wordcount_mr_pb2.TaskRequest(
        status=wordcount_mr_pb2.TaskRequest.INIT, worker_id=worker_id
    )

