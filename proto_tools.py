import wordcount_mr_pb2


FIELDS = {
    wordcount_mr_pb2.Task.MAP: (
        "input_path", "output_path", "job_id", "input_file_names",
        "output_prefix", "ignore_case", "sort", "n_buckets"
    ),
    wordcount_mr_pb2.Task.REDUCE: (
        "input_path", "output_path", "job_id", "output_prefix", "merge_join",
    ),
    wordcount_mr_pb2.Task.WAIT: ("wait_milliseconds",),
    wordcount_mr_pb2.Task.TERMINATE: tuple(),
}


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
    input_path, output_path, job_id, input_file_names, n_buckets,
    output_prefix, ignore_case, sort
):
    return wordcount_mr_pb2.Task(
        type=wordcount_mr_pb2.Task.MAP,
        input_path=input_path,
        output_path=output_path,
        input_file_names=input_file_names,
        job_id=job_id,
        output_prefix=output_prefix,
        ignore_case=ignore_case,
        sort=sort,
        n_buckets=n_buckets
    )


def make_reduce_task(
    job_id, input_path, output_path, output_prefix, merge_join
):
    return wordcount_mr_pb2.Task(
        type=wordcount_mr_pb2.Task.REDUCE,
        input_path=input_path,
        output_path=output_path,
        job_id=job_id,
        output_prefix=output_prefix,
        merge_join=merge_join
    )


def make_terminate_task():
    return wordcount_mr_pb2.Task(
        type=wordcount_mr_pb2.Task.TERMINATE,
    )


def make_wait_task(milliseconds):
    return wordcount_mr_pb2.Task(
        type=wordcount_mr_pb2.Task.WAIT, wait_milliseconds=milliseconds
    )


def task_type_to_string(task_type):
    return ('map', 'reduce', 'wait', 'terminate')[task_type]


def unpack_params(request):
    task_type = request.type
    assert task_type in FIELDS, "Unknown task type"
    params = dict()
    for field in FIELDS[task_type]:
        try:
            assert request.HasField(field), f"no {field} param"
        except ValueError:
            assert getattr(request, field), f"no {field} param"
        params[field] = getattr(request, field)
    return task_type, params


def make_init_task_request(worker_id):
    return wordcount_mr_pb2.TaskRequest(
        status=wordcount_mr_pb2.TaskRequest.INIT, worker_id=worker_id
    )
