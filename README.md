An example of solving a test task.

The task is in the file map-reduce-task.pdf

#### Brief design description

Distributed mapReduce program to solve word count problem.

A driver creates tasks at the time of startup according to the specified parameters. The tasks themselves are metadata that a worker needs to perform atomic operations of mapReduce. The worker requests a task from the driver (unary grpc-call) and after execution requests for a new task in a loop. The driver keeps records of the order of execution and the statuses of tasks. When all tasks are completed, the driver waits for repeated calls of all known workers to send them a signal to terminate and terminates itself afterwards.

#### Example of running the program

`./wcmr.sh ./files/inputs/testing/final 6 4 3`
> `./files/inputs/testing/final` path to an input dir
>
>`6` number of map tasks (N)
>
>`4` number of reduce tasks (M)
>
>`3` number of workers

#### You can run driver separately

`python driver.py`

>`--n_map any_int` or `-N any_int` number of map tasks
>
>`--n_reduce any_int` or `-M any_int` number of reduce tasks
>
>`--input_path path_str` path to dir with input files
>
>`--intermediate_path path_str` path to dir for intermediate files
>
>`--output_path path_str` path to dir for output files
>
>`--intermediate_prefix path_str` prefix for intermediate files
>
>`--output_prefix prefix_str` prefix for output files
>
>`--ignore_case` or `-i` ignore case in all tasks
>
>`--sort` or `-s` add sort after map and use merge join in reduce
>
>`--wait` or `-w` time for wait tasks in milliseconds


#### You can run worker separately

`python worker.py`

>`--worker_id any_int` or `-i any_int` to specify worker id
>
>`--sleepy` or `-s` to add sleep to received tasks (heavy task imitation)


#### You can run the tests with

`python testing.py`

#### Future work

Thanks to the extra time, I was able to complete the task.
There are many ways to improve the program, some of them are listed below.

- Improve the split of files into map tasks. The current split scheme does not take into account the size of files, as well as the situation in which the number of incoming files is less than the number of jobs/workers. To evenly distribute the load, we can divide large files into parts. For more efficient execution, we do not even want to equalize tasks, but to minimize an idleness of workers at the time of transition from the map stage to the reduce stage.In this case, we can write a combinatorial-heuristic or linearly optimising algorithm that will distribute the pieces of files so that the workers finish the task at the same time. If desired, we can even take into account the statistics of the speed of execution of tasks by different workers.
- Improve bucketing. Given that the distribution of the first letters of words in texts is not uniform, it is possible to make a more balanced sharding (if we intend to exclude the possibility of sharding by hash). We can calculate the coefficients of occurrence of the first letters and try to split them into buckets more evenly. But if there is no goal to equalize the execution time of tasks, then we can try to optimize the total execution time of the reduce stage by sorting the order of execution from a larger task (by the sum of coefficients or simply by the number of rows) to a smaller one.
- Consider the possibility of starting the execution of the reduce stage before the completion of the map stage. Since by default, the reduce does not require all intermediate files to be present (files are read one by one), we can initiate reduce task and send files once any map task is complited.
- Add warnings. It is possible to increase the verbality of execution due to known potentially faulty conditions (for example, empty input files). Usiage of the program will become more convenient.
- Make tokenization better. The case-sensitivity flag is implemented, others can be added, in order to be more like like nltk or gensim.
- Compare the effectiveness of the sorted and unsorted mapReduce approach. The mapReduce execution scheme proposed in the exercise is as follows: we flatten incoming texts into lists of words in one task, and then aggregate the lists in another. Strictly speaking, this scheme differs from the classic mapReduce with sorting and subsequent merge join. In the first case, we need more memory in the reduce, and in the second in the map (sorting after map demands memory to be more precise). The proposed scheme looks reasonable, but its superiority is not obvious and requires a proof. The answer to the question "which approach is better for this task?" will depend on the requirements, as well as on the input data. For example, during exploitation, it may appear that the data comes to us is partially or completely sorted, and all the words are unique. Therefore, it is necessary to measure resource consumption, although the choice of approach was given to user.
- Implement and add flag for map tasks to produce aggregated reduce-like output. Although the task directly says not to implement this feature, it increases the effectiveness of the classic mapReduce, and also helps in extreme cases, such as a large number of identical repeated words at the input.
- Add caching. In general, the data indirectly tells us that tasks can be repeatative. If this is the case, then depending on the completeness of the repetition, it may be reasonable to add caching at the input to the driver (if the dataset is completely repeated) or at the preparation of map tasks (if part of the dataset is repeated, or there are duplicate files in the set itself). The cache for the reduce does not make sense, because if at least one file is not in the map cache, then the reduce will have to be recalculated, elif all the files are in the cache, then the cache of the reduce will appear only if the program itself was called with these parameters, and the answer will be in the driver cache. Given the relatively short execution time of the task, the cache may seem meaningless. The reasonability depends on the data. In addition, as the complexity of tokenization increases, resource consumption may increase significantly, especially when it comes to fusional languages like Spanish or Russian. + Note: When writing a cache, it is necessary to take into account the parameters with which it was received.
- A significant upgrade will be an improvement in fault tolerance with better error and crashes handling. In the current code, the worker's possible crushes are only partially processed. In addition, there is a problem that if the worker for some reason has never managed to get a task, then the driver may terminate before sending the worker a signal for termination.

#### Reviewers' comments:
- Dividing a file into parts can be made easier by passing the path to the file and an offset.
- Too intensive testing, does not look like a real production project.
