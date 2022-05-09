
Hello benshi.ai team,

Thank you for this opportunity! I will be glad to tell you about the advantages
of my implementation on the review, as well as to get any feedback.

Example of running a program:
python worker.py & python driver.py -N 6 -M 4

Running tests (which, among other checks, executes the command mentioned above):
python testing.py

Unfortunately, I didn't manage to complete the task entirely within the allotted
time, so I wrote a detailed TODO-list. If the work done at the moment
is not enough to successfully proceed to the next stage, then please write to me
and give me a little more time and I will refine the code to meet your
requirements.


# TODO:
- Work with multiple servers.
- Load balancing.
- Improve the split of files into map tasks. The current split scheme does not
take into account the size of files, as well as the situation in which
the number of incoming files is less than the number of jobs/workers. To evenly
distribute the load, we can divide large files into parts. For more efficient
execution, we do not even want to equalize tasks, but to minimize an idleness
of workers at the time of transition from the map stage to the reduce stage.
In this case, we can write a combinatorial-heuristic or linearly optimising
algorithm that will distribute the pieces of files so that the workers finish
the task at the same time. If desired, we can even take into account
the statistics of the speed of execution of tasks by different workers.
- Improve bucketing. Given that the distribution of the first letters of words
in texts is not uniform, it is possible to make a more balanced sharding (if we
intend to exclude the possibility of sharding by hash). We can calculate
the coefficients of occurrence of the first letters and try to split them
into buckets more evenly. But if there is no goal to equalize the execution time
of tasks, then we can try to optimize the total execution time of the reduce
stage by sorting the order of execution from a larger task (by the sum
of coefficients or simply by the number of rows) to a smaller one.
- Consider the possibility of starting the execution of the reduce stage before
the completion of the map stage. Since by default, the reduce does not require
all intermediate files to be present (files are read one by one), we can
initiate reduce task and send files once any map task is complited.
- Add warnings. It is possible to increase the verbality of execution due to
known potentially faulty conditions (for example, empty input files). Usiage of
the program will become more convenient.
- Make task parameters configurable from the cli. There are parameters
not visible to the user, which are probably expected to be seen in such
a program. For example, tokenization parameters. The case-sensitivity flag is
implemented, others can be added, in order to be more like like nltk or gensim.
- Compare the effectiveness of the sorted and unsorted mapReduce approach.
The map-reduce execution scheme proposed in the exercise is as follows:
we flatten incoming texts into lists of words in one task, and then aggregate
the lists in another. Strictly speaking, this scheme differs from the classic
map-reduce with sorting and subsequent merge-join. In the first case, we need
more memory in the reduce, and in the second in the map (sorting after map
demands memory to be more precise). The proposed scheme looks reasonable, but
its superiority is not obvious and requires proof. The answer to the question
"which approach is better for this task" will depend on the requirements,
as well as on the input data. For example, during exploitation, it may appear
that the data comes to us is partially or completely sorted, and all the words
are unique. Therefore, it is necessary to measure resource consumption and,
as noted above, give the user the opportunity to choose.
- Implement and add flag for map tasks to produce reduce-like output. It will
increase the effectiveness of the classic map-reduce, and also help in extreme
cases, such as a large number of identical repeated words at the input.
- Add caching. In general, the data indirectly tells us that tasks can be
repeatative. If this is the case, then depending on the completeness of
the repetition, it may be reasonable to add caching at the input to the driver
(if the dataset is completely repeated) or at the grpc input to the map task
(if part of the dataset is repeated, or there are duplicate files in the set
itself). The cache for the reduce does not make sense, because if at least one
file is not in the map cache, then the reduce will have to be recalculated,
elif all the files are in the cache, then the cache of the reduce will appear
only if the program itself was called with these parameters, and the answer will
be in the driver cache. Given the relatively short execution time of the task,
the cache may seem meaningless. The reasonability depends on the data.
In addition, as the complexity of tokenization increases, resource consumption
may increase significantly, especially when it comes to fusional languages like
Spanish or Russian. + Note: When writing a cache, it is necessary to take into
account the parameters with which it was received.
