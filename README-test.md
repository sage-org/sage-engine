# Testing a workload with many clients

A simple way to test the performance of SaGe with many clients running in parallel.

We consider a workload ie. a set of queries to execute stored in a
file. Suppose the file "queries" contains a sequence of queries to
execute (One line = 1 path to a query file)

To start multiple clients in // we rely on xargs linux command.

xargs takes each line of queries one by one (-n1) and execute it with
a pool of process (-P10). The process executes the command at the end
of the xargs command. Each time a process finish, it restarts with new
query taken from queries.

The -I% just assign  the line of the file under evaluation to the pattern %

To execute the workload ("queries") with a pool of 10 process:

```
time cat queries |  xargs -t -I % -P 10 -n1   poetry run sage-query  http://localhost:8000/sparql http://example.org/watdiv  -f % > /dev/null
```

Then we can compare performance of // by just comparing the time with a pool a 1
```
time cat queries |  xargs -t -I % -P 1 -n1   poetry run sage-query  http://localhost:8000/sparql http://example.org/watdiv  -f % > /dev/null
```
