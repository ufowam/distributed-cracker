This is a distributed password cracker built with Java and Zookeeper. Given an MD5 hash, it will perform a dictionary attack and attempt to find the word matching the hash. The different nodes comprising the system can run on different machines and everything is coordinated using zookeeper. You will therefore need to have zookeeper running and provide it's address and port to the different nodes.

You can launch multiple nodes of each module:
- The fileservers and job trackers will operate on a primary/backup basis. If the primary fails, one of the backups will take over.
- The workers will keep consuming tasks until shut down and coordinate with each other so that no task is processed more than once. If there are no tasks, they will stay idle until there are tasks ready to be processed again.
- The client drivers is where users can launch password cracking jobs and check for the result.

For more information, you can check the [design](https://github.com/ufowam/distributed-cracker/wiki/Design) page.

## Compiling

```
% cd src
% make
```

## Running the code

To submit a job: 
    `./submit_job.sh <Zookeeper Host> <Zookeeper Port> <MD5 hash>`

To check the status of a job:
    `./check_job_status.sh <Zookeeper Host> <Zookeeper Port> <MD5 hash>`

To start the FileServer run: 
    `./start_fileserver.sh <Zookeeper Host> <Zookeeper Port>`

To start the JobTracker run: 
    `./start_jobtracker.sh <Zookeeper Host> <Zookeeper Port>`

To start the Worker run: 
    `./start_worker.sh <Zookeeper Host> <Zookeeper Port>`


## Notes


You can provide optional server ports to JobTracker and FileServer in case you
want to create more than one on the same machine.
By default, FileServer will use port 5000 and JobTracker will use port 3000.

When a FileServer or JobTracker starts, it tries to register itself as primary,
if this fails, this is probably because a primary exists, and it
becomes secondary.




