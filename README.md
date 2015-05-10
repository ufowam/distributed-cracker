## Compiling

```
% cd src
% make
```

## Client

All the following should be run from the src directory.

To start the Client run: 
    `java ClientDriver <Zookeeper Host>:<Zookeeper Port>`

## FileServer

To start the FileServer run: 
    `java Fileserver <Zookeeper Host>:<Zookeeper Port> <Path to dictionary> [Server Port]`

## JobTracker

To start the JobTracker run: 
    `java JobTracker <Zookeeper Host>:<Zookeeper Port> [Server Port]`

## Worker

To start the Worker run: 
    `java Worker <Zookeeper Host>:<Zookeeper Port>`


## Notes


You can provide optional server ports to JobTracker and FileServer in case you
want to create more than one on the same machine.
By default, FileServer will use port 5000 and JobTracker will use port 3000.

When a FileServer or JobTracker starts, it tries to register itself as primary,
if this fails, this is probably because a primary exists, and it
becomes secondary.




