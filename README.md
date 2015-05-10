

## Client

To start the Client run: 
    `java ClientDriver <IP Address of ZK>:<Port>`

## FileServer

To start the FileServer run: 
    `java Fileserver <IP Address of ZK>:<Port> <Path to dictionary> [Server Port]`

## JobTracker

To start the JobTracker run: 
    `java JobTracker <IP Address of ZK>:<Port> [Server Port]`

## Worker

To start the Worker run: 
    `java Worker <IP Address of ZK>:<Port>`


## Notes


You can provide optional server ports to JobTracker and FileServer in case you
want to create more than one on the same machine.
By default, FileServer will use port 5000 and JobTracker will use port 3000.

When a FileServer or JobTracker starts, it tries to register itself as primary,
if this fails, this is probably because a primary exists, and it
becomes secondary.




