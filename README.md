# Example Putting JSON config into Zookeeper

## Build

gradle build

## Run

java com.bscllc.zk.Main <path to JSON configuration file>

## Description

Loads the JSON file, flattens it, using '/' as the separator, for the path. It then stores it in Zookeeper.


## Testing

1. Start Zookeeper
1. Use the Zookeeper CLI command to attach to zookeeper
> $ZK_HOME/bin/zkCli.sh -server localhost:2181
> 
>help
>
>ls /
>
3. Use 'get' to be able see values in the tree
 