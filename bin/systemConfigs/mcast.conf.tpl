## LibMCast configuration file

# Specify a groups id, an id, an ip address and a port for each node.
# GroupIds/Ids must start from 0 and must be unique.

$learners
#node 1 0 127.0.0.1 8703
#node 1 1 127.0.0.1 8704
#node 1 2 127.0.0.1 8705
#node 2 0 127.0.0.1 8706
#node 2 1 127.0.0.1 8707
#node 2 2 127.0.0.1 8708


# Verbosity level: must be one of quiet, error, info, or debug.
# Default is info.
# verbosity debug

# Enable TCP_NODELAY?
# Default is 'yes'.
# tcp-nodelay no

# How many us (micro-seconds) between proposals
# Default is 100000
message-timeout 1000000

# How many messages before start a new consensus instance
# Default is 1
# batch-size 2

# Try to avoid convoy effect?
# Default is 'yes'
# avoid-convoy no

# Send all delivered messages to the registered replicas?
# Default is 'no'
# replica-mode yes

############################ LMDB storage ############################

# Persist messages before delivering
# Default is 'no'.
# persist yes

# Should the nodes trash previous storage files and start from scratch?
# This is here only for testing purposes.
# Default is 'no'.
# trash-files yes

# Should lmdb write to disk synchronously?
# Default is 'no'.
# lmdb-sync yes

# Path for lmdb database environment.
lmdb-env-path /tmp/node

# lmdb's map size in bytes (maximum size of the database).
# Accepted units are mb, kb and gb.
# Default is 100mb.
# lmdb-mapsize 1gb
