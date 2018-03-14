#!/usr/bin/python


import sys
import time

import common
import systemConfigurer
from common import iarg
from common import sarg


def usage():
    print "usage: " + sys.argv[
        0] + " gathererHost numPartitions numClients numServers numOracles logFolder"
    sys.exit()


# command = "java -cp target/libsense-git.jar ch.usi.dslab.bezerra.sense.DataGatherer 60000 /tmp/ latency CLIENT_LATENCY_CONSERVATIVE 1 throughput CLIENT_THROUGHPUT_CONSERVATIVE 1"

if (len(sys.argv) not in [7]):
    usage()

# parameters
clientNodes = None
gathererHost = sarg(1)
numPartitions = iarg(2)
numClients = iarg(3)
numServers = iarg(4)
numOracles = iarg(5)
logFolder = sarg(6)

logsargs = []

# client logs
clientNodes = systemConfigurer.getClientNodes(numPartitions, numOracles)
numUsedClientNodes = common.numUsedClientNodes(numClients, clientNodes)

clientIndividualLogs = ["latency", "throughput", "timeline"]
clientIndividualLogs = ["latency", "throughput"]
clientIndividualSubLogs = ["overall", "new_order", "payment", "delivery", "order_status", "stock_level"]

if not common.MULTITHREADED_CLIENT:
    numUsedClientNodes = numClients

if numClients > 0:
    for log in clientIndividualLogs:
        for sublog in clientIndividualSubLogs:
            logsargs.append(log)
            logsargs.append('_'.join(["client", sublog]))
            logsargs.append(str(numUsedClientNodes))

# logsargs.append("latencydistribution")
# logsargs.append("client_overall")
# logsargs.append(str(numUsedClientNodes))

#
# server active monitor
if not common.LOCALHOST:
    serverLogs = ["cpu"]  # , "bandwidth", "memory"]
    if numServers > 0:
        for log in serverLogs:
            logsargs.append(log)
            logsargs.append("PARTITION")
            logsargs.append(str(numServers))

    if numOracles > 0:
        for log in serverLogs:
            logsargs.append(log)
            logsargs.append("ORACLE")
            logsargs.append(str(numOracles * common.replicasPerPartition))

# server passive monitor
# logsargs.append("throughput")
# logsargs.append("partition_move")
# logsargs.append(str(numServers))

# logsargs.append("throughput")
# logsargs.append("oracle_query")
# logsargs.append(str(numOracles))

# logsargs.append("message")
# logsargs.append("oracle_repartitioning")
# logsargs.append(str(numOracles))

#
# logsargs.append("throughput")
# logsargs.append("client_retry_command_rate")
# logsargs.append(str(numUsedClientNodes))
#
# logsargs.append("throughput")
# logsargs.append("client_query_rate")
# logsargs.append(str(numUsedClientNodes))

logPath = common.TPCC_LOG_BASE + logFolder

cmdArgs = [common.getJavaExec(gathererHost, 'GATHERER'), common.JAVA_CLASSPATH, common.javaGathererClass,
           common.EXPERIMENT_WARMUP_MS, common.gathererPort, logFolder] + logsargs
cmdString = ' '.join([str(val) for val in cmdArgs])
# print cmdString

timetowait = common.EXPERIMENT_DURATION * 3

exitcode = common.sshcmd(gathererHost, cmdString, timetowait)
if exitcode != 0:
    common.localcmd("touch %s/FAILED.txt" % (logFolder))

common.localcmd(common.cleaner)
time.sleep(10)
