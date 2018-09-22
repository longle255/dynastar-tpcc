#!/usr/bin/python

import datetime
import sys
import time
import os
from os.path import abspath

import common
import systemConfigurer
from common import farg
from common import iarg
from common import sarg, localcmdbg


# usage
def usage():
    print "usage: " \
          + sarg(0) \
          + " numClients numPartitions data_file runningMode" \
            " preloadData [wNO wP wD wOS wSL name]"
    sys.exit(1)


# ./runAllOnce.py 1 2 /Users/longle/Dropbox/Workspace/PhD/ScalableSMR/dynastarTPCC/bin/databases/w_2_d_10_c_100_i_1000.data DynaStar True 45 43 4 4 4 w_2_d_10_c_100_i_1000

# parameters
numClients = iarg(1)
numPartitions = iarg(2)
data_file = abspath(sarg(3))
runningMode = sarg(4)
preloadData = True if sarg(5).lower() == 'true' else False
clientNodes = None
numServers = 0
numCoordinators = 0
numAcceptors = 0
numOracles = common.numOracle

wNewOrder = iarg(6)
wPayment = iarg(7)
wDelivery = iarg(8)
wOrderStatus = iarg(9)
wStockLevel = iarg(10)
workloadName = sarg(11)

if not common.ENV_LOCALHOST:
    common.localcmd(common.cleaner)
    time.sleep(5)

sysConfig = None
serverList = None
oracleList = None
sysConfigFile = None
partitioningFile = None
gathererNode = None
minClientId = None

# common.localcmd(common.systemParamSetter)


# disable clock sync
if not common.ENV_LOCALHOST and common.MCAST_LIB == 'ridge':
    for node in common.getNonScreenNodes():
        common.sshcmdbg(node, common.continousClockSynchronizer)

sysConfig = systemConfigurer.generateSystemConfiguration(numPartitions, numOracles)
time.sleep(2)

if common.ENV_EC2:
    # need to sync this sysConfig with other instances
    print "Syncing config file"
    os.system(
        "/home/ubuntu/apps/ScalableSMR/bin/aws/control-sync-code.sh /home/ubuntu/apps/ScalableSMR/chirperV2/bin/systemConfigs")
    time.sleep(5)

serverList = sysConfig["server_list"]
oracleList = sysConfig["oracle_list"]
numServers = len(serverList)

sysConfigFile = sysConfig["config_file"]
partitioningFile = sysConfig["partitioning_file"]
gathererNode = sysConfig["gatherer_node"]
minClientId = sysConfig["client_initial_pid"]
clientNodes = sysConfig["remaining_nodes"]

# logfolder
logFolder = ""
logFolder = "_".join([str(val) for val in
                      [workloadName, runningMode, "p", numPartitions, "c", numClients,
                       "no", wNewOrder, "p", wPayment, "d",
                       wDelivery, "os", wOrderStatus, "sl", wStockLevel, "#",
                       datetime.datetime.now().isoformat().replace(':', '-')]])

# Launch Redis
# common.sshcmdbg(gathererNode, "/home/long/.local/bin/redis-server --protected-mode no")
# localcmdbg(serverCmdStr)
###########################################################################
# LAUNCH SERVERS
###########################################################################

# launch server(s)

# launch multicast infrastructure
mcastCmdPieces = [common.multicastDeployer, sysConfigFile]
mcastCmdString = " ".join(mcastCmdPieces)
localcmdbg(mcastCmdString)

time.sleep(5)

# # launch replicas
serverCmdPieces = [common.tpccServerDeployer, sysConfigFile, partitioningFile,
                   data_file, gathererNode, str(common.gathererPort),
                   common.TPCC_LOG_SERVERS,
                   str(common.EXPERIMENT_DURATION), str(common.EXPERIMENT_WARMUP_MS)]
serverCmdStr = " ".join(serverCmdPieces)
localcmdbg(serverCmdStr)

# time.sleep(5)
#
# launch oracle

oracleCmdPieces = [common.tpccOracleDeployer, sysConfigFile, partitioningFile,
                   data_file, gathererNode, str(common.gathererPort),
                   common.TPCC_LOG_SERVERS,
                   str(common.EXPERIMENT_DURATION), str(common.EXPERIMENT_WARMUP_MS)]
oracleCmdString = " ".join(oracleCmdPieces)
localcmdbg(oracleCmdString)

if "d_10_c_3000_" in data_file:
    print "Wait for partitioning."
    time.sleep(25)
else:
    print "Waiting for servers to be ready..."
    time.sleep(5)
# # time.sleep(3 * numPartitions + 5)
# print "Servers should be ready."

# time.sleep(5)
###########################################################################
# LAUNCH CLIENTS
###########################################################################

# launch clients
# deployTestRunners.py: data_file numClients weightPost weightFollow weightUnfollow weightGetTimeline algorithm [(for chirper:) numPartitions minClientId configFile partsFile]"

print 'Deploying clients'

clientCmdPieces = [common.tpccClientDeployer, sysConfigFile, partitioningFile,
                   numPartitions, numClients, minClientId, data_file, wNewOrder,
                   wPayment, wDelivery, wOrderStatus , wStockLevel]

clientCmdString = " ".join([str(val) for val in clientCmdPieces])
common.localcmd(clientCmdString)

###########################################################################
# LAUNCH ACTIVE MONITORS
###########################################################################

if not common.ENV_LOCALHOST:
    for nonClient in serverList:
        # bwmCmdPieces = [common.javabin, common.javacp, common.javaBWMonitorClass, "PARTITION",
        #                 nonClient["pid"], chirperSysConfig["gatherer_node"], str(common.gathererPort),
        #                 common.SENSE_DURATION, common.nonclilogdirchirper]
        # bwmCmdString = " ".join([str(val) for val in bwmCmdPieces])
        # common.sshcmdbg(nonClient["host"], bwmCmdString)

        cpumCmdPieces = [common.getJavaExec(sysConfig["gatherer_node"], 'GATHERER'), common.JAVA_CLASSPATH,
                         common.javaCPUMonitorClass, "PARTITION", nonClient["pid"], sysConfig["gatherer_node"],
                         str(common.gathererPort),
                         str(common.EXPERIMENT_DURATION), common.TPCC_LOG_SERVERS]
        cpumCmdString = " ".join([str(val) for val in cpumCmdPieces])
        common.sshcmdbg(nonClient["host"], cpumCmdString)

        # memmCmdPieces = [common.javabin, common.javacp, common.javaMemoryMonitorClass, "PARTITION",
        #                  nonClient["pid"], chirperSysConfig["gatherer_node"], str(common.gathererPort),
        #                  common.SENSE_DURATION, common.nonclilogdirchirper]
        # memmCmdString = " ".join([str(val) for val in memmCmdPieces])
        # common.sshcmdbg(nonClient["host"], memmCmdString)

    for nonClient in oracleList:
        # bwmCmdPieces = [common.javabin, common.javacp, common.javaBWMonitorClass, "ORACLE",
        #                 nonClient["pid"], chirperSysConfig["gatherer_node"], str(common.gathererPort),
        #                 common.SENSE_DURATION, common.nonclilogdirchirper]
        # bwmCmdString = " ".join([str(val) for val in bwmCmdPieces])
        # common.sshcmdbg(nonClient["host"], bwmCmdString)

        cpumCmdPieces = [common.getJavaExec(sysConfig["gatherer_node"], 'GATHERER'), common.JAVA_CLASSPATH,
                         common.javaCPUMonitorClass, "ORACLE", nonClient["pid"], sysConfig["gatherer_node"],
                         str(common.gathererPort),
                         str(common.EXPERIMENT_DURATION), common.TPCC_LOG_SERVERS]
        cpumCmdString = " ".join([str(val) for val in cpumCmdPieces])
        common.sshcmdbg(nonClient["host"], cpumCmdString)


###########################################################################
# LAUNCH GATHERER
###########################################################################

# launch gatherer last and wait for it to finish
# wait for gatherer with timeout; if timeout, kill everything, including Redis
# gathererCmdPieces = [common.gathererDeployer, gathererNode, algorithm, numPartitions, numClients, numServers,
#                      numCoordinators, numAcceptors, logFolder]

common.localcmd("mkdir -p " + common.TPCC_LOG_SERVERS)
fullLogDir = common.TPCC_LOG_BASE + logFolder + "/"
detailLogDir = common.TPCC_LOG_BASE + logFolder + "/details/"
gathererCmdPieces = [common.gathererDeployer, gathererNode, numPartitions, numClients, numServers,
                     numOracles, detailLogDir]

gathererCmd = " ".join([str(val) for val in gathererCmdPieces])

#
# print gathererCmd
common.localcmd(gathererCmd)

###########################################################################
# processing logs
###########################################################################
# collect main log files
if common.ENV_EC2:
    # need to bring file back
    common.localcmd(
        'rsync -e "ssh -o StrictHostKeyChecking=no" -rav ' + gathererNode + ':' + common.TPCC_LOG_BASE + '/* ' + common.TPCC_LOG_BASE)

common.localcmd("cp " + common.benchCommonPath + " " + detailLogDir)
common.localcmd("cp " + common.runBatchPath + " " + detailLogDir)
common.localcmd("cp " + common.SYSTEM_CONFIG_FILE + " " + detailLogDir)
common.localcmd("cp " + common.PARTITION_CONFIG_FILE + " " + detailLogDir)
common.localcmdbg("mv " + common.TPCC_LOG_SERVERS + " " + detailLogDir)

main_logs = ["throughput_client_overall_aggregate.log", "latency_client_overall_average.log"]
for log in main_logs:
    common.localcmdbg("mv " + detailLogDir + "/" + log + " " + fullLogDir)

## this is only for plotting, need to change later
common.localcmd(
    "cp " + fullLogDir + "/throughput_client_overall_aggregate.log" + " " + fullLogDir + "/throughput_client_conservative_overall_aggregate.log")
common.localcmd(
    "cp " + fullLogDir + "/latency_client_overall_average.log" + " " + fullLogDir + "/latency_client_conservative_overall_average.log")

###########################################################################
# PLOTTING GRAPHS
###########################################################################

tput_plot = fullLogDir + "throughput.png"
tput_plot_cmd = ["MPLBACKEND=agg", common.throughputPlotting, "-i", detailLogDir, "-s", tput_plot]
tput_plotting_cmd = " ".join([str(val) for val in tput_plot_cmd])
print tput_plotting_cmd
common.localcmdbg(tput_plotting_cmd)


###########################################################################
# clean up
if not common.ENV_LOCALHOST:
    common.localcmd(common.cleaner)
