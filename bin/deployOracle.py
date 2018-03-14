#!/usr/bin/python

import json
import logging
import sys

import common

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

if len(sys.argv) != 9:
    print "usage: " + common.sarg(
        0) + " <config_file> <partitioning_file> <data_file> <gatherer_host> <gatherer_port> <gatherer_dir> <gatherer_duration> <gatherer_Warmup>"
    sys.exit(1)


# ./deployOracle.py /Users/longle/Dropbox/Workspace/PhD/ScalableSMR/dynastarTPCC/bin/systemConfigs/jmcast_minimal_system_config.json /Users/longle/Dropbox/Workspace/PhD/ScalableSMR/dynastarTPCC/bin/systemConfigs/jmcast_minimal_partitioning.json /Users/longle/Dropbox/Workspace/PhD/ScalableSMR/dynastarTPCC/bin/databases/w_2_d_10_c_100_i_1000.data localhost 50000 /Users/longle/Dropbox/Workspace/PhD/ScalableSMR/dynastarTPCC/logs 90 20000
config_file = common.sarg(1)
partitioning_file = common.sarg(2)
data_file = common.sarg(3)
gatherer_host = common.sarg(4)
gatherer_port = common.iarg(5)
gatherer_dir = common.sarg(6)
gatherer_duration = common.iarg(7)
gatherer_warmup = common.iarg(8)
config_stream = open(config_file)
partition_stream = open(partitioning_file)
config = json.load(config_stream)
partition = json.load(partition_stream)


def getHostType(host):
    for group in partition["partitions"]:
        if host["pid"] in group["servers"]:
            return group["type"]


cmdList = []

for member in config["group_members"]:
    pid = member["pid"]
    group = member["group"]
    host = member["host"]
    port = member["port"]
    if getHostType(member) == "ORACLE":
        oracleCmdPieces = [common.getJavaExec(host, 'SERVER'), '-DHOSTNAME=' + str(pid) + "-" + str(group),
                           common.JAVA_CLASSPATH, common.TPCC_CLASS_ORACLE]
        oracleCmdPieces += [pid, config_file, partitioning_file, data_file+'.oracle']
        oracleCmdPieces += [gatherer_host, gatherer_port, gatherer_dir, gatherer_duration, gatherer_warmup]
        oracleCmdString = " ".join([str(val) for val in oracleCmdPieces])

        cmdList.append({"node": host, "port": port, "cmdstring": oracleCmdString})

config_stream.close()
partition_stream.close()

launcherThreads = []
# print cmdList

thread = common.LauncherThread(cmdList)
thread.start()
thread.join()
