#!/usr/bin/python

# This script initiates the helper nodes, that is, the Ridge Infrastructure:
# All ensembles, each with its coordinator and its acceptors

# try: import simplejson as json
# except ImportError: import json

import inspect
import json
import os
import sys
import threading

import common


# ./deployJMcast.py /Users/longle/Dropbox/Workspace/PhD/ScalableSMR/dynastarTPCC/bin/systemConfigs/jmcast_minimal_system_config.json

# ====================================
# ====================================

# ====================================
# ====================================

if len(sys.argv) != 2:
    print("Usage: " + sys.argv[0] + " config_file")
    sys.exit(1)

system_config_file = os.path.abspath(sys.argv[1])

config_json = open(system_config_file)

config = json.load(config_json)

paxos_groups = config["paxos_groups"]
paxos_config_file = config["paxos_config_file"]

cmdList = []
count=0
for group in paxos_groups:
    for node in group["group_members"]:
        launchNodeCmdPieces = [common.LIBMCAST_PAXOS_PROCESS, node["pid"],
                               paxos_config_file + '.' + str(group["group_id"]),
                               "> /tmp/px"+str(count)+".log"]
        count+=1
        launchNodeCmdString = " ".join([str(val) for val in launchNodeCmdPieces])
        cmdList.append({"node": node['host'], "cmdstring": launchNodeCmdString})
server_thread = common.LauncherThread(cmdList)
server_thread.start()
server_thread.join()
