#!/usr/bin/python


# ============================================
# ============================================
# preamble

import sys

import common
import systemConfigurer
from common import iarg
from common import sarg


# ./deployTestRunners.py /Users/longle/Dropbox/Workspace/PhD/ScalableSMR/dynastarTPCC/bin/systemConfigs/jmcast_minimal_system_config.json /Users/longle/Dropbox/Workspace/PhD/ScalableSMR/dynastarTPCC/bin/systemConfigs/jmcast_minimal_partitioning.json 2 1 100 /Users/longle/Dropbox/Workspace/PhD/ScalableSMR/dynastarTPCC/bin/databases/w_2_d_10_c_100_i_1000.data 45 43 4 4 4


def usage():
    print "usage: " + sarg(0) + \
          "configFile partsFile numPartitions numClients minClientId dataFile wNewOrder wPayment wDelivery wOrderStatus wStockLevel"
    sys.exit(1)


# print sys.argv, len(sys.argv)
if len(sys.argv) not in [12]:
    usage()

# constants
NODE = 0
CLIENTS = 1

# deployment layout
configFile = sarg(1)
partsFile = sarg(2)
numPartitions = iarg(3)
numClients = iarg(4)
minCientId = iarg(5)
dataFile = sarg(6)

numOracle = common.numOracle

# command arguments
wNewOrder = iarg(7)
wPayment = iarg(8)
wDelivery = iarg(9)
wOrderStatus = iarg(10)
wStockLevel = iarg(11)

algargs = []
gathererNode = systemConfigurer.getGathererNode()

clientNodes = None

config = systemConfigurer.generateSystemConfiguration(numPartitions, numOracle, saveToFile=True)
clientNodes = systemConfigurer.getClientNodes(numPartitions, numOracle)

clientId = minCientId
numPermits = 10
algargs = [clientId, configFile, partsFile, numPermits]

NUM_TRANS = -1  # unlimited
# ============================================
# ============================================
# begin

logDir = common.TPCC_LOG_CLIENTS
clientMapping = common.mapClientsToNodes(numClients, clientNodes)

tmp = dataFile.split('/')
tmp = tmp[len(tmp) - 1]
tmp = tmp.split('_')
tmp = tmp[1]
numWarehouse = int(tmp)
numDistrict = 10
clientDistrict = []
terminalPerClient = 10

for i in range(numClients):
    clientDistrict.append([])

k = 0
for i in range(numDistrict):
    for j in range(numWarehouse):
        if len(clientDistrict[k]) < terminalPerClient:
            clientDistrict[k].append([j + 1, i + 1])
        else:
            k += 1
            clientDistrict[k].append([j + 1, i + 1])

# print clientDistrict
for i in range(len(clientDistrict)):
    clientI = clientDistrict[i]
    for j in range(len(clientI)):
        tmp = clientI[j]
        clientI[j] = "w=" + str(tmp[0]) + ":d=" + str(tmp[1])
    clientDistrict[i] = "_".join([str(val) for val in (clientI)])

print 'AAAAA', clientDistrict
for mapping in clientMapping:
    if mapping[CLIENTS] == 0:
        continue

    numPermits = mapping[CLIENTS]
    numPermits = 10
    commonargs = [common.getJavaExec(mapping[NODE], 'CLIENT'), common.JAVA_CLASSPATH,
                  '-DHOSTNAME=' + str(clientId), common.TPCC_CLASS_CLIENT, ]
    commonargs += [clientId, configFile, partsFile, dataFile + '.oracle', numPermits, NUM_TRANS]
    commonargs += [wNewOrder, wPayment, wDelivery, wOrderStatus, wStockLevel]
    commonargs += [gathererNode, common.gathererPort, logDir, common.EXPERIMENT_DURATION,
                   common.EXPERIMENT_WARMUP_MS, clientDistrict[clientId - minCientId - 1]]

    # algargs[0] = str(clientId)
    # algargs[3] = str(numPermits)
    cmdString = " ".join([str(val) for val in (commonargs)])
    print cmdString
    common.sshcmdbg(mapping[NODE], cmdString)
    clientId += 1
