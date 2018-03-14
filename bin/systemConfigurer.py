#!/usr/bin/python

# try: import simplejson as json
# except ImportError: import json

import json

import math
import sys

import common
from common import get_item


def containsCoordinator(sequence):
    for process in sequence:
        if process["role"] == "coordinator":
            return True
    return False


def getCoordinator(ensemble):
    for process in ensemble:
        if process["role"] == "coordinator":
            return process
    return None


def addCoordinator(sequence, ensemble):
    coordinator = getCoordinator(ensemble)
    sequence.insert(0, coordinator)


serverList = [{"id": 0, "partition": 0}]


def generateRidgeConfiguration(nodes, numPartitions, numOracles, replicasPerPartition, ensembleSize, configFilePath,
                               saveToFile, ridgeProcessPerNode=1, serverPerNode=1):
    config = dict()
    config["agent_class"] = "RidgeMulticastAgent"
    config["rmcast_agent_class"] = "SimpleReliableMulticastAgent"
    config["batch_size_threshold_bytes"] = common.batch_size_threshold_bytes
    config["batch_time_threshold_ms"] = common.batch_time_threshold_ms
    config["delta_null_messages_ms"] = common.delta_null_messages_ms
    config["deliver_conservative"] = True
    config["deliver_optimistic_uniform"] = False
    config["deliver_optimistic_fast"] = False
    config["direct_fast"] = False
    config["latency_estimation_sample"] = common.latency_estimation_sample
    config["latency_estimation_devs"] = common.latency_estimation_devs
    config["latency_estimation_max"] = common.latency_estimation_max
    config["batching_enabled"] = common.batching_enabled

    # OKAY
    # groups (1:1 with partitions)
    config["groups"] = []
    for p in range(1, numPartitions + 1):
        config["groups"].append({"group_id": p})

    # OKAY
    # ensembles
    config["ensembles"] = []
    ensembleRange = None
    if numPartitions == 1:
        ensembleRange = [1]
    else:
        ensembleRange = range(0, numPartitions + 1)
    numEnsembles = len(ensembleRange)
    for e in ensembleRange:
        if (e == 0):
            destination_groups = range(1, numPartitions + 1)
        else:
            destination_groups = [e]
        ensemble = {"ensemble_id": e, "learner_broadcast_mode": "DYNAMIC", "destination_groups": destination_groups}
        config["ensembles"].append(ensemble)

    # OKAY
    # helper processes (neither servers nor clients)
    helperList = {"coordinator": [], "acceptor": []}
    firstServerPid = 0
    usedNodeIndex = 0;
    allEnsembles = dict()
    config["ensemble_processes"] = []
    # quorumSize = int(ensembleSize / 2) + 1
    quorumSize = int(math.ceil(float(ensembleSize) / 2)) + 1
    # numDeployedPerEnsemble = ensembleSize
    numDeployedPerEnsemble = quorumSize  # in practice, only the quorum nodes need to be deployed
    for pid in range(0, len(nodes), numDeployedPerEnsemble):
        if pid >= numDeployedPerEnsemble * numEnsembles:
            firstServerPid = pid
            break
        eid = ensembleRange[int(pid / numDeployedPerEnsemble)]
        allEnsembles[eid] = []
        for j in range(numDeployedPerEnsemble):
            if usedNodeIndex < (pid + j) / ridgeProcessPerNode:
                usedNodeIndex = (pid + j) / ridgeProcessPerNode
            process = {"pid": pid + j, "ensemble": eid, "host": nodes[(pid + j) / ridgeProcessPerNode],
                       "port": 50000 + pid + j}
            if j == 0:
                process["role"] = "coordinator"
            else:
                process["role"] = "acceptor"
            config["ensemble_processes"].append(process)
            allEnsembles[eid].append(process)
            helperList[process["role"]].append(process)

    # OKAY... this is the simple version that creates a single acceptor sequence per ensemble
    # acceptor sequences v1.0
    config["acceptor_sequences"] = []
    quorumSize = int(ensembleSize / 2) + 1
    seqId = 0
    for eid in allEnsembles:
        ensemble = allEnsembles[eid]
        sequence = ensemble[:quorumSize]
        onlyAccIds = [process["pid"] for process in sequence]
        formattedSequence = {"id": seqId, "ensemble_id": eid, "coordinator_writes": True, "acceptors": onlyAccIds}
        seqId += 1
        config["acceptor_sequences"].append(formattedSequence)

    # learners
    remainingNodes = None
    minClientId = 0
    serverList = []
    oracleList = []
    usedNodeIndex += 1
    config["group_members"] = []
    for sid in range(firstServerPid, firstServerPid + (len(nodes) - usedNodeIndex) * serverPerNode):
        gid = 1 + int((sid - firstServerPid) / replicasPerPartition)
        i_serverIndex = ((sid - firstServerPid) / serverPerNode) + usedNodeIndex
        serverIndex = nodes[i_serverIndex]
        if gid > numPartitions:
            minClientId = sid
            remainingNodes = nodes[i_serverIndex:]
            break
        learner = {
            "pid": sid,
            "group": gid,
            "host": serverIndex,
            "port": 51000 + sid,
            "rmcast_address": serverIndex,
            "rmcast_port": 52000 + sid
        }
        config["group_members"].append(learner)
        if gid <= numPartitions - numOracles:
            role = "PARTITION"
            server = {
                "id": sid,
                "partition": gid,
                "host": serverIndex,
                "pid": sid,
                "role": role
            }
            serverList.append(server)
        else:
            role = "ORACLE"
            server = {
                "id": sid,
                "partition": gid,
                "host": serverIndex,
                "pid": sid,
                "role": role}
            oracleList.append(server)

    if saveToFile:
        systemConfigurationFile = open(configFilePath, 'w')
        json.dump(config, systemConfigurationFile, sort_keys=False, indent=4, ensure_ascii=False)
        systemConfigurationFile.flush()
        systemConfigurationFile.close()
    ridgeConfiguration = {
        "config_file": configFilePath,
        "coordinator_list": helperList["coordinator"],
        "acceptor_list": helperList["acceptor"],
        "server_list": serverList,
        "oracle_list": oracleList,
        "client_initial_pid": minClientId,
        "remaining_nodes": remainingNodes
    }

    return ridgeConfiguration


def generateJMcastConfiguration(nodes_available, numPartitions, numOracles, replicasPerPartition, replicaPerPaxosGroup,
                                configFilePath,
                                saveToFile, paxosReplicaPerNode=1, serverPerNode=1):
    paxos_config_file = common.SYSTEM_CONFIG_DIR + "/generated_paxos.conf"
    jmcast_config_file = common.SYSTEM_CONFIG_DIR + "/generated_mcast.conf"
    config = dict()
    config["agent_class"] = "JMcastAgent"
    config["jmcast_config_file"] = jmcast_config_file
    config["paxos_config_file"] = paxos_config_file
    config["rmcast_agent_class"] = "SimpleReliableMulticastAgent"
    config["batch_size_threshold_bytes"] = common.batch_size_threshold_bytes
    config["batch_time_threshold_ms"] = common.batch_time_threshold_ms
    config["delta_null_messages_ms"] = common.delta_null_messages_ms
    config["batching_enabled"] = common.batching_enabled
    # OKAY
    # paxos_groups
    # paxos_nodes = []
    node_index = 0

    config["paxos_groups"] = []
    pid = 0
    serverList = []
    oracleList = []
    count = 0
    processPerNodeCount = 0
    for p in range(numPartitions + numOracles):
        paxos_group = dict()
        paxos_group["group_id"] = p
        paxos_group["group_members"] = [];
        count = 0
        processPerNodeCount += 1
        for i_p in range(replicaPerPaxosGroup):
            paxos_group["group_members"].append({
                "pid": i_p,
                "host": nodes_available[node_index + count],
                "port": 51000 + pid,
                "inc": pid
            })
            count += 1
            pid += 1

        config["paxos_groups"].append(paxos_group)
        if processPerNodeCount >= paxosReplicaPerNode:
            processPerNodeCount = 0
            node_index += replicaPerPaxosGroup
    pid = 0
    if processPerNodeCount != 0: node_index += replicaPerPaxosGroup
    # node_index = count
    count = 0
    config["group_members"] = []
    for p in range(numPartitions + numOracles):
        for i_p in range(replicasPerPartition):
            config["group_members"].append({
                "pid": pid,
                "gpid": i_p,
                "group": p,
                "host": nodes_available[node_index],
                "port": 50000 + pid,
                "rmcast_address": nodes_available[node_index],
                "rmcast_port": 56000 + pid
            })

            if p < numPartitions:
                serverList.append({
                    "id": pid,
                    "partition": p,
                    "host": nodes_available[node_index],
                    "pid": pid,
                    "role": "PARTITION"
                })
            else:
                oracleList.append({
                    "id": pid,
                    "partition": p,
                    "host": nodes_available[node_index],
                    "pid": pid,
                    "role": "ORACLE"
                })
            count += 1
            pid += 1
            if count >= serverPerNode:
                node_index += 1
                count = 0
    if saveToFile:
        systemConfigurationFile = open(configFilePath, 'w')
        json.dump(config, systemConfigurationFile, sort_keys=False, indent=4, ensure_ascii=False)
        systemConfigurationFile.flush()
        systemConfigurationFile.close()

        # save paxos config
        for group in config["paxos_groups"]:
            replicas = []
            proposers = []
            acceptors = []
            replicas = []
            for node in group["group_members"]:
                replicas.append("#replica " + str(node["pid"]) + " " + node["host"] + " " + str(node["port"]))
                proposers.append("proposer " + str(node["pid"]) + " " + node["host"] + " " + str(52000 + node["inc"]))
                acceptors.append("acceptor " + str(node["pid"]) + " " + node["host"] + " " + str(53000 + node["inc"]))
            file_name = paxos_config_file + "." + str(group["group_id"])
            common.render_template(common.jmcastPaxosTemplate, {"replicas": '\n'.join(replicas),
                                                                "proposers": '\n'.join(proposers),
                                                                "acceptors": '\n'.join(acceptors)}, file_name)

        # save mcast config
        records = []
        for node in config["group_members"]:
            records.append(
                "node " + str(node["group"]) + " " + str(node["gpid"]) + " " + node["host"] + " " + str(node["port"]))
        common.render_template(common.jmcastMcastTemplate, {"learners": '\n'.join(records)},
                               jmcast_config_file)

    # print config
    remainingNodes = nodes_available[node_index + 1:]
    minClientId = pid + 1
    return {
        "config_file": configFilePath,
        # "coordinator_list": helperList["coordinator"],
        # "acceptor_list": helperList["acceptor"],
        "server_list": serverList,
        "oracle_list": oracleList,
        "client_initial_pid": 10001,
        "remaining_nodes": remainingNodes
    }
    # ridgeConfiguration = generateRidgeConfiguration(availableNodes, numPartitions, replicasPerPartition, ensembleSize, configFilePath)


def generatePartitioningFile(serverList, oracleList, partitionsFile, saveToFile):
    pconf = dict()
    pconf["partitions"] = []
    for s in (serverList + oracleList):
        pentry = get_item(pconf["partitions"], "id", s["partition"])
        if pentry == None:
            pentry = {"id": s["partition"], "servers": [], "type": s["role"]}
            pconf["partitions"].append(pentry)
        pentry["servers"].append(s["id"])

    if saveToFile:
        partitioningFile = open(partitionsFile, 'w')
        json.dump(pconf, partitioningFile, sort_keys=False, indent=4, ensure_ascii=False)
        partitioningFile.flush()
        partitioningFile.close()

    return pconf


def generateSystemConfigurationForRidge(numPartitions, numOracles, saveToFile=True):
    availableNodes = common.NODES
    ensembleSize = common.ensembleSize
    replicasPerPartition = common.replicasPerPartition
    sysConfigFilePath = common.SYSTEM_CONFIG_FILE
    partitionsFilePath = common.PARTITION_CONFIG_FILE
    numOracles = numOracles
    screenNode = availableNodes[0]
    gathererNode = availableNodes[1]
    remainingNodes = availableNodes[2:]

    systemConfiguration = generateRidgeConfiguration(remainingNodes, numPartitions + numOracles, numOracles,
                                                     replicasPerPartition,
                                                     ensembleSize,
                                                     sysConfigFilePath, saveToFile, common.ridgeProcessPerNode,
                                                     common.serverPerNode)
    generatePartitioningFile(systemConfiguration["server_list"], systemConfiguration["oracle_list"], partitionsFilePath,
                             saveToFile)

    systemConfiguration["screen_node"] = screenNode
    systemConfiguration["gatherer_node"] = gathererNode
    systemConfiguration["partitioning_file"] = partitionsFilePath
    #     configuration = {"config_file": configFilePath, "partitioning_file": None, "server_list": serverList, "client_initial_pid": minClientId}
    return systemConfiguration


def generateSystemConfigurationForJMcast(numPartitions, numOracles, saveToFile=True):
    availableNodes = common.NODES
    ensembleSize = common.ensembleSize
    replicasPerPartition = common.replicasPerPartition
    sysConfigFilePath = common.SYSTEM_CONFIG_FILE
    partitionsFilePath = common.PARTITION_CONFIG_FILE
    numOracles = numOracles
    screenNode = availableNodes[0]
    gathererNode = availableNodes[1]
    remainingNodes = availableNodes[2:]

    systemConfiguration = generateJMcastConfiguration(remainingNodes, numPartitions, numOracles,
                                                      replicasPerPartition,
                                                      ensembleSize,
                                                      sysConfigFilePath, saveToFile, common.paxosProcessPerNode,
                                                      common.serverPerNode)
    generatePartitioningFile(systemConfiguration["server_list"], systemConfiguration["oracle_list"], partitionsFilePath,
                             saveToFile)

    systemConfiguration["screen_node"] = screenNode
    systemConfiguration["gatherer_node"] = gathererNode
    systemConfiguration["partitioning_file"] = partitionsFilePath
    return systemConfiguration


def generateSystemConfiguration(numPartitions, numOracles=1, saveToFile=True):
    if common.MCAST_LIB == 'ridge':
        return generateSystemConfigurationForRidge(numPartitions, numOracles, True)
    else:
        return generateSystemConfigurationForJMcast(numPartitions, numOracles, True)


def getRetwisServerNode():
    return common.NODES[2]


def getGathererNode():
    return common.NODES[1]


def getClientNodes(numPartitions=None, numOracles=1):
    chirperConfig = generateSystemConfiguration(numPartitions, numOracles, False)
    return chirperConfig["remaining_nodes"]


if __name__ == '__main__':
    # usage
    def usage():
        print "usage: <partitions_num>"
        sys.exit(1)

    numPartitions = common.iarg(1)
    numOracle = 1
    if common.MCAST_LIB == 'ridge':
        generateSystemConfigurationForRidge(numPartitions, numOracle, True)
    else:
        generateSystemConfigurationForJMcast(numPartitions, numOracle, True)
5