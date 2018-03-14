import inspect
import json
import logging
import os
import re
import shlex
import socket
import subprocess
import threading
from string import Template

import sys

# logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

EXPERIMENT_DURATION = 180
EXPERIMENT_DURATION = 360  #300 for 2,4, 360 for 8
EXPERIMENT_WARMUP_MS = 100  # ms
EXPERIMENT_WARMUP_MS = 50000  # ms //70 for 2,4 100 for 8 16

LOCALHOST = False
LOCALHOST_CLUSTER = False
if socket.gethostname()[:4] != 'node': LOCALHOST = True
if socket.gethostname() == 'node92': LOCALHOST_CLUSTER = True

# LOCALHOST=False

LOCALHOST_NODES = []
for i in range(1, 50): LOCALHOST_NODES.append("127.0.0.1")

DEAD_NODES = [81, 89, 83, 60]
NODES_RANGE_FIRST = 1
NODES_RANGE_LAST = 60
PROFILING_PATH = "/home/long/softwares/yjp-2017.02/bin/linux-x86-64/libyjpagent.so"

DEBUGGING = True
DEBUGGING = False

PROFILING = True
PROFILING = False

if PROFILING: EXPERIMENT_DURATION = 600

# PARTITION CONFIG
replicasPerPartition = 1
ensembleSize = 3
ridgeProcessPerNode = 2
serverPerNode = 1
numOracle = 1

MCAST_LIB = 'ridge'
MCAST_LIB = 'jmcast'

MULTITHREADED_CLIENT = False
MULTITHREADED_CLIENT = True

if LOCALHOST:
    REMOTE_ENV = ""
else:
    REMOTE_ENV = " LD_LIBRARY_PATH=/home/long/.local/lib:/home/long/apps/ScalableSMR/libjmcast/libmcast/build/local/lib LD_PRELOAD=/home/long/apps/ScalableSMR/libjmcast/libmcast/build/local/lib/libevamcast.so:/home/long/apps/ScalableSMR/libjmcast/libmcast/build/local/lib/libevmcast.so"
# REMOTE_ENV = "" if LOCALHOST else
if not MCAST_LIB == 'jmcast': REMOTE_ENV = ""


# available machines
def noderange(first, last):
    return ["192.168.3." + str(val) for val in [node for node in range(first, last + 1) if node not in DEAD_NODES]]


NODES = LOCALHOST_NODES if LOCALHOST or LOCALHOST_CLUSTER else noderange(NODES_RANGE_FIRST, NODES_RANGE_LAST)
NODES = LOCALHOST_NODES if LOCALHOST or LOCALHOST_CLUSTER else noderange(1, 19) + noderange(31, 60)
NODES = LOCALHOST_NODES if LOCALHOST or LOCALHOST_CLUSTER else noderange(1, 80)

RUNNING_MODE_DYNASTAR = "DYNASTAR"
RUNNING_MODE_SSMR = "SSMR"


class Command(object):
    def __init__(self, cmd):
        self.cmd = cmd
        self.process = None

    def run(self, timeout):
        def target():
            logging.debug('Thread started')
            run_args = shlex.split(self.cmd)
            self.process = subprocess.Popen(run_args)
            self.process.communicate()
            logging.debug('Thread finished')

        thread = threading.Thread(target=target)
        thread.start()

        thread.join(timeout)
        if thread.is_alive():
            logging.debug('Terminating process')
            self.process.terminate()
            thread.join()
        return self.process.returncode


class LauncherThread(threading.Thread):
    def __init__(self, clist):
        threading.Thread.__init__(self)
        self.cmdList = clist

    def run(self):
        for cmd in self.cmdList:
            logging.debug("Executing: %s", cmd["cmdstring"])
            sshcmdbg(cmd["node"], cmd["cmdstring"])


def script_dir():
    return os.path.dirname(os.path.abspath(inspect.getsourcefile(lambda _: None)))


def sshcmd(node, cmdstring, timeout=None):
    finalstring = "ssh -o StrictHostKeyChecking=no " + node + REMOTE_ENV + " \"" + cmdstring + "\""
    logging.debug(finalstring)
    cmd = Command(finalstring)
    return cmd.run(timeout)


def localcmd(cmdstring, timeout=None):
    logging.debug("localcmd:%s", cmdstring)
    cmd = Command(cmdstring)
    return cmd.run(timeout)


def sshcmdbg(node, cmdstring):
    cmd = "ssh -o StrictHostKeyChecking=no " + node + REMOTE_ENV + " \"" + cmdstring + "\" &"
    logging.debug("sshcmdbg: %s", cmd)
    os.system(cmd)


def localcmdbg(cmdstring):
    logging.debug("localcmdbg: %s", cmdstring)
    os.system(cmdstring + " &")


def get_item(lst, key, value):
    index = get_index(lst, key, value)
    if index == -1:
        return None
    else:
        pass
    return lst[index]


def get_index(lst, key, value):
    for i, dic in enumerate(lst):
        if dic[key] == value:
            return i
    return -1


def get_system_config_file(config_type):
    if config_type is None:
        return {'partitioning': SYSTEM_CONFIG_DIR + '/partitioning.json',
                'system_config': SYSTEM_CONFIG_DIR + '/system_config.json'}
    partitioning_file = SYSTEM_CONFIG_DIR + '/' + config_type + '_partitioning.json'
    system_config_file = SYSTEM_CONFIG_DIR + '/' + config_type + '_system_config.json'
    if not os.path.isfile(partitioning_file):
        logging.error('ERROR: parititoning file not found: %s', partitioning_file)
        sys.exit(1)
    if not os.path.isfile(system_config_file):
        logging.error('ERROR: system config file not found: %s', system_config_file)
        sys.exit(1)
    return {'partitioning': partitioning_file,
            'system_config': system_config_file}


def read_json_file(file_name):
    file_stream = open(file_name)
    content = json.load(file_stream)
    file_stream.close()
    return content


def sarg(i):
    return sys.argv[i]


def iarg(i):
    return int(sarg(i))


def farg(i):
    return float(sarg(i))


def getScreenNode():
    return NODES[0]


def getNonScreenNodes():
    return NODES[1:]


NODE = 0
CLIENTS = 1


def mapClientsToNodes(numClients, nodesList):
    # clientMap is a list of dicts
    # clientMap = [{NODE: x, CLIENTS: y}, {NODE: z, CLIENTS: w}]
    clientMap = []
    clientsPerNode = int(numClients / len(nodesList))
    for node in nodesList:
        clientMap.append({NODE: node, CLIENTS: clientsPerNode})
    for extra in range(numClients % len(nodesList)):
        clientMap[extra][CLIENTS] += 1
    return clientMap


def numUsedClientNodes(arg1, arg2=None):
    if arg2 is None:
        return numUsedClientNodes_1(arg1)
    elif arg2 is not None:
        return numUsedClientNodes_2(arg1, arg2)


def numUsedClientNodes_2(numClients, clientNodes):
    return min(numClients, len(clientNodes))


def numUsedClientNodes_1(clientNodesMap):
    numUsed = 0
    for mapping in clientNodesMap:
        if mapping[CLIENTS] > 0:
            numUsed += 1
    return numUsed


# parameters
HOME = '/'.join(os.path.dirname(os.path.abspath(__file__)).split('/')[:-1])

GLOBAL_HOME = os.path.normpath(script_dir() + '/../../')

BIN_HOME = os.path.normpath(GLOBAL_HOME + '/dynastarTPCC/bin')

DYNASTAR_HOME = os.path.normpath(GLOBAL_HOME + '/dynastarV2')
DYNASTAR_CP = os.path.normpath(DYNASTAR_HOME + '/target/classes')
DYNASTAR_CLASS_SERVER = 'ch.usi.dslab.lel.dynastarv2.sample.AppServer'
DYNASTAR_CLASS_ORACLE = 'ch.usi.dslab.lel.dynastarv2.sample.AppOracle'
DYNASTAR_CLASS_CLIENT = 'ch.usi.dslab.lel.dynastarv2.sample.AppClient'

LIBMCAD_HOME = os.path.normpath(GLOBAL_HOME + '/libmcad')
LIBMCAD_CP = os.path.normpath(LIBMCAD_HOME + '/target/classes')
LIBMCAD_CLASS_RIDGE = 'ch.usi.dslab.bezerra.mcad.ridge.RidgeEnsembleNode'

NETWRAPPER_HOME = os.path.normpath(GLOBAL_HOME + '/netwrapper')
NETWRAPPER_CP = os.path.normpath(NETWRAPPER_HOME + '/target/classes')

RIDGE_HOME = os.path.normpath(GLOBAL_HOME + '/ridge')
RIDGE_CP = os.path.normpath(RIDGE_HOME + '/target/classes')

SENSE_HOME = os.path.normpath(GLOBAL_HOME + '/sense')
SENSE_CP = os.path.normpath(SENSE_HOME + '/target/classes')

SYSTEM_CONFIG_DIR = os.path.normpath(BIN_HOME + '/systemConfigs')
SYSTEM_CONFIG_FILE = SYSTEM_CONFIG_DIR + "/generatedSysConfig.json"
PARTITION_CONFIG_FILE = SYSTEM_CONFIG_DIR + "/generatedPartitionsConfig.json"

LIBMCAST_HOME = os.path.normpath(GLOBAL_HOME + '/libjmcast')
LIBMCAST_CP = os.path.normpath(LIBMCAST_HOME + '/target/classes')
LIBMCAST_PAXOS_PROCESS = os.path.normpath(LIBMCAST_HOME + '/libmcast/build/local/bin/proposer-acceptor')

TPCC_HOME = os.path.normpath(GLOBAL_HOME + '/dynastarTPCC')
TPCC_CP = os.path.normpath(TPCC_HOME + '/target/classes')
TPCC_CLASS_SERVER = 'ch.usi.dslab.lel.dynastar.tpcc.TpccServer'
TPCC_CLASS_ORACLE = 'ch.usi.dslab.lel.dynastar.tpcc.TpccOracle'
TPCC_CLASS_CLIENT = 'ch.usi.dslab.lel.dynastar.tpcc.TpccClient'
TPCC_CLASS_TEST_RUNNER = 'ch.usi.dslab.lel.dynastar.tpcc.TestRunner'

DEPENDENCIES_DIR = os.path.normpath(GLOBAL_HOME + '/dependencies/*')

_class_path = [os.path.normpath(GLOBAL_HOME + '/dependencies/guava-19.0.jar'), TPCC_CP, DYNASTAR_CP, LIBMCAST_CP,
               LIBMCAD_CP, NETWRAPPER_CP, RIDGE_CP, SENSE_CP, DEPENDENCIES_DIR]

JAVA_BIN = 'java -XX:+UseG1GC -Xmx8g -Dlog4j.configuration=file:' + script_dir() + '/log4jLocal.xml'
JAVA_CLASSPATH = '-cp \'' + ':'.join([str(val) for val in _class_path]) + "\'"

gathererPort = 40000

# RIDGE CONFIG
batch_size_threshold_bytes = 50000
batch_time_threshold_ms = 5
delta_null_messages_ms = 5
latency_estimation_sample = 10
latency_estimation_devs = 0
latency_estimation_max = 10
clockSyncInterval = 3
batching_enabled = True
batching_enabled = False

# JMCast CONFIG
paxosProcessPerNode = 5
jmcastSysConfigFile = SYSTEM_CONFIG_DIR + "/generatedSysConfig.json"
jmcastMcastTemplate = SYSTEM_CONFIG_DIR + "/mcast.conf.tpl"
jmcastPaxosTemplate = SYSTEM_CONFIG_DIR + "/paxos.conf.tpl"
jmcastPartitionsFile = SYSTEM_CONFIG_DIR + "/generatedPartitionsConfig.json"

# SCRIPTS
clockSynchronizer = HOME + "/bin/clockSynchronizer.py"
continousClockSynchronizer = HOME + "/bin/continuousClockSynchronizer.py"
tpccServerDeployer = HOME + "/bin/deployServer.py.partitioneddb"
tpccOracleDeployer = HOME + "/bin/deployOracle.py"
tpccClientDeployer = HOME + "/bin/deployTestRunners.py"
tpccClientDynamicDeployer = HOME + "/bin/deployTestRunnerActors.py"
tpccAllInOne = HOME + "/bin/runAllOnce.py"
cleaner = HOME + "/bin/cleanUp.py"
benchCommonPath = HOME + "/bin/common.py"
runBatchPath = HOME + "/bin/runBatch.py"
# multicastDeployer = HOME + "/bin/deployMcast.py"
if MCAST_LIB is 'ridge':
    multicastDeployer = HOME + "/bin/deployRidge.py"
else:
    multicastDeployer = HOME + "/bin/deployJMcast.py"

# MONITORING
gathererDeployer = HOME + "/bin/deployGatherer.py"
javaGathererClass = "ch.usi.dslab.bezerra.sense.DataGatherer"
javaBWMonitorClass = "ch.usi.dslab.bezerra.sense.monitors.BWMonitor"
javaCPUMonitorClass = "ch.usi.dslab.bezerra.sense.monitors.CPUEmbededMonitorJavaMXBean"
javaCPUMonitorClass = "ch.usi.dslab.bezerra.sense.monitors.CPUMonitorMPStat"
javaMemoryMonitorClass = "ch.usi.dslab.bezerra.sense.monitors.MemoryMonitor"

TPCC_LOG_BASE = HOME + "/log/experiments/"
TPCC_LOG_CLIENTS = HOME + "/log/clients"
TPCC_LOG_SERVERS = HOME + "/log/server"

# PLOTTING
oracleMovePlotting = HOME + "/bin/graph_scripts/calculate_dynamic_log.py"
throughputPlotting = HOME + "/bin/graph_scripts/plot_throughput_together.py"


def get_social_network_file(set, edgecut, num_user, num_partition):
    return os.path.normpath(
        (TPCC_HOME + '/bin/graphs/{}/{}/users_{}_partitions_{}.json').format(set, edgecut, num_user, num_partition));


def getJavaExec(node, role):
    if not DEBUGGING:
        log = 'log4j.xml'
    else:
        log = 'log4jDebug.xml'

    java = "java"
    if PROFILING: java = java + " -agentpath:" + PROFILING_PATH
    if LOCALHOST:
        log = 'log4jLocal.xml'
        return java + " -XX:+UseG1GC -Xmx2g -Dlog4j.configuration=file:" + HOME + "/bin/" + log
    if LOCALHOST_CLUSTER:
        log = 'log4j.xml'
        return java + " -XX:+UseG1GC -Xmx2g -Dlog4j.configuration=file:" + HOME + "/bin/" + log

    if role == 'GATHERER':
        return java + " -XX:+UseG1GC -Xmx6g -Dlog4j.configuration=file:" + HOME + "/bin/" + log
    if role == 'SERVER':
        return java + "  -server -Xloggc:/home/long/gc." + node + ".log -XX:+PrintGCTimeStamps -XX:+PrintGC -XX:+UseConcMarkSweepGC -XX:SurvivorRatio=15 -XX:+UseParNewGC -Xms7g -Xmx7g -Dlog4j.configuration=file:" + HOME + "/bin/" + log
        # return java + "  -server -Xloggc:/home/long/gc." + node + ".log -Xmx4096m -Xms4096m -Xmn2g  -XX:ParallelGCThreads=20 -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:SurvivorRatio=8 -XX:TargetSurvivorRatio=90 -XX:MaxTenuringThreshold=15  -Dlog4j.configuration=file:" + HOME + "/bin/" + log
        # return java + "  -Xloggc:/home/long/gc." + node + ".log -XX:+PrintGCTimeStamps -XX:+PrintGC -XX:+UseG1GC -Xmx4g -Dlog4j.configuration=file:" + HOME + "/bin/" + log

        #-XX:+UseConcMarkSweepGC -XX:+UseParNewGC
        #Selects the Concurrent Mark Sweep collector. This collector may deliver better response time properties for the application
        # (i.e., low application pause time). It is a parallel and mostly-concurrent collector and and can be a good match for the
        # threading ability of an large multi-processor systems.
        #-XX:SurvivorRatio=8
        #Sets survivor space ratio to 1:8, resulting in larger survivor spaces (the smaller the ratio, the larger the space).
        # Larger survivor spaces allow short lived objects a longer time period to die in the young generation.


        # return java + " -XX:+UseParallelGC -Xms2g -Xmx2g -XX:+UseCompressedOops -Dlog4j.configuration=file:" + HOME + "/bin/" + log
        # return java + " -Xms4096m -Xmx4096m -Xmn1536m -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:MaxTenuringThreshold=1 -XX:SurvivorRatio=90 -XX:TargetSurvivorRatio=90 -Dlog4j.configuration=file:" + HOME + "/bin/" + log
        # return java + " -XX:+UseG1GC -Xmx6g -Xms6g -Xmn5g -XX:+UseCompressedOops -XX:TargetSurvivorRatio=90 -XX:MaxTenuringThreshold=1 -Dlog4j.configuration=file:" + HOME + "/bin/" + log
    if role == 'CLIENT':
        regex = re.compile("192.168.3.(\d*)")
        matched = regex.match(node)
        if int(matched.groups()[0]) in [90, 91, 92]:
            return java + " -client -XX:+UseG1GC -Xmx120g -Dlog4j.configuration=file:" + HOME + "/bin/" + log
        if int(matched.groups()[0]) in range(1, 46):
            return java + " -client -XX:+UseG1GC -Xmx4g -Dlog4j.configuration=file:" + HOME + "/bin/" + log
        if int(matched.groups()[0]) in range(46, 89):
            return java + " -client -XX:+UseG1GC -Xmx2g -Dlog4j.configuration=file:" + HOME + "/bin/" + log
        return java + " -client -XX:+UseG1GC -Xmx4g -Dlog4j.configuration=file:" + HOME + "/bin/" + log


def render_template(ftpl, value, fout):
    # open the file
    filein = open(ftpl)
    # read it
    src = Template(filein.read())
    result = src.substitute(value)
    fileout = open(fout, "w")
    fileout.write(result)
    fileout.close()
