#!/usr/bin/python

import os

import common

# ./deployClient.py
data_file = common.sarg(1)
client_id = "12345" + common.sarg(2)
terminal_num = common.iarg(2)

# command arguments
wNewOrder = common.iarg(3)
wPayment = common.iarg(4)
wDelivery = common.iarg(5)
wOrderStatus = common.iarg(6)
wStockLevel = common.iarg(7)
mod = common.sarg(8)

if common.ENV_CLUSTER:
    system_config_file = common.script_dir() + "/systemConfigs/clusterSysConfig.json"
    partitioning_file = common.script_dir() + "/systemConfigs/clusterPartitionsConfig.json"
    hostname='192.168.3.1'
else:
    system_config_file = common.script_dir() + "/systemConfigs/jmcast_minimal_system_config.json"
    partitioning_file = common.script_dir() + "/systemConfigs/jmcast_minimal_partitioning.json"
    hostname='localhost'
# data_file = common.script_dir() + "/databases/w_2_d_10_c_100_i_1000.data"
num_permits = "1"


# client_cmd = java_bin + app_classpath + client_class + client_id + config_file + partitioning_file + num_permits
client_cmd = [common.getJavaExec(hostname, 'CLIENT'), common.JAVA_CLASSPATH, common.TPCC_CLASS_CLIENT, client_id, system_config_file,
              partitioning_file, data_file, terminal_num, 5000, wNewOrder, wPayment, wDelivery, wOrderStatus,
              wStockLevel, mod]
cmdString = " ".join([str(val) for val in client_cmd])
print cmdString
os.system(cmdString)
