#!/usr/bin/python

from os.path import isfile as file_exists

import sys

import common

data_file = common.BIN_HOME + "/databases/w_4_d_10_c_3000_i_100000.data"
data_file = common.BIN_HOME + "/databases/w_4_d_10_c_20_i_100.data"
data_file = common.BIN_HOME + "/databases/w_2_d_10_c_20_i_100.data"
data_file = common.BIN_HOME + "/databases/w_16_d_10_c_20_i_100.data"
data_file = common.BIN_HOME + "/databases/w_2_d_10_c_3000_i_100000.data"
data_file_template = common.BIN_HOME + "/databasesNoC/w_{}_d_10_c_3000_i_100000.data"
data_file_template = common.BIN_HOME + "/databases/w_16_d_10_c_20_i_100.data"
data_file_template = common.BIN_HOME + "/databases/w_{}_d_10_c_3000_i_100000.data"
data_file_template = common.BIN_HOME + "/databases/w_{}_d_10_c_20_i_100.data"

loads = [4] #number of load equal number of warehouses

preloadData = "True"

partitionings = [2, 4, 8]
partitionings = [1, 2, 4, 8]
partitionings = [2, 4, 8]
partitionings = [16]
partitionings = [4, 8, 16]
partitionings = [16]
partitionings = [4, 8, 16]
partitionings = [4]

runCount = 1

workloads = {
    # "default": {"wNO": 45, "wP": 43, "wD": 4, "wOS": 4, "wSL": 4},
    # "default": {"wNO": 45, "wP": 0, "wD": 4, "wOS": 4, "wSL": 4},
    # "exceptNO": {"wNO":0, "wP":50, "wD":4, "wOS":4, "wSL":4},
    # "exceptNO_SL": {"wNO":0, "wP":50, "wD":4, "wOS":4, "wSL":0},
    # "default": {"wNO":100, "wP":0, "wD":0, "wOS":0, "wSL":0},
    # "NO_D": {"wNO":50, "wP":0, "wD":50, "wOS":0, "wSL":0},
    # "NO_OS": {"wNO":50, "wP":0, "wD":0, "wOS":50, "wSL":0},
    # "NO_SL": {"wNO":50, "wP":0, "wD":0, "wOS":0, "wSL":50},
    # "NO_D_SL": {"wNO":50, "wP":0, "wD":50, "wOS":0, "wSL":50},
    # "P_D_SL": {"wNO":0, "wP":50, "wD":50, "wOS":0, "wSL":50},
    # "D_OS_SL": {"wNO":0, "wP":0, "wD":50, "wOS":50, "wSL":50},
    # "NO_D_OS_SL": {"wNO":45, "wP":0, "wD":4, "wOS":4, "wSL":4},
    "pureNO": {"wNO": 100, "wP": 0, "wD": 0, "wOS": 0, "wSL": 0},
    "pureP": {"wNO": 0, "wP": 100, "wD": 0, "wOS": 0, "wSL": 0},
    "pureD": {"wNO": 0, "wP": 0, "wD": 100, "wOS": 0, "wSL": 0},
    "pureOS": {"wNO": 0, "wP": 0, "wD": 0, "wOS": 100, "wSL": 0},
    "pureSL": {"wNO":0, "wP":0, "wD":0, "wOS":0, "wSL":100},
}

MODES = [common.RUNNING_MODE_SSMR]
MODES = [common.RUNNING_MODE_DYNASTAR]


def run():
    for run in range(0, runCount):
        for mode in MODES:
            for numPartitions in partitionings:
                data_file = data_file_template.format(numPartitions)
                if not file_exists(data_file):
                    print('Data File doesn\'t exist ' + data_file)
                    sys.exit(1)

                for wl in workloads:

                    # ####################################
                    # WORKLOAD
                    wNewOrder = workloads[wl]["wNO"]
                    wPayment = workloads[wl]["wP"]
                    wDelivery = workloads[wl]["wD"]
                    wOrderStatus = workloads[wl]["wOS"]
                    wStockLevel = workloads[wl]["wSL"]
                    workloadName = wl
                    # ####################################

                    for numClients in loads:
                        # numClients = numClients * (numPartitions / 2)
                        # numClients = numClients * (numPartitions / 2)
                        experimentCmd = ' '.join([str(val) for val in
                                                  [common.tpccAllInOne, numClients, numPartitions,
                                                   data_file, mode, preloadData, wNewOrder, wPayment,
                                                   wDelivery, wOrderStatus, wStockLevel, workloadName]])
                        print experimentCmd
                        common.localcmd(experimentCmd)

                        # def runTestBatch():


if __name__ == '__main__':
    run()
