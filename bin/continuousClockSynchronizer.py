#!/usr/bin/python

import os
import time

import common

while True:
    os.system("sudo ntpdate -b node249 &> /dev/null")
    time.sleep(common.clockSyncInterval)
