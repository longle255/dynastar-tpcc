#!/usr/bin/python

import sys
import common
import getpass

screenNode = common.getScreenNode()
nonScreenNodes = common.getNonScreenNodes()

user = getpass.getuser()

if len(sys.argv) > 1 :
    print "Killing runBatch.py"
    common.localcmd("pkill -9 runBatch.py")

if common.LOCALHOST or common.LOCALHOST_CLUSTER:
    common.localcmd("pkill -9 java")
    common.localcmdbg("ps ax | grep 'proposer-acceptor' | awk -F ' ' '{print $1}' | xargs kill -9")
else:
    # cleaning remote nodes
    for node in nonScreenNodes:
        # common.sshcmdbg(node, "pkill java &> /dev/null")
        # common.sshcmdbg(node,"ps ax | grep 'java' | awk -F ' ' '{print $1}' | xargs sudo kill -9")
        # common.sshcmdbg(node,"ps ax | grep 'proposer-acceptor' | awk -F ' ' '{print $1}' | xargs kill -9")
        common.sshcmdbg(node, "killall -9 -u " + user + " &> /dev/null")

