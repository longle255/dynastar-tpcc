#!/usr/bin/env python
import sys, re, getopt
from os import listdir, path
import matplotlib.pyplot as plt
import numpy as np

def usage():
    print 'usage: ' + sys.argv[0] + ' <throughput directory>'
    sys.exit(1)

def get_filenames(path, regex):
    throughput_re = re.compile(regex)
    list_files = [x for x in listdir(path) if throughput_re.match(x)]
    list_files = map(lambda f: path + '/' + f, list_files)
    return list_files

def get_clients_throughput(path_list):
    client_throughput = []
    for f in path_list:
        cli_line = []
        with open(f, 'r') as cli_tput:
            for line in cli_tput:
                if line[0] == '#':
                    continue
                line = line.strip()
                tmp = line.split(' ')
                cli_line.append((int(tmp[0]), float(tmp[-1])))
        cli_line.sort()
        client_throughput.append(cli_line)
    return client_throughput

def to_discrete(list_tuples, maximum, minimum, time_interval):
    intervals = (maximum - minimum)/time_interval + 1
    client_intervals = [[] for x in range(intervals)]
    min_interval = minimum
    min_interval_plus = minimum + time_interval
    idx = 0
    idx_tput = 0
    count=0
    while idx < len(client_intervals):
        if idx_tput == len(list_tuples):
            break
        if list_tuples[idx_tput][0] >= min_interval and list_tuples[idx_tput][0] < min_interval_plus:
            client_intervals[idx].append(list_tuples[idx_tput])
            count+=1
            idx_tput+=1
        else:
            idx+=1
            min_interval+=time_interval
            min_interval_plus+=time_interval
    assert(count == len(list_tuples))
    return client_intervals

if __name__ == '__main__':
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'i:o:g:p:d:s:')
    except getopt.GetoptError:
        usage()

    granularity = 1000
    local_path = ''
    oracle_path = ''
    partitions = 0
    dump_files = False
    filename = ''
    save_fig = None
    for opt, arg in opts:
        if opt == '-h':
            usage()
        elif opt in ('-i', '--ipath'):
            local_path = arg 
        elif opt in ('-o', '--opath'):
            oracle_path = arg 
        elif opt in ('-p', '--partitions'):
            partitions = arg
        elif opt in ('-d', '--dump'):
            dump_files = True
            filename = arg
        elif opt in ('-s', '--save'):
            save_fig = arg
        elif opt in ('-g', '--granularity'):
            granularity = int(arg)

    paths = get_filenames(path.abspath(local_path), '^throughput_client_overall_\d+.log$')
    #paths = get_filenames(path.abspath(local_path), '^throughput_client_retry_command_rate_\d+.log$')
    #paths = get_filenames(path.abspath(local_path), '^throughput_client_retry_prepare_rate_\d+.log$')
    #paths = get_filenames(path.abspath(local_path), '^throughput_client_query_rate_\d+.log$')

    oracle_path = path.abspath(oracle_path)

    tput_data = get_clients_throughput(paths)

    minimum = float('inf')
    maximum = 0

    for r in tput_data:
        for d in r:
            minimum = min(minimum, d[0])
            maximum = max(maximum, d[0])

    seconds = 0
    normalized_tput = map(lambda d : to_discrete(d, maximum, minimum, granularity), tput_data)

    fig = plt.figure()
    ax = fig.add_subplot(111)
    seconds = [x for x in range((maximum - minimum)/granularity + 1)]
    tput_avg = []
    times_avg = []
    n_clients = []

    for s in seconds:
        avg_tput = []
        avg_time = []
        for tputs in normalized_tput:
            for el in tputs[s]:
                avg_time.append(el[0])
                avg_tput.append(el[1])
        tput_avg.append(np.sum(avg_tput))
        n_clients.append(len(avg_tput)*10)
        times_avg.append(np.average(avg_time))

    repartitions = []
    repartitions_time = []
    from_part = partitions 

    min_time = min(times_avg)#min(min(times_avg), min(repartitions_time))
    print "MIN:", min(times_avg)
    times_avg = map(lambda i : i - min_time, times_avg)
    repartitions_time = map(lambda i : i - min_time, repartitions_time)
    ax.plot(times_avg, tput_avg, '-', label='avg throughput')

    print n_clients
    ax.plot(times_avg, n_clients, '-')

    for partition in repartitions_time:
        ax.plot([partition, partition], ax.get_ylim(), '--r')

    ax_partition = ax.twiny()
    ax_partition.set_xlim(ax.get_xlim())
    ax_partition.set_xticks(repartitions_time)
    ax_partition.set_xticklabels(repartitions)

    if dump_files:
        with open(filename, 'w') as f:
            for i in xrange(len(times_avg)):
                f.write('{} {}\n'.format(times_avg[i], tput_avg[i]))
        with open('./repart.txt', 'w') as f:
            for rep in xrange(len(repartitions)):
                f.write('{} {} \"{}\"\n'.format(repartitions_time[rep], ax.get_ylim()[0],
                    repartitions[rep].replace('$', '').replace('\\Rightarrow', ' {/Symbol \\256} ')))
                f.write('{} {} \"{}\"\n'.format(repartitions_time[rep], ax.get_ylim()[1],
                    repartitions[rep].replace('$', '').replace('\\Rightarrow', ' {/Symbol \\256} ')))
                f.write('\n\n')

    if save_fig:
        plt.savefig(save_fig)
    else:
        plt.show()
