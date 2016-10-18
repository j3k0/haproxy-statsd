#!/usr/bin/python
"""
usage: report_haproxy.py [-h] [-c CONFIG] [-1]

Report haproxy stats to statsd

optional arguments:
  -h, --help            show this help message and exit
  -c CONFIG, --config CONFIG
                        Config file location
  -1, --once        Run once and exit

Config file format
------------------
[haproxy-statsd]
haproxy_url = http://127.0.0.1:1936/;csv
haproxy_user =
haproxy_password =
statsd_host = 127.0.0.1
statsd_port = 8125
statsd_namespace = haproxy.(HOSTNAME)
interval = 5
"""

import time
import csv
import socket
import argparse
import ConfigParser
import os

import requests
from requests.auth import HTTPBasicAuth

MAX_PACKET_SIZE = float(os.getenv('MAX_PACKET_SIZE', 1386))
stats = ""
def add_stat(udp_sock, host, port, stat):
    global stats
    global MAX_PACKET_SIZE
    new_stats = stats + '\n' + stat
    if len(new_stats) > MAX_PACKET_SIZE:
        flush_stats(udp_sock, host, port)
        stats = stat
    else:
        stats = new_stats

def flush_stats(udp_sock, host, port):
    global stats
    udp_sock.sendto(stats, (host, port))
    time.sleep(0.1) # Limits sending rate to reduce packet losses
    stats = ""

def get_haproxy_report(url, user=None, password=None):
    auth = None
    if user:
        auth = HTTPBasicAuth(user, password)
    r = requests.get(url, auth=auth)
    r.raise_for_status()
    data = r.content.lstrip('# ')
    return csv.DictReader(data.splitlines())


def report_to_statsd(stat_rows,
                     host=os.getenv('STATSD_HOST', '127.0.0.1'),
                     port=os.getenv('STATSD_PORT', 8125),
                     namespace=os.getenv('STATSD_NAMESPACE', 'haproxy')):
    udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    stat_count = 0

    # Report for each row
    for row in stat_rows:
        pxname = row.get('pxname').replace('_', '.')
        svname = row.get('svname').lower()
        status = row.get("status")
        #if (svname != 'backend' and svname != 'frontend' and status != 'UP' and status != 'OPEN'):
        #    continue
        path = '%s.%s.%s' % (namespace, pxname, svname)

        # Report each stat that we want in each row
        for stat in ['scur', 'qcur', 'qtime', 'ctime', 'rtime', 'ttime', 'ereq', 'eresp', 'econ', 'bin', 'bout', 'hrsp_1xx', 'hrsp_2xx', 'hrsp_3xx', 'hrsp_4xx', 'hrsp_5xx']:
            val = row.get(stat) or 0
            add_stat(udp_sock, host, port, '%s.%s:%s|g' % (path, stat, val))
            stat_count += 1

        stat = "status"
        if status == "UP" or status == "OPEN":
            status_int = 3
        elif status == "DOWN" or status == "CLOSED":
            status_int = 0
        elif status == "no check":
            status_int = 1
        else:
            status_int = 2
        add_stat(udp_sock, host, port, '%s.%s:%s|g' % (path, stat, status_int))

    flush_stats(udp_sock, host, port)
    return stat_count


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Report haproxy stats to statsd')
    parser.add_argument('-c', '--config',
                        help='Config file location',
                        default='./haproxy-statsd.conf')
    parser.add_argument('-1', '--once',
                        action='store_true',
                        help='Run once and exit',
                        default=False)

    args = parser.parse_args()
    config = ConfigParser.ConfigParser({
        'haproxy_url': os.getenv('HAPROXY_HOST', 'http://127.0.0.1:1936/;csv'),
        'haproxy_user': os.getenv('HAPROXY_USER',''),
        'haproxy_password': os.getenv('HAPROXY_PASS',''),
        'statsd_namespace': os.getenv('STATSD_NAMESPACE', 'haproxy.(HOSTNAME)'),
        'statsd_host': os.getenv('STATSD_HOST', '127.0.0.1'),
        'statsd_port': os.getenv('STATSD_PORT', 8125),
        'interval': os.getenv('INTERVAL', '10')
    })
    config.add_section('haproxy-statsd')
    config.read(args.config)

    # Generate statsd namespace
    namespace = config.get('haproxy-statsd', 'statsd_namespace')
    if '(HOSTNAME)' in namespace:
        namespace = namespace.replace('(HOSTNAME)', socket.gethostname())

    interval = config.getfloat('haproxy-statsd', 'interval')

    try:
        while True:
            report_data = get_haproxy_report(
                config.get('haproxy-statsd', 'haproxy_url'),
                user=config.get('haproxy-statsd', 'haproxy_user'),
                password=config.get('haproxy-statsd', 'haproxy_password'))

            report_num = report_to_statsd(
                report_data,
                namespace=namespace,
                host=config.get('haproxy-statsd', 'statsd_host'),
                port=config.getint('haproxy-statsd', 'statsd_port'))

            print("Reported %s stats" % report_num)
            if args.once:
                exit(0)
            else:
                time.sleep(interval)
    except KeyboardInterrupt:
        exit(0)
