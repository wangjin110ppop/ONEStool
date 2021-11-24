#!usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author:Peter.Li
@file: check_log.py
@time: 2021/05/19

检查RGW日志中各种类型的操作的日志，包括 put get list delete
"""
import re
import os
import sys
import time
import json
import commands
import datetime

operation_type = sys.argv[1]

hostname = commands.getoutput("hostname")
log_path = "/var/log/storage/RGW/client.radosgw.%s.log" % hostname

print "获取类型为 %s S3 请求" % operation_type
print "检查日志路径为 : %s" % log_path


def read_log():
    with open(log_path, "rb") as f:
        lines = f.readlines()
        last_line = lines[-1]
    return last_line


def get_op_pattern(op_type):
    """
    根据不同的 op 类型来获取对应 iops

    :param op_type:
    :return:
    """
    op_type = op_type.lower()
    if op_type == "get":
        pattern = re.compile(r'GET /(.*)/(.*) HTTP/1.1" 200')
    elif op_type == "put":
        pattern = re.compile(r'PUT /(.*)/(.*) HTTP/1.1" 200')
    elif op_type == "list":
        pattern = re.compile(r'GET /(.*)\?max-keys=(.*) HTTP/1.1" 200')
    elif op_type == "delete":
        pattern = re.compile(r'DELETE /(.*)/(.*) HTTP/1.1" 204')
    else:
        print "[ERROR] Unknown operation type，EXIT"
        sys.exit(-1)
    return pattern


def check_op_count(op_type):
    """

    cat <日志路径> | grep <时间点> | grep <操作类型> | grep "HTTP/1.1" 200" | wc -l

    """
    last_line = read_log()
    match = re.search(r"2021-\d+-\d+ \d+:\d+:\d+", last_line)
    last_time = match.group()
    date_format = "%Y-%m-%d %H:%M:%S"
    start_time = datetime.datetime.strptime(last_time, date_format)
    time.sleep(1)
    if op_type == "list":
        op = "get"
    else:
        op = op_type
    while True:
        check_time_str = start_time.strftime(date_format)
        op_count = commands.getoutput("cat %s | grep \"%s\" | grep %s | grep \"HTTP\/1.1\\\" 20\" | wc -l" % (log_path, check_time_str, op.upper()))
        print op_count
        print "%s  %s  %s" % (check_time_str, op_type, op_count)
        time.sleep(1)
        check_time = start_time + datetime.timedelta(seconds=1)
        start_time = check_time


def check_put_bandwidth_object():
    temp = check_data_pool_objects()
    while True:
        time.sleep(2)
        objects_start = check_data_pool_objects()
        print objects_start - temp
        temp = objects_start


def check_data_pool_objects():
    ret = commands.getoutput("ceph df -f json")
    ret = json.loads(ret)
    pools = ret["pools"]
    for pool in pools:
        if pool["name"] == "default.rgw.buckets.data":
            return pool["stats"]["objects"]


check_op_count(operation_type)
