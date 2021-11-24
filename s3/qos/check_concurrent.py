#!usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author:lilin
@file: check_concurrent.py
@time: 2021/11/22

校验并发度计算日志信息
日志路径： /var/log/ceph/ceph-osd.*

日志格式（可能会随着版本的变更日志格式）：
2021-11-22 14:35:16.233030 7f273e3ea700 848961 4 DEBUG <cls> /dse/jjt/ceph-L/src/cls/rgw/cls_rgw.cc:4204: oid=192.168.163.0/24-concurrent-req,name=radosgw.lym0,weight=0, total_weight=19, rgw_num=3, get token size=1

"""
import re
import csv
import time
import datetime
import requests

log_path = "/var/log/ceph/ceph-osd.11.log"
print "检查的日志路径：{}".format(log_path)


class Api:

    def __init__(self, host, is_admin=True, **kwargs):
        url = "http://{}/api/v1/auth/login/".format(host)
        if is_admin:
            body = {
                    "username": "admin",
                    "password": "QWRtaW5AMTIz",
                    "next": "/manage/#/dashboard"
            }
        else:
            body = {
                    "username": kwargs.get("user"),
                    "password": "VXNlckAxMjM=",
                    "next": "./#/dashboard"
            }
        self.host = host
        self.session = requests.session()
        self.token = self.session.cookies.get('XSRF-TOKEN')
        if not self.token:
            self.session.post(url, json=body).json()
            self.token = self.session.cookies.get('XSRF-TOKEN')
        self.session.headers = {
                'Content-Type': 'application/json',
                'X-XSRF-TOKEN': self.token
            }
        self.ceph_id = self.get_cluster_id(host)

    def get_cluster_id(self, host):
        """
        获取集群ID

        :param host: Handy IP
        :return: 集群ID
        """
        url = "http://{}/api/v3/plat/cluster".format(host)
        ret = self.session.get(url)
        cluster_id = eval(ret.content).get("data").get("id")
        return cluster_id

    def modify_qos_strategy(self, qos_name, **kwargs):
        """
        创建QoS策略

        :param qos_name: 策略名称
        :param kwargs:
        :return:
        """
        kwargs.setdefault("put_tps", "0")
        kwargs.setdefault("get_tps", "0")
        kwargs.setdefault("list_tps", "0")
        kwargs.setdefault("del_tps", "0")
        kwargs.setdefault("put_bps", "0Byte/s")
        kwargs.setdefault("get_bps", "0Byte/s")
        kwargs.setdefault("concurrent_req", "0")
        kwargs.setdefault("description", "auto test")
        url = "http://{}/api/v3/onestor/{}/objectstorage/qos/policy".format(self.host, self.ceph_id)
        body = {
            "name": qos_name,
            "put_tps": str(kwargs.get("put_tps")),
            "get_tps": str(kwargs.get("get_tps")),
            "del_tps": str(kwargs.get("del_tps")),
            "list_tps": str(kwargs.get("list_tps")),
            "put_bps": str(kwargs.get("put_bps")),
            "get_bps": str(kwargs.get("get_bps")),
            "concurrent_req": str(kwargs.get("concurrent_req")),
            "description": str(kwargs.get("description")),
        }
        ret = self.session.patch(url, json=body)
        if ret.status_code == 200:
            print "[success] Create qos {}".format(qos_name)
        else:
            print "[failed] Create qos {}".format(qos_name)


def read_log(concurrent, start_time, end_time):
    with open(log_path, "rb") as f:
        line = f.readline()
        while line:
            ret = get_calc_qos_detail(line, start_time, end_time, concurrent)
            if ret:
                write_csv(ret)
            line = f.readline()


def get_log_time(log_info):
    match = re.search(r"2021-\d+-\d+ \d+:\d+:\d+.\d+", log_info)
    if match:
        return match.group()
    else:
        return False


def check_time_range(check_time_str, start_time, end_time):
    date_format = "%Y-%m-%d %H:%M:%S.%f"
    # date_format_2 = "%Y-%m-%d %H:%M:%S"
    check_time = datetime.datetime.strptime(check_time_str, date_format)
    # start_time = datetime.datetime.strptime(start_time_str, date_format_2)
    # end_time = datetime.datetime.strptime(end_time_str, date_format_2)
    if (check_time > start_time) and (check_time < end_time):
        return True
    else:
        return False


def write_csv(row):
    with open("concurrence.csv", "a+") as file:
        f = csv.writer(file)
        f.writerow(row)


def get_calc_qos_detail(log_info, start_time, end_time, concurrent):
    """
    日志格式 oid=192.168.163.0/24-concurrent-req,name=radosgw.lym2,weight=0, total_weight=0, rgw_num=1, get token size=1
    :param start_time: "2021-11-22 14:29:35"
    :param end_time: "2021-11-22 14:35:35"
    :param log_info:
    :return:
    """
    log_time = get_log_time(log_info)
    if log_time:
        flag = check_time_range(log_time, start_time, end_time)
        if flag:
            regex = r'oid=(.*),name=radosgw.(.*),weight=(\d+), total_weight=(\d+), rgw_num=(\d+), get token size=(\d+)'
            match = re.search(regex, log_info)
            if match:
                print "Time             {}".format(log_time)
                print "OID              {}".format(match.group(1))
                print "rgw_host_name    {}".format(match.group(2))
                print "weight           {}".format(match.group(3))
                print "total_weight     {}".format(match.group(4))
                print "rgw_num          {}".format(match.group(5))
                print "get token size   {}".format(match.group(6))
                ret = [log_time, match.group(1), match.group(2), match.group(3), match.group(4), match.group(5), match.group(6), concurrent]
                return ret


def start():
    for i in range(1, 50):
        start_time = datetime.datetime.now()
        api = Api("192.168.163.30")
        api.modify_qos_strategy("qos1", concurrent_req=i)
        time.sleep(60*8)
        end_time = datetime.datetime.now()
        start_time = start_time + datetime.timedelta(minutes=2)
        end_time = end_time - datetime.timedelta(minutes=1)
        print "start time : {}".format(start_time)
        print "end time : {}".format(end_time)
        read_log(i, start_time, end_time)


start()




