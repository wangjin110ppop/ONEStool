#!usr/bin/env python
# -*- coding: utf-8 -*- 
"""
@author:lilin
@file: s3client.py
@time: 2021/11/21
"""
"""
@time: 2021/05/27

How to use：

"""
import sys
import time
import boto3
import random
import string
import threading
from log import get_logger
from concurrent import futures
from s3client_conf import *

lock = threading.Lock()
upload_all = 0
upload_success = 0
upload_fail = 0
log = get_logger("pys3")


class S3Client(object):
    """
    For QoS test

    """

    def __init__(self):
        pass

    @staticmethod
    def init_s3(endpoint_url):
        """
        init S3 session

        :param endpoint_url:
        :return:
        """
        s3 = boto3.client(
            service_name='s3',
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            endpoint_url=endpoint_url,
            verify=False
        )
        return s3

    @staticmethod
    def update_op_all():
        """
        更新全部上传计数

        :return:
        """
        global upload_all
        lock.acquire()
        upload_all += 1
        lock.release()

    @staticmethod
    def update_op_succeed():
        """
        更新上传成功的计数

        :return:
        """
        global upload_success
        lock.acquire()
        upload_success += 1
        lock.release()

    @staticmethod
    def update_op_failed():
        """
        更新上传失败的计数

        :return:
        """
        global upload_fail
        lock.acquire()
        upload_fail += 1
        lock.release()

    @staticmethod
    def get_object_count():
        """
        周期打印上传信息

        :return:
        """
        global upload_all, upload_success, upload_fail
        while True:
            tim = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            print "{} Total request: {}, Succeed: {}, Failed: {}  IOPS: {}" \
                  "".format(tim, upload_all, upload_success, upload_fail, upload_success / POLL_INTERVAL)
            lock.acquire()
            upload_all = upload_success = upload_fail = 0
            lock.release()
            time.sleep(POLL_INTERVAL)

    @staticmethod
    def get_random_character(len_str=6):
        return ''.join(random.choice(string.ascii_lowercase) for _ in range(len_str))

    def put_object(self, endpoint, worker_index, data_body):
        """
        Put object

        :param s3_client:
        :param worker_index:
        :param data_body:
        :return:
        """
        while True:
            s3_client = self.init_s3(endpoint["endpoint_url"])
            bucket_number = random.randint(BUCKET_RANGE[0], BUCKET_RANGE[1])
            bucket_name = "%s%s" % (BUCKET_PREFIX, bucket_number)
            object_name = "%s_%s_%s" % (OBJECT_PREFIX, worker_index, self.get_random_character())
            start = time.time()
            start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            self.update_op_all()
            try:
                s3_client.put_object(Bucket=bucket_name, Body=data_body, Key=object_name)
                self.update_op_succeed()
                is_success = True
            except Exception as e:
                is_success = False
                self.update_op_failed()
                print e.message
            end = time.time()
            log.info("put_start_time:{} Bucket:{} Object:{} is_success:{} "
                     "total_cost:{}".format(start_time, bucket_name, object_name, is_success, (end - start)))

    def do_put(self):
        log.info("Start put operation {}".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
        data_body = "a" * UPLOAD_FILE_SIZE
        with futures.ThreadPoolExecutor(max_workers=WORKERS) as pool:
            pool.submit(self.get_object_count)
            for index in range(WORKERS):
                endpoint = ENDPOINT_LIST[index % len(ENDPOINT_LIST)]
                a = pool.submit(self.put_object, endpoint, index, data_body)
                print a.result()

    def list_object(self, s3):
        """
        List Object

        :param s3:
        :return:
        """
        while True:
            bucket_number = random.randint(BUCKET_RANGE[0], BUCKET_RANGE[1])
            bucket_name = "%s%s" % (BUCKET_PREFIX, bucket_number)
            self.update_op_all()
            try:
                s3.list_objects(Bucket=bucket_name, MaxKeys=1)
                self.update_op_succeed()
            except Exception as e:
                self.update_op_failed()
                print e.message

    def do_list(self):
        log.info("Start list operation {}".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
        with futures.ThreadPoolExecutor(max_workers=WORKERS) as pool:
            pool.submit(self.get_object_count)
            for index in range(WORKERS):
                endpoint = ENDPOINT_LIST[index % len(ENDPOINT_LIST)]
                s3_client = self.init_s3(endpoint["endpoint_url"])
                pool.submit(self.list_object, s3_client)

    def delete_object(self, s3):
        """
        删除操作，事先需要使用 cosbench 预埋一些对象数量

        :param s3:
        :return:
        """
        while True:
            bucket_number = random.randint(BUCKET_RANGE[0], BUCKET_RANGE[1])
            object_number = random.randint(OBJECT_RANGE[0], OBJECT_RANGE[1])
            bucket_name = "%s%s" % (BUCKET_PREFIX, bucket_number)
            object_name = "%s%s" % (OBJECT_PREFIX, object_number)
            self.update_op_all()
            try:
                s3.delete_object(Bucket=bucket_name, Key=object_name)
                self.update_op_succeed()
            except Exception as e:
                self.update_op_failed()
                print e.message

    def do_delete(self):
        """
        多线程执行删除操作

        :return:
        """
        log.info("Start delete operation {}".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
        with futures.ThreadPoolExecutor(max_workers=WORKERS) as pool:
            pool.submit(self.get_object_count)
            for index in range(1, WORKERS + 1):
                endpoint = ENDPOINT_LIST[index % len(ENDPOINT_LIST)]
                s3_client = self.init_s3(endpoint["endpoint_url"])
                pool.submit(self.delete_object, s3_client)

    def get_object(self, endpoint):
        while True:
            s3 = self.init_s3(endpoint["endpoint_url"])
            bucket_number = random.randint(BUCKET_RANGE[0], BUCKET_RANGE[1])
            object_number = random.randint(OBJECT_RANGE[0], OBJECT_RANGE[1])
            bucket_name = "%s%s" % (BUCKET_PREFIX, bucket_number)
            object_name = "%s%s" % (OBJECT_PREFIX, object_number)
            self.update_op_all()
            try:
                res = s3.get_object(Bucket=bucket_name, Key=object_name)
                self.update_op_succeed()
            except Exception as e:
                self.update_op_failed()
                print e.message

    def do_get(self):
        log.info("Start get operation {}".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
        with futures.ThreadPoolExecutor(max_workers=WORKERS) as pool:
            pool.submit(self.get_object_count)
            for index in range(1, WORKERS + 1):
                endpoint = ENDPOINT_LIST[index % len(ENDPOINT_LIST)]
                # s3_client = self.init_s3(endpoint["endpoint_url"])
                pool.submit(self.get_object, endpoint)

    def create_test_bucket(self):
        s3 = self.init_s3(ENDPOINT_LIST[0]["endpoint_url"])
        for ret in (BUCKET_RANGE[0], BUCKET_RANGE[1] + 1):
            bucket_name = BUCKET_PREFIX + str(ret)
            try:
                s3.create_bucket(Bucket=bucket_name)
                print "Creat bucket {}".format(bucket_name)
            except:
                print "Bucket {} is already exists".format(bucket_name)

    def do_work(self, OP_TYPE):
        if OP_TYPE == "put":
            self.create_test_bucket()
            self.do_put()
        elif OP_TYPE == "list":
            self.do_list()
        elif OP_TYPE == "delete":
            self.do_delete()
        elif OP_TYPE == "get":
            self.do_get()
        else:
            print "Unrecognized operation"


if __name__ == "__main__":
    OP_GET = 'get'
    OP_PUT = 'put'
    OP_LIST = 'list'
    OP_DEL = 'delete'
    ENDPOINT_LIST = [
        {"endpoint_name": "host116", "endpoint_url": "http://192.168.163.30:8082"},
        {"endpoint_name": "host117", "endpoint_url": "http://192.168.163.31:8082"},
        {"endpoint_name": "host118", "endpoint_url": "http://192.168.163.32:8082"}
    ]
    s3c = S3Client()
    s3c.do_work(OP_LIST)

