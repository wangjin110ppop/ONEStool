#!usr/bin/env python
# -*- coding: utf-8 -*- 
"""
@file: multipart_config.py
"""
import os.path
from tools.env import *

print """
    ###################################################################################################
    使用注意：
    1、配置文件 multipart_config.py 中 VIRTUAL_FILE_FLAG 为 False 需要存在真实文件，才能进行分片上传。
    2、配置文件 multipart_config.py 中 VIRTUAL_FILE_FLAG 为 True 则构造虚拟文件进行分片上传，待上传文件不用真实存在。
    3、单个分片最大 5G
    4、最大分片数 10000
    ###################################################################################################
"""

"""
S3 接入信息

必填
"""
ACCESS_KEY = access_key
SECRET_KEY = secret_key
ENDPOINT_URL = endpoint_url

"""
测试目标桶

必填
"""
DESTINATION_BUCKET = "multipart"

"""
待上传文件

示例：/home/peter/A1505.tar.gz
"""
MULTIPART_UPLOAD_FILE = "/Users/lilin/Desktop/movie/IronMan3.mkv"

MULTIPART_UPLOAD_FILE_NAME = os.path.basename(MULTIPART_UPLOAD_FILE)
MULTIPART_UPLOAD_FILE_SIZE = os.path.getsize(MULTIPART_UPLOAD_FILE)

"""
单个分片大小 必填

示例：
5 * 1024 * 1024 = 5MB
"""
CHUNK_SIZE = 5*1024*1024

"""
分片上传最大重传次数 选填
"""
MAX_RETRY_TIME = 5

"""
多线程上传，线程数 选填
"""
MAX_THREADS = 1

"""
构造虚拟文件，进行分片上传

VIRTUAL_FILE_FLAG 是否构造虚拟文件
VIRTUAL_FILE_NAME 虚拟文件名称
VIRTUAL_FILE_SIZE 虚拟文件大小

示例：
1024 * 1024 * 1024 = 1GB 

注意：
如果 VIRTUAL_FILE_FLAG = TRUE,将会构造虚拟文件进行分片上传，虚拟文件大小可以根据参数设置任意大小。虚拟文件最大为 5GB * 10000
"""
VIRTUAL_FILE_FLAG = False
VIRTUAL_FILE_NAME = "virtual_test"
VIRTUAL_FILE_SIZE = 50 * 1024 * 1024

if VIRTUAL_FILE_FLAG:
    MULTIPART_UPLOAD_FILE = "虚拟构造文件 " + VIRTUAL_FILE_NAME
    MULTIPART_UPLOAD_FILE_NAME = VIRTUAL_FILE_NAME
    MULTIPART_UPLOAD_FILE_SIZE = VIRTUAL_FILE_SIZE

print "是否上传虚拟文件：{}".format(VIRTUAL_FILE_FLAG)
print "待上传文件 ：{}".format(MULTIPART_UPLOAD_FILE)
print "待上传文件大小: {} MB".format(MULTIPART_UPLOAD_FILE_SIZE/(1024*1024))
