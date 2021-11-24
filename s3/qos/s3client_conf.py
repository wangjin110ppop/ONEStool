#!usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author:Peter.Li
@file: pysss_conf.py.py
@time: 2021/05/28
"""

"""
用户秘钥
"""
ACCESS_KEY = "NH2DH0QZDEQIBMXRSPXD"
SECRET_KEY = "nb0CBUSm9FGrWqDtMkKQ0T6NIBWtamyXdcCxdxRU"

"""
所有对象网关的列表
"""
ENDPOINT_LIST = [
    {
        "endpoint_name": "host116",
        "endpoint_url": "http://stor.test.com:8082"
    },
    # {
    #     "endpoint_name": "host117",
    #     "endpoint_url": "http://192.168.163.31:8082"
    # },
    # {
    #     "endpoint_name": "host118",
    #     "endpoint_url": "http://192.168.163.32:8082"
    # }
]

"""
桶名称前缀
"""
BUCKET_PREFIX = "peter"

"""
桶名称范围， 配合 BUCKET_PREFIX 使用
"""
BUCKET_RANGE = (2, 2)

"""
对象名称前缀
"""
OBJECT_PREFIX = "myobjects"

"""
对象名称范围，配合 OBJECT_PREFIX 使用
"""
OBJECT_RANGE = (1, 1000)

"""
待上传对象大小
"""
KB = 1024
UPLOAD_FILE_SIZE = 4 * KB

"""
统计周期 单位 秒
"""
POLL_INTERVAL = 2

"""
线程数（压力）
"""
WORKERS = 32

"""
OP_TYPE 只能为四种值  put / get / list / delete
"""
OP_TYPE = "put"


