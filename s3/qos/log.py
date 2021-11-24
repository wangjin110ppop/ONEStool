#!usr/bin/env python
# -*- coding: utf-8 -*- 
"""
@author:lilin
@file: log.py
@time: 2021/11/21
"""
import os
import logging

if not os.path.exists("./log/"):
    os.mkdir("./log/")


def get_logger(name):
    log = logging.getLogger(name)
    if not log.handlers:
        log.setLevel(level=logging.INFO)
        handler = logging.FileHandler("./log/{}.log".format(name))
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(funcName)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        formatter_console = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        console = logging.StreamHandler()
        console.setFormatter(formatter_console)
        console.setLevel(logging.DEBUG)
        log.addHandler(handler)
    return log
