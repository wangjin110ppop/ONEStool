#!usr/bin/env python
# -*- coding: utf-8 -*- 
"""
@author:lilin
@file: concurrent_data_analysis.py
@time: 2021/11/24
"""

import pandas as pd

data_list = []
df = pd.read_csv("/Users/lilin/Desktop/data.csv")
for i in range(1, 50):
    df_concurrent = df[df["concurrent"] == i]
    data_count = df_concurrent["concurrent"].value_counts().values[0] #
    token_sum = df_concurrent["get token size"].sum()
    temp = data_count / 3
    avg_concurrent = round(float(token_sum)/temp, 2)
    error_value = (avg_concurrent - i).__abs__() / i
    print "》》》》》》》》》》》》》》》》》》》》》"
    print "当前设置并发度:     {}".format(i)
    print "采集数据(条)       {}".format(data_count)
    print "数据组            {}".format(temp)
    print "token size总和    {}".format(token_sum)
    print "平均并发度:        {}".format(avg_concurrent)
    print "误差值：           {}".format(error_value)

    element = {
        "设置并发度": i,
        "采集数据条数": data_count,
        "统计数据组":temp,
        "token总和":token_sum,
        "平均并发度":avg_concurrent,
        "误差":round(error_value, 3)
    }
    data_list.append(element)
data_df = pd.DataFrame(data_list)
data_df.to_csv("concurrent_analysis.csv")

