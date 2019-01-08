# !/usr/bin/env python3
# -*- encoding: utf-8 -*-
# @author: condi
# @file: ProxiesSet.py
# @time: 18-12-10 下午1:40


# 读取http的ip列表
with open('./test_proxies.txt', 'r') as f:
    ip_list = f.readlines()
    ip_list_rmn = []
    # 将\n去掉
    for i in ip_list:
        ip_list_rmn.append(i.replace('\n', ''))


# http和https的ip代理列表
PROXIES = {
    # http代理
    # 'http': ip_list_rmn,

    # https代理
    'https': ip_list_rmn
}
