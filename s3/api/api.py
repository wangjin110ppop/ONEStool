#!/user/bin/env python
# -*- coding: utf-8 -*-

import json
import requests


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

    def create_rgw_user(self, user_id, tenant):
        """
        创建对象存储用户

        :param user_id: 用户名
        :param tenant: 租户名
        :return:
        """
        url = "http://{}/api/v3/onestor/{}/objectstorage/user".format(self.host, self.ceph_id)
        email = "{}@unis.com".format(user_id)
        body = {
            "users": [
                {
                    "user_id": user_id,
                    "tenant": tenant,
                    "email": email,
                    "max_objects": -1,
                    "max_size": -1
                }
            ],
            "__async__": True
        }
        print self.session.post(url, json=body).content

    def open_compression(self):
        """
        打开对象网关压缩功能

        :return: None
        """
        url = "http://{}/api/v3/onestor/{}/objectstorage/obs/compress".format(self.host, self.ceph_id)
        body = {
            "compress_method": "zlib",  # 只支持这一种
            "compression": True
        }
        ret = self.session.post(url, json=body)
        if ret.status_code == 200:
            print "[success] Open object compression"
        else:
            print "[failed] Open object compression"

    def close_compression(self):
        """
        关闭对象网关压缩功能

        :return: None
        """
        url = "http://{}/api/v3/onestor/{}/objectstorage/obs/compress".format(self.host, self.ceph_id)
        body = {
            "compress_method": "zlib",
            "compression": False
        }
        ret = self.session.post(url, json=body)
        if ret.status_code == 200:
            print "[success] Close object compression"
        else:
            print "[failed] Close object compression"

    def create_bucket(self, tenant, bucket_name):
        """
        Handy 创建桶

        :param tenant: 租户名称
        :param bucket_name: 待创建桶名称
        :return: None
        """
        url = "http://{}/api/v3/onestor/{}/objectstorage/bucket".format(self.host, self.ceph_id)
        body = {
            "tenant": tenant,
            "name": bucket_name,
            "quota": {
                "quota": False,
                "max_size": "-1",
                "max_objects": "-1"
            },
            "__async__": True
        }
        self.session.post(url, json=body)

    def list_bucket(self, page_num=1, page_size=10):
        """
        Handy 列举桶信息

        :param page_num:
        :param page_size:
        :return:
        """
        url = 'http://{}/api/v3/onestor/{}/objectstorage/bucket?page_num={}&page_size={}' \
              '&search_key=name&search_value=&ordering=-created'.format(self.host, self.ceph_id, page_num, page_size)
        data = self.session.get(url).json().get("data")
        count = data.get("count")
        num = count / page_size + 1
        bucket_ids = map(lambda x: x.get('uuid'), data.get("data"))
        return num, bucket_ids

    def delete_bucket(self, bucket_ids):
        """
        Handy 删除桶

        :param bucket_ids:
        :return:
        """
        url = 'http://{}/api/v3/onestor/{}/objectstorage/bucket/delete'.format(self.host, self.ceph_id)
        body = {
            "force": True,
            "bucket_ids": bucket_ids,
            "__async__": True
        }
        self.session.post(url, json=body)

    def delete_all_buckets(self):
        """
        Handy 删除所有桶

        :return: None
        """
        page_num = 1
        num, bucket_ids = self.list_bucket()
        for i in range(num):
            bucket_ids = self.list_bucket(page_num=page_num)[1]
            self.delete_bucket(bucket_ids)
            page_num += 1

    def create_plat_user_group(self, group_name):
        """
        创建 平台用户组

        :param group_name: 平台用户组名称
        :return:
        """
        url = "http://{}/api/v3/onestor/{}/plat/handy/group".format(self.host, self.ceph_id)
        body = {
                "group_name": group_name,
                "permissions": [
                    {"name_en": "Resources", "id": 41, "name": "资源管理"},
                    {"name_en": "Hosts", "id": 31, "name": "主机管理"},
                    {"name_en": "Storage Pools", "id": 27, "name": "Pool信息"},
                    {"name_en": "Block Storage", "id": 28, "name": "块存储"},
                    {"name_en": "Object Storage", "id": 29, "name": "对象存储"},
                    {"name_en": "File Storage", "id": 37, "name": "文件存储"},
                    {"name_en": "Management Availability", "id": 36, "name": "管理高可用"},
                    {"name_en": "Monitor", "id": 33, "name": "监控报表"},
                    {"name_en": "Alarm Info", "id": 34, "name": "告警信息"},
                    {"name_en": "Alarm Settings", "id": 35, "name": "告警设置"},
                    {"name_en": "SNMP Settings", "id": 42, "name": "SNMP设置"},
                    {"name_en": "Operation Log", "id": 26, "name": "操作日志"},
                    {"name_en": "Audit Log", "id": 44, "name": "审计日志"},
                    {"name_en": "System Log", "id": 25, "name": "系统日志"},
                    {"name_en": "Multi Cluster", "id": 43, "name": "多集群管理"},
                    {"name_en": "Parameters", "id": 38, "name": "参数配置"},
                    {"name_en": "Replace Hardware", "id": 48, "name": "硬件更换向导"}
                ],
                "summary": "创建用户组",
                "__async__": "true"
        }
        ret = self.session.post(url, json=body)
        if ret.status_code == 200:
            print "[success] create user_group {}".format(group_name)
        else:
            print "[failed] create user_group {}".format(group_name)

    def create_plat_user(self, user_name, group_name):
        """
        创建 平台用户

        :param user_name:
        :param group_name:
        :return:
        """
        url = "http://{}/api/v3/onestor/{}/plat/handy/user".format(self.host, self.ceph_id)
        body = {
            "user_name": user_name,
            "group_name": group_name,
            "summary": "创建用户",
            "__async__": "true"
        }
        ret = self.session.post(url, json=body)
        if ret.status_code == 200:
            print "[success] create user {}, user_group {}".format(user_name, group_name)
        else:
            print "[failed] create user {}, user_group {}".format(user_name, group_name)

    def get_plat_users(self):
        """
        获取平台所有用户

        :return:
        """
        url = "http://{}/api/v3/onestor/{}/objectstorage/tenant/plat_users".format(self.host, self.ceph_id)
        ret = self.session.get(url)
        if ret.status_code == 200:
            return ret.content
        else:
            print "[failed] Get plat users failed"

    def get_plat_user_id(self, plat_user_name):
        """
        根据 平台用户 用户名 寻找用户id

        :param plat_user_name:
        :return:
        """
        users = self.get_plat_users()
        user_list = json.loads(users)["data"]["plat_users"]
        for user_info in user_list:
            if user_info["username"] == plat_user_name:
                return user_info["id"]
        print "[failed] Can't find plat user {}".format(plat_user_name)

    def create_tenant(self, tenant_name, plat_user_name):
        """
        创建租户

        :param tenant_name: 租户名称
        :param plat_user_name: 平台用户名称
        :return:
        """

        url = "http://{}/api/v3/onestor/{}/objectstorage/tenant".format(self.host, self.ceph_id)
        plat_user_id = self.get_plat_user_id(plat_user_name)
        body = {
            "name": tenant_name,
            "description": "",
            "plat_users": [int(plat_user_id)],
            "__async__": "true"
        }
        ret = self.session.post(url, json=body)
        if ret.status_code == 200:
            print "[success] Create tenant {}".format(tenant_name)
        else:
            print "[failed] Create tenant {}".format(tenant_name)

    def create_object_storage_user(self, username):
        """
        创建对象存储用户

        :return: None
        """
        url = "http://{}/api/v3/onestor/{}/objectstorage/user".format(self.host, self.ceph_id)
        body = {
            "users": [{
                "user_id": username,
                "tenant": "",
                "email": "{}@unisyue.com".format(username),
                "max_objects": -1,
                "max_size": -1}],
            "__async__": "true"
        }
        ret = self.session.post(url, json=body)
        if ret.status_code == 200:
            print "[success] Create object storage user {}".format(username)
        else:
            print "[failed] Create object storage user {}".format(username)

    def get_object_users_info(self):
        """
        获取该租户下所有用户的信息

        :return: None
        """
        url = "http://{}/api/v3/onestor/{}/objectstorage/user?tenant=".format(self.host, self.ceph_id)
        res = self.session.get(url)
        return res.content

    def get_object_user_info(self, username):
        """
        获取某个用户的 access_key 和 secret_key

        :param username:
        :return:
        """
        users_info = self.get_object_users_info()
        user_list = json.loads(users_info)["data"]
        for user in user_list:
            if user["display_name"] == username:
                print user["s3_keys"][0]["access_key"]
                print user["s3_keys"][0]["secret_key"]

    def create_lifecycle(self, lifecycle_name, prefix, expiration):
        url = "http://{}/api/v3/onestor/{}/objectstorage/lifecycle".format(self.host, self.ceph_id)
        body = {
            "lifecycle":
                [{
                    "LifeCycleName": lifecycle_name,
                    "Status": "",
                    "Filter": {"Prefix": prefix},
                    "IA_Transition":
                        {
                            "Days": "",
                            "Date": "",
                            "StorageClass": ""
                        },
                    "Transition":
                        {
                        "Days": "",
                        "Date": "",
                        "StorageClass": ""
                        },
                    "Expiration":
                        {
                            "Days": str(expiration),
                            "Date": ""
                        },
                    "CapacityOverwrite":
                        {
                            "Size": ""
                        },
                    "TimeOverwrite":
                        {
                            "Minutes": ""
                        },
                    "NoncurrentVersionExpiration":
                        {
                            "NoncurrentDays": "",
                            "Date": ""
                        },
                    "NoncurrentVersionTransition":
                        {
                            "NoncurrentDays": "",
                            "StorageClass": "",
                            "Date": ""
                        }
                }],
            "bucket_ids": ["932cfe4d-d3eb-4a79-9a7d-a3c1b4ffa4ee.59762022.5"],
            "Status": "enable",
            "__async__": "true"
        }
        ret = self.session.post(url, json=body)
        if ret.status_code == 200:
            print "[success] Create lifecycle {}".format(lifecycle_name)
        else:
            print "[failed] Create lifecycle {}".format(lifecycle_name)

    def create_lifecycles(self, body):
        url = "http://{}/api/v3/onestor/{}/objectstorage/lifecycle".format(self.host, self.ceph_id)
        body = {
            "lifecycle": body,
            "bucket_ids": ["4b614340-577b-41a2-b62d-e36931eed60f.10205799.9"],
            "Status": "enable",
            "__async__": "true"
        }
        ret = self.session.post(url, json=body)
        if ret.status_code == 200:
            print "[success] Create lifecycle prefix"
        else:
            print "[failed] Create lifecycle prefix"

    def create_rgw_ec(self, host_ip, diskpool_name):
        """
        创建 EC 2+1 的存储池

        :param host_ip:
        :param diskpool_name:
        :return:
        """
        url = "http://{}/api/v3/onestor/{}/objectstorage/obs".format(self.host, self.ceph_id)
        body = {
            "host_ip": host_ip,
            "ssl": False,
            "diskpool_name": diskpool_name,
            "redundancy": "ec",
            "replicate_num": {
                "k": 2,
                "m": 1,
                "b": 0,
                "chunk": 4,
                "stripe_width": 32768,
                "ec_overwrite": True
            },
            "min_size": "task",
            "enable_encrypt": False,
            "is_local_kms": True,
            "__async__": True
        }
        ret = self.session.post(url, json=body)
        if ret.status_code == 200:
            print "[success] Create rgw"
        else:
            print "[failed] Create rgw"
        pass

    def delete_rgw(self, host_ip):
        """
        删除对象网关

        :param host_ip: 待删除网关的管理IP
        :return:
        """
        url = "http://{}/api/v3/onestor/{}/objectstorage/obs?host_ip={}&offline=false&__async__=false".format(
            self.host, self.ceph_id, host_ip)
        ret = self.session.delete(url)
        if ret.status_code == 200:
            print "[success] Delete rgw {}".format(host_ip)
        else:
            print "[failed] Delete rgw {}".format(host_ip)
        pass

    def create_qos_strategy(self, qos_name, **kwargs):
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
            "__async__": "true"
        }
        ret = self.session.post(url, json=body)
        if ret.status_code == 200:
            print "[success] Create qos {}".format(qos_name)
        else:
            print "[failed] Create qos {}".format(qos_name)

    def qos_user_associate(self, qos_name, tenant_name, user_name):
        """
        用户关联QoS策略

        :param qos_name:
        :param tenant_name:
        :param user_name:
        :return:
        """
        url = "http://{}/api/v3/onestor/{}/objectstorage/qos/user".format(self.host, self.ceph_id)
        body = {
            "qos_name": qos_name,
            "tenant": tenant_name,
            "user": user_name,
            "__async__": "true"
        }
        ret = self.session.post(url, json=body)
        if ret.status_code == 200:
            print "[success] QoS {} associate bucket {}".format(qos_name, user_name)
        else:
            print "[failed] QoS {} associate bucket {}".format(qos_name, user_name)

    def qos_bucket_associate(self, qos_name, tenant_name, bucket_name):
        """
        桶关联QoS策略

        :param qos_name:
        :param tenant_name:
        :param bucket_name:
        :return:
        """
        url = "http://{}/api/v3/onestor/{}/objectstorage/qos/bucket_associate".format(self.host, self.ceph_id)
        body = {
            "qos_name": qos_name,
            "buckets": [
                {
                    "tenant": tenant_name,
                    "bucket": bucket_name
                }
            ],
            "__async__": "true"
        }
        ret = self.session.post(url, json=body)
        if ret.status_code == 200:
            print "[success] QoS {} associate bucket {}".format(qos_name, bucket_name)
        else:
            print "[failed] QoS {} associate bucket {}".format(qos_name, bucket_name)

    def qos_multi_bucket_associate(self, qos_name, buckets_info_list):
        """
        多桶关联QoS策略

        :param qos_name:
        :param buckets_info_list:
        :return:
        """
        url = "http://{}/api/v3/onestor/{}/objectstorage/qos/bucket_associate".format(self.host, self.ceph_id)
        body = {
            "qos_name": qos_name,
            "buckets": buckets_info_list,
            "__async__": "true"
        }
        ret = self.session.post(url, json=body)
        if ret.status_code == 200:
            print "[success] QoS {} associate multi_bucket".format(qos_name)
        else:
            print "[failed] QoS {} associate multi_bucket".format(qos_name)

    def qos_ip_associate(self, qos_name, ip_range):
        """
        QoS策略关联IP网段

        :param qos_name:
        :param ip_range: e.g. "192.168.231.0/24"
        :return:
        """
        url = "http://{}/api/v3/onestor/{}/objectstorage/qos/ip".format(self.host, self.ceph_id)
        body = {
            "ip": ip_range,
            "qos_name": qos_name,
            "__async__": "true"
        }
        ret = self.session.post(url, json=body)
        if ret.status_code == 200:
            print "[success] QoS {} associate ip {}".format(qos_name, ip_range)
        else:
            print "[failed] QoS {} associate ip {}".format(qos_name, ip_range)

    def set_qos_lower_limit(self, **kwargs):
        """
        创建  QoS 下限

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
        url = "http://{}/api/v3/onestor/{}/objectstorage/qos/lower_limit".format(self.host, self.ceph_id)
        body = {
            "put_tps": str(kwargs.get("put_tps")),
            "get_tps": str(kwargs.get("get_tps")),
            "del_tps": str(kwargs.get("del_tps")),
            "list_tps": str(kwargs.get("list_tps")),
            "put_bps": str(kwargs.get("put_bps")),
            "get_bps": str(kwargs.get("get_bps")),
            "concurrent_req": str(kwargs.get("concurrent_req")),
            "description": str(kwargs.get("description")),
            "__async__": "true"
        }
        ret = self.session.post(url, json=body)
        if ret.status_code == 200:
            print "[success] Create qos lower limit"
        else:
            print "[failed] Create qos lower limit"

    def delete_qos_strategy(self, qos_name):
        """
        删除QoS策略

        :param qos_name:
        :return:
        """
        url = "http://{}/api/v3/onestor/{}/objectstorage/qos/policy?" \
              "name={}&__async__=false".format(self.host, self.ceph_id, qos_name)
        ret = self.session.delete(url)
        print ret.content
        if ret.status_code == 200:
            print "[success] Delete QoS {}".format(qos_name)
        else:
            print "[failed] Delete QoS {}".format(qos_name)

    def delete_qos_ip_associate(self, ip_range):
        """
        删除 QoS ip 关联

        :param ip_range:
        :return:
        """
        url = "http://{}/api/v3/onestor/{}/objectstorage/qos/ip/delete".format(self.host, self.ceph_id)
        body = {
            "ips": [ip_range],
            "__async__": "true"
        }
        ret = self.session.post(url, json=body)
        if ret.status_code == 200:
            print "[success] Delete IP {} QoS ".format(ip_range)
        else:
            print "[failed]  Delete IP {} QoS".format(ip_range)

    def update_qos_strategy(self, qos_name, **kwargs):
        """
        修改策略

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
            # "put_bps": str(kwargs.get("put_bps")),
            # "get_bps": str(kwargs.get("get_bps")),
            "concurrent_req": str(kwargs.get("concurrent_req")),
            "description": str(kwargs.get("description")),
            "__async__": "true"
        }
        ret = self.session.patch(url, json=body)
        print json.dumps(ret.content, sort_keys=True, indent=4, default=str)
        if ret.status_code == 200:
            print "[success] Create qos {}".format(qos_name)
        else:
            print "[failed] Create qos {}".format(qos_name)


if __name__ == "__main__":
    api = Api("192.168.163.30")
    # api.update_qos_strategy("test")
    # api.delete_qos_ip_associate("")
    # api.set_qos_lower_limit(get_tps="0123")
    api.qos_user_associate("qos1", "", "testqq1")
    # api.qos_bucket_associate("test", "", "recoverytime")
    # api.qos_ip_associate("qos3", "192.168.243.0/24")
    # api.delete_qos_strategy("qos_auto_2")
    # api.create_qos_strategy("qos_test", put_tps=1)
    # api.create_plat_user_group("peter")
    # api.create_plat_user("peter", "peter")
    # api.create_tenant("peter", "peter")
    # api.create_rgw_ec("192.168.231.116", "datapool")
