#!usr/bin/env python
# -*- coding: utf-8 -*- 
"""
@file: multipart_upload.py
"""
import sys
import time
import botocore.exceptions
from concurrent import futures
from multipart_config import *


class MultiPartUpload(object):

    def __init__(self):
        self.s3_client = self.get_s3_session()

    @staticmethod
    def get_s3_session():
        """
        获取 S3 客户端

        :return: s3 client
        """
        s3_client = boto3.client(
            service_name="s3",
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            endpoint_url=ENDPOINT_URL,
            verify=False
        )
        return s3_client

    def check_access(self):
        """
        检查目标 s3 是否能写入

        :return:
        """
        bucket_flag = True
        try:
            res = self.s3_client.list_buckets()
            for bucket in res["Buckets"]:
                if bucket["Name"] == DESTINATION_BUCKET:
                    bucket_flag = False
            if bucket_flag:
                self.s3_client.create_bucket(Bucket=DESTINATION_BUCKET)
            self.s3_client.put_object(
                Bucket=DESTINATION_BUCKET,
                Key="access_test",
                Body="This is a access test"
                )
        except Exception as e:
            print e.message

    def get_object_info(self):
        """
        查看桶内是否以存在同名对象

        :return:
        """
        try:
            res = self.s3_client.head_object(
                Bucket=DESTINATION_BUCKET,
                Key=MULTIPART_UPLOAD_FILE_NAME
            )
            object_size = res["ContentLength"]
        except botocore.exceptions.ClientError:
            print "对象 {} 不存在，请检查是否存在分片上传信息".format(MULTIPART_UPLOAD_FILE_NAME)
            object_size = -1
        return object_size

    def get_upload_id_list(self):
        """
        获取桶内是否存在未完成的multipart

        :return: 未完成 upload id list
        """
        print "开始检查是否存在未完成分片上传..."
        upload_id_list = []
        res = self.s3_client.list_multipart_uploads(Bucket=DESTINATION_BUCKET)
        try:
            # 如果桶内不存在未完成的分片上传，response里就不存在 Uploads 字段
            for upload_info in res["Uploads"]:
                upload_id_list.append(
                    {
                        "Key": upload_info["Key"],
                        "Initiated": upload_info["Initiated"],
                        "UploadId": upload_info["UploadId"]
                    }
                )
                print "存在未完成分片  对象名 : {}, 初始化上传时间 : {}".format(upload_info["Key"], upload_info["Initiated"])
        except Exception as e:
            # TODO 后面需要确定是哪种异常类型
            print "桶 {} 没有未完成的分片上传".format(DESTINATION_BUCKET)
        return upload_id_list

    def clean_unfinished_upload(self, upload_id_list):
        """
        清理（abort）未完成的分片上传

        :param upload_id_list: 未完成分片上传的 upload id
        :return:
        """
        if upload_id_list:
            keyboard_input = raw_input("是否清理未完成分片，输入 yes 完成清理，其他代表不清理\n")
            if keyboard_input == "yes":
                for upload_info in upload_id_list:
                    self.s3_client.abort_multipart_upload(
                        Bucket=DESTINATION_BUCKET,
                        Key=upload_info["Key"],
                        UploadId=upload_info["UploadId"]
                    )
                    print "清理分片对象 ：{}, UploadId : {}".format(upload_info["Key"], upload_info["UploadId"])
                return True
            else:
                print "不清理未完成分片上传信息"
                return False

    def check_object_exist(self, upload_id_list, clean_flag):
        """
        检查源文件是否存在于对象存储中

        :param clean_flag 是否清理了未完成的分片上传
        :param upload_id_list: 未完成的 upload id
        :return:
        """
        object_size = self.get_object_info()
        if object_size == -1:
            if upload_id_list and (not clean_flag):
                # 查看对象是否存在于未完成的分片上传中，如果不存在，全新上传。如果在继续完成分片上传
                for upload_info in upload_id_list:
                    if MULTIPART_UPLOAD_FILE_NAME in upload_info["Key"]:
                        # 表示存在该对象未完成上传的分片信息，返回该分片信息
                        print "存在对象 {} 分片上传信息".format(MULTIPART_UPLOAD_FILE_NAME)
                        return upload_info["UploadId"]
            else:
                # 表示这个对象不存在，也不存在分片信息。
                print "不存在对象 {} 分片上传信息".format(MULTIPART_UPLOAD_FILE_NAME)
                return self.init_multipart_upload()
        elif object_size == MULTIPART_UPLOAD_FILE_SIZE:
            # 表示对象已经存在，不用重复上传
            print "文件 {} 已存在，请检查后再上传！".format(MULTIPART_UPLOAD_FILE)
            sys.exit(-1)

    def check_part_number_list(self, upload_id):
        """
        检查是否存在以上传的分片

        :param upload_id: 未完成分片上传文件的upload_id
        :return:
        """
        try:
            is_truncated = True
            part_number_marker = 0
            part_number_list = []
            while is_truncated:
                res = self.s3_client.list_parts(
                    Bucket=DESTINATION_BUCKET,
                    Key=MULTIPART_UPLOAD_FILE_NAME,
                    UploadId=upload_id,
                    MaxParts=1000,
                    PartNumberMarker=part_number_marker
                )
                next_part_number_marker = res["NextPartNumberMarker"]
                is_truncated = res["IsTruncated"]
                if next_part_number_marker > 0:
                    for part_number_object in res["Parts"]:
                        part_number_list.append(part_number_object["PartNumber"])
                part_number_marker = next_part_number_marker
            if part_number_list:
                print "已存在上传分片 : {}".format(part_number_list)
        except Exception as e:
            print "Exception error {}, quit \n".format(e.message)
            sys.exit(-1)
        return part_number_list

    @staticmethod
    def split_source_file():
        """
        对文件进行分片

        :return:
        """
        part_number = 1
        index_list = [0]
        while CHUNK_SIZE * part_number < MULTIPART_UPLOAD_FILE_SIZE:
            index_list.append(CHUNK_SIZE * part_number)
            part_number += 1
        if part_number > 10000:
            print "分片数 %s 已超过最大允许分片数 10000，请调整分片大小." % part_number
            sys.exit(-1)
        return index_list

    def upload_thread(self, upload_id, part_number, part_start_index, total):
        """

        :param upload_id: multipart upload id
        :param part_number: 分片数
        :param part_start_index: 分片在文件的偏移位置
        :param total:总分片量
        :return:
        """
        print "Uploading {} / {}".format(part_number, total)
        with open(MULTIPART_UPLOAD_FILE, "rb") as data:
            retry_time = 0
            while retry_time <= MAX_RETRY_TIME:
                try:
                    data.seek(part_start_index)
                    self.s3_client.upload_part(
                        Body=data.read(CHUNK_SIZE),
                        Bucket=DESTINATION_BUCKET,
                        Key=MULTIPART_UPLOAD_FILE_NAME,
                        PartNumber=part_number,
                        UploadId=upload_id
                    )
                    break
                except Exception as e:
                    retry_time += 1
                    print "上传分片失败，日志: {}".format(e.message)
                    print "上传分片失败, 失败part: {}, 重传次数: {}".format(part_number, retry_time)
                    if retry_time > MAX_RETRY_TIME:
                        print "已达最大重传次数 {}，退出".format(retry_time)
                        sys.exit(-1)
                    time.sleep(5)
        print "                        Complete {} / {} ".format(part_number, total)

    def upload_virtual_file_thread(self, upload_id, part_number, upload_body, total):
        """

        :param upload_id: multipart upload id
        :param part_number: 分片数
        :param upload_body: 虚拟文件分片内容
        :param total:总分片量
        :return:
        """
        print "Uploading {} / {}".format(part_number, total)

        retry_time = 0
        while retry_time <= MAX_RETRY_TIME:
            try:
                self.s3_client.upload_part(
                    Body=upload_body,
                    Bucket=DESTINATION_BUCKET,
                    Key=MULTIPART_UPLOAD_FILE_NAME,
                    PartNumber=part_number,
                    UploadId=upload_id
                )
                break
            except Exception as e:
                retry_time += 1
                print "上传分片失败，日志: {}".format(e.message)
                print "上传分片失败, 失败part: {}, 重传次数: {}".format(part_number, retry_time)
                if retry_time > MAX_RETRY_TIME:
                    print "已达最大重传次数 {}，退出".format(retry_time)
                    sys.exit(-1)
                time.sleep(5)
        print "                        Complete {} / {} ".format(part_number, total)

    def upload_part(self, upload_id, index_list, part_number_list):
        """
        多线程上传分片

        :param upload_id: 分片上传的upload id
        :param index_list: 所有分片的
        :param part_number_list:
        :return:
        """
        part_number = 1
        total = len(index_list)
        with futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as pool:
            for part_start_index in index_list:
                if part_number not in part_number_list:
                    pool.submit(self.upload_thread, upload_id, part_number, part_start_index, total)
                part_number += 1
        print "所有分片文件已上传, 文件名: {}, 文件大小:{}".format(MULTIPART_UPLOAD_FILE_NAME, MULTIPART_UPLOAD_FILE_SIZE)
        return part_number - 1

    def upload_virtual_file_part(self, upload_id, index_list, part_number_list):
        """
        多线程上传虚拟文件分片

        :param upload_id: 分片上传的upload id
        :param index_list: 所有分片的
        :param part_number_list:
        :return:
        """
        part_number = 1
        total = len(index_list)
        part_data_body = CHUNK_SIZE * "a"
        with futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as pool:
            for part_start_index in index_list:
                if part_number not in part_number_list:
                    pool.submit(self.upload_virtual_file_thread, upload_id, part_number, part_data_body, total)
                part_number += 1
        print "所有分片文件已上传, 文件名: {}, 文件大小:{}".format(MULTIPART_UPLOAD_FILE_NAME, MULTIPART_UPLOAD_FILE_SIZE)
        return part_number - 1

    def complete_upload(self, upload_id, len_index_list):
        """
        合并分片

        :param upload_id:
        :param len_index_list:
        :return:
        """
        uploaded_list_parts_clean = []
        part_number_marker = 0
        is_truncated = True
        while is_truncated:
            res = self.s3_client.list_parts(
                Bucket=DESTINATION_BUCKET,
                Key=MULTIPART_UPLOAD_FILE_NAME,
                UploadId=upload_id,
                MaxParts=1000,
                PartNumberMarker=part_number_marker
            )
            next_part_number_marker = res["NextPartNumberMarker"]
            is_truncated = res["IsTruncated"]
            if next_part_number_marker > 0:
                for part_object in res["Parts"]:
                    e_tag = part_object["ETag"]
                    part_number = part_object["PartNumber"]
                    add_up = {
                        "ETag": e_tag,
                        "PartNumber": part_number
                    }
                    uploaded_list_parts_clean.append(add_up)
            part_number_marker = next_part_number_marker
        if len(uploaded_list_parts_clean) != len_index_list:
            print "以上传分片数量与源文件分片数量不匹配！！！"
            sys.exit(-1)
        complete_struck_json = {
            "Parts": uploaded_list_parts_clean
        }
        res = self.s3_client.complete_multipart_upload(
            Bucket=DESTINATION_BUCKET,
            Key=MULTIPART_UPLOAD_FILE_NAME,
            UploadId=upload_id,
            MultipartUpload=complete_struck_json
        )
        print "分片上传已完成并合并，对象位置 ：{}".format(res["Location"])

    def init_multipart_upload(self):
        res = self.s3_client.create_multipart_upload(
            Bucket=DESTINATION_BUCKET,
            Key=MULTIPART_UPLOAD_FILE_NAME
        )
        print "文件 {} 初始化分片上传 upload_id {}".format(MULTIPART_UPLOAD_FILE, res["UploadId"])
        return res["UploadId"]

    def start_upload(self):
        clean_flag = False
        # 检查是否能够正常接入
        self.check_access()
        # 获取桶内是否存在分片上传信息
        upload_id_list = self.get_upload_id_list()
        # 是否清理未完成的分片
        if upload_id_list:
            clean_flag = self.clean_unfinished_upload(upload_id_list)
        upload_id = self.check_object_exist(upload_id_list, clean_flag)
        part_number_list = self.check_part_number_list(upload_id)
        file_index_list = self.split_source_file()
        # 分片上传文件
        if VIRTUAL_FILE_FLAG:
            self.upload_virtual_file_part(upload_id, file_index_list, part_number_list)
        else:
            self.upload_part(upload_id, file_index_list, part_number_list)
        # 合并文件
        self.complete_upload(upload_id, len(file_index_list))


if __name__ == "__main__":
    mp = MultiPartUpload()
    mp.start_upload()
