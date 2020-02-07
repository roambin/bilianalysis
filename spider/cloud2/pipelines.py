# -*- coding: utf-8 -*-
from cloud2.io.my_file import File
import json
import pyhdfs
import pymongo
hdfsClient = pyhdfs.HdfsClient(hosts='172.19.240.230:50070')
mongoClient = pymongo.MongoClient('mongodb://root:123@172.19.240.108:27017/admin?w=majority')
collection = mongoClient["crawl"]["data"]


class Cloud2Pipeline(object):
    def process_item(self, item, spider):
        print('av' + str(spider.av_num))
        print(item)
        if item['title'] is not None:
            json_str = json.dumps(dict(item))
            File.write(File.data_file, json_str + '\n', 'a')
            # hdfsClient.append("/data/crawl_data/f1", json_str + '\n')
            # collection.insert_one(dict(item))
            # hdfsClient.delete('/data/streaming/s' + str(spider.av_num))
            # hdfsClient.create('/data/streaming/s' + str(spider.av_num), json_str)
            print('success')
        else:
            print('not existed')
        number = int(spider.av_num) + 1
        File.write(File.number_file, str(number))
        return item
