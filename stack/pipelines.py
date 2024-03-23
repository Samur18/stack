# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import logging
from scrapy.exceptions import DropItem
from scrapy.utils.project import get_project_settings
import pymongo


class MongoDBPipeline(object):

    def __init__(self):
        settings = get_project_settings()
        uri = settings.get("MONGODB_URI")
        self.client = pymongo.MongoClient(uri)
        db = self.client.get_database(settings.get("MONGODB_DB"))
        self.collection = db.get_collection(settings.get("MONGODB_COLLECTION"))

    def process_item(self, item, spider):
        valid = True
        for data in item:
            if not data:
                valid = False
                raise DropItem("Missing {0}!".format(data))
        if valid:
            self.collection.insert_one(dict(item))
            logging.debug("Question added to MongoDB database!")
        return item
