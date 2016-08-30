# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import rethinkdb as r

HOST = 'localhost'
PORT = 28015
DB = 'PTT'

class PttPipeline(object):

    def process_item(self, item, spider):
    	r.table('Gossiping').insert(item).run(self.conn)
    	r.table('User').insert({
    		"author":item['authorID'],
    		"nickname":item['authorNickname'],
    		"ip": item['ip']
    	}).run(self.conn)
    	return item

	# Close DB connection if deactivate a spider
    def close_spider(self, spider):
    	self.conn.close()

    # Open DB connection if activate a spider
    def open_spider(self, spider):
    	self.conn = r.connect(host=HOST, port=PORT, db=DB)