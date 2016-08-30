# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy

# <root_dir>/ptt/items.py
class PttItem(scrapy.Item):
    # define the fields for your item here like:
    title = scrapy.Field()
    authorID = scrapy.Field()
    authorNickname = scrapy.Field()
    date = scrapy.Field()
    ip = scrapy.Field()
    content = scrapy.Field()
    comments = scrapy.Field()
    score = scrapy.Field()
    url = scrapy.Field()
    page = scrapy.Field()
    # pass
