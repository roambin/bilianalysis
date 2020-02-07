# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class Cloud2Item0(scrapy.Item):
    av_num = scrapy.Field()
    title = scrapy.Field()
    view = scrapy.Field()
    barrage = scrapy.Field()
    like = scrapy.Field()
    coin = scrapy.Field()
    collect = scrapy.Field()
    share = scrapy.Field()
    comment = scrapy.Field()
    tag = scrapy.Field()
    time = scrapy.Field()
    pass

class Cloud2Item1(scrapy.Item):
    av_num = scrapy.Field()
    title = scrapy.Field()
    tag = scrapy.Field()
    time = scrapy.Field()
    pass

class Cloud2Item2(scrapy.Item):
    av_num = scrapy.Field()
    view = scrapy.Field()
    barrage = scrapy.Field()
    like = scrapy.Field()
    coin = scrapy.Field()
    collect = scrapy.Field()
    share = scrapy.Field()
    comment = scrapy.Field()
    pass
