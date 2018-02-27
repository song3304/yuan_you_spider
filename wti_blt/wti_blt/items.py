# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class WtiBltItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    wti = scrapy.Field()
    blt = scrapy.Field()
    pass
