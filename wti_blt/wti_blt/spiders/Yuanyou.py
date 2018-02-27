# -*- coding: utf-8 -*-
import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from wti_blt.items import WtiBltItem
from scrapy import log
import json, pprint
import requests
import datetime

'''
{"code":200,
"data":
    {"count":34,
    "snapshot":
        {"data_arrs":[
            {"XAUUSD":["黄金",1332.15,1332.15,1332.24,1325.89,1519418467,1329.97,-2.18,-0.16,2,"forexdata","ENDTR"]},
            {"UKOIL":["布伦特原油",66.39,66.39,67.38,65.8,1519423136,67.33,0.94,1.42,2,"forexdata","ENDTR"]},
            {"USOIL":["WTI原油",62.769999999999996,62.769999999999996,63.72,62.34,1519423142,63.58,0.81,1.29,2,"forexdata","ENDTR"]},
            {"XAGUSD":["白银",16.619,16.619,16.639,16.505,1519418467,16.544,-0.075,-0.45,2,"forexdata","ENDTR"]},
            {"COPPER":["纽约铜",3.242,3.242,3.236,3.191,1519423195,3.207,-0.035,-1.06,3,"forexdata","ENDTR"]},
            {"XPTUSD":["铂金",999.4,999.4,1002.3,992,1519423196,998.1,-1.3,-0.13,2,"forexdata","ENDTR"]},
            {"XPDUSD":["钯金",1033.5,1033.5,1042,1028.8,1519423183,1040.65,7.15,0.69,2,"forexdata","ENDTR"]},
            {"NGAS":["天然气",2.634,2.634,2.633,2.555,1519423158,2.63,-0.004,-0.15,3,"forexdata","ENDTR"]},
            {"CORN":["玉米",367.2,367.2,367.75,365.25,1519413594,366.25,-0.95,-0.26,2,"forexdata","ENDTR"]},
            {"WHEAT":["小麦",452,452,456.25,450.75,1519413594,452,0,0,2,"forexdata","ENDTR"]},
            {"SOYBEAN":["大豆",1032.6,1032.6,1039.5,1028.5,1519413597,1037,4.4,0.43,2,"forexdata","ENDTR"]},
            {"SUGAR":["糖",13.68,13.68,13.77,13.59,1519408737,13.68,0,0,2,"forexdata","ENDTR"]},
            {"AUTD":["上海黄金t+d",271.8,271.8,272.48,271.69,1519410598,271.95,0.15,0.06,2,"forexdata","ENDTR"]},
            {"AGTD":["上海白银t+d",3620,3620,3634,3618,1519410599,3621,1,0.03,2,"forexdata","ENDTR"]},
            {"MAUTD":["上海迷你黄金t+d",271.88,271.88,272.5,271.72,1519410598,272.18,0.3,0.11,2,"forexdata","ENDTR"]}],
        "fields":[
                    "prod_name",      名称
                    "preclose_px",  昨收
                    "open_px",      今开
                    "high_px",      最高
                    "low_px",       最低
                    "update_time",  时间
                    "last_px",      最新数据
                    "px_change",    涨幅
                    "px_change_rate",   涨幅百分比
                    "price_precision","market_type","trade_status"]}}}
'''

class YuanyouSpider(CrawlSpider):
    name = 'Yuanyou'
    domain_prefix = 'http://energy.cn'
    allowed_domains = ['forexdata.wallstreetcn.com']
    start_urls = ['https://forexdata.wallstreetcn.com/real_list?fields=prod_name,preclose_px,open_px,high_px,low_px,update_time,last_px,px_change,px_change_rate,price_precision,market_type,trade_status&type=commodity&page=1&limit=15']

    def save_result(self, blt, wti):
        '''
        将blt和wti数据发送到服务器端
        :param blt: 布伦特数据
        :param wti: wti数据
        :return: None
        '''
        url = self.domain_prefix+'/api/SpiderApi/yuanyou'
        dateArray = datetime.datetime.utcfromtimestamp(blt['update_time'])
        update_date = dateArray.strftime("%Y-%m-%d")
        post_data = {'date': update_date,
                     'blt': blt['last_px'], 'blt_rose': blt['px_change_rate'],
                     'wti': wti['last_px'], 'wti_rose': wti['px_change_rate']
                     }
        try:
            r = requests.post(url, data=post_data)
        except Exception:
            # 上报出错了
            url = self.domain_prefix + '/api/SpiderApi/report'
            self.report(url, self.name, 'save_result', '上报发生异常')
            pass
        else:
            if r.status_code != 200:
                url = self.domain_prefix + '/api/SpiderApi/report'
                self.report(url, self.name, 'save_result', '上报返回状态非200')
                pass

    def report(self, url, name, where, reason = ''):
        '''
        上报失败原因
        :param url:
        :param name:
        :param where:
        :param reason:
        :return:
        '''
        post_data = {'name': name, 'where': where, 'reason': reason}
        try:
            r = requests.post(url, data=post_data)
        except Exception:
            # 上报出错了
            pass


    def get_result(self, data_arrs, fields):
        '''
        解析数据
        :param data_arrs: 服务器返回的数据集合
        :param fields: 服务器数据集合每个字段的表示意义
        :return: None
        '''
        update_time = fields.index('update_time') if 'update_time' in fields else -1
        last_px = fields.index('last_px') if 'last_px' in fields else -1
        px_change_rate = fields.index('px_change_rate') if 'px_change_rate' in fields else -1
        if update_time == -1 or last_px == -1 or px_change_rate == -1:
            #数据不对了，需要上报
            url = self.domain_prefix + '/api/SpiderApi/report'
            self.report(url, self.name, 'get_result', json.dumps(fields))
            return

        blt = {}
        wti = {}

        for item in data_arrs:
            for key in item.keys():
                if key == 'UKOIL':
                    # 布伦特
                    blt['update_time'] = item[key][update_time]
                    blt['last_px'] = item[key][last_px]
                    blt['px_change_rate'] = item[key][px_change_rate]
                elif key == 'USOIL':
                    #wti
                    wti['update_time'] = item[key][update_time]
                    wti['last_px'] = item[key][last_px]
                    wti['px_change_rate'] = item[key][px_change_rate]
                else:
                    continue
        #保存wti和blt数据
        print(blt)
        print(wti)
        self.save_result(blt, wti)

    def parse(self, response):
        json_obj = json.loads(response.text)
        try:
            if json_obj['code'] == 200:
                data_arrs = json_obj['data']['snapshot']['data_arrs']
                fields = json_obj['data']['snapshot']['fields']
                self.get_result(data_arrs, fields)
        except Exception:
            #数据不对了，需要上报
            url = self.domain_prefix + '/api/SpiderApi/report'
            self.report(url, self.name, 'parse', json.dumps(json_obj))
            pass
        finally:
            #已经正常流程处理结束
            pass


    # def parse(self, response):
    #     item = WtiBltItem()
    #     tr_blt = response.css('tbody tr')[1].css('td')
    #     blt = {'name': tr_blt[0].css('::text').extract()[0], \
    #            'price': tr_blt[1].css('::text').extract()[0], \
    #            'change': tr_blt[3].css('font::text').extract()[0],\
    #            'date': tr_blt[4].css('::text').extract()[0]}
    #     tr_wti = response.css('tbody tr')[2].css('td')
    #     wti = {'name': tr_wti[0].css('::text').extract()[0], \
    #            'price': tr_wti[1].css('::text').extract()[0], \
    #            'change': tr_wti[3].css('font::text').extract()[0],\
    #            'date': tr_wti[4].css('::text').extract()[0]}
    #
    #     item['blt'] = blt
    #     item['wti'] = wti
    #     log.msg('----------test----------', level=log.ERROR)
    #     return item

