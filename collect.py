#!/usr/bin/python3
# -*- coding: utf8 -*-
import json
import time
import csv

from lxml import html
import requests

import logging

class Collect:

    def __init__(self, loglevel=logging.INFO):
        logging.basicConfig(level=loglevel)
        self.logger = logging.getLogger(__name__)


    def run(self):
        t1 = time.monotonic()
        url = 'https://www.vinbudin.is/addons/origo/module/ajaxwebservices/search.asmx/DoSearch?category=beer&skip=0&count=1000&orderBy=random'

        headers = {
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.108 Safari/537.36',
            'Content-Type': 'application/json; charset=utf-8',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Referer': 'https://www.vinbudin.is/heim/vorur/vorur.aspx/?category=beer/',
            'X-Requested-With': 'XMLHttpRequest',
            'Connection': 'keep-alive'
        }

        rq = json.loads(requests.get(url = url, headers = headers).json()['d'])

        url = 'https://www.vinbudin.is/heim/vorur/stoek-vara.aspx/?productid=%s/'

        data = {}

        self.logger.debug('Open file')
        with open('raw.csv', 'w') as fd:
            writer = csv.writer(fd, delimiter=',')

            skipped = 0
            for i in rq['data']:
                try:
                    rq = requests.get(url = url % (str(i['ProductID']).rjust(5, '0')))
                    root = html.fromstring(rq.text)

                    name = root.xpath('.//span[@id = "ctl01_ctl01_Label_ProductName"]')[0].text
                    price = root.xpath('.//span[@id = "ctl01_ctl01_Label_ProductPrice"]')[0].text
                    tpe = root.xpath('.//span[@class = "taste T60LL"]')
                    if tpe:
                        tpe = tpe[0].xpath('.//span[@class = "text"]')[0].text
                    else:
                        tpe = None
                    desc = root.xpath('.//span[@id = "ctl01_ctl01_Label_ProductDescription"]')[0].text
                    alc = root.xpath('.//span[@id = "ctl01_ctl01_Label_ProductAlchoholVolume"]')[0].text.replace(',', '.')

                    writer.writerow([name, int(price.replace('.', '')), tpe, desc, alc])
                    time.sleep(0.5)

                except Exception as ex:
                    set_trace()
                    print(ex)
                    continue

        self.logger.info('Finished in %s' % (time.monotonic() - t1))

if __name__ == '__main__':
    Collect().run()

    