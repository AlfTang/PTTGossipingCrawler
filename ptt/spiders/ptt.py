import logging

from datetime import datetime, timedelta
import pytz
import re
from functools import partial
import scrapy
import rethinkdb as r
from ptt.items import PttItem
from scrapy.http import FormRequest

class PTTSpider(scrapy.Spider):
    name = 'ptt'
    allowed_domains = ['ptt.cc']
    start_urls = ('https://www.ptt.cc/bbs/Gossiping/index.html', )

    _retries = 0
    MAX_RETRY = 1
    _pages = 0
    MAX_PAGES = 2
    TZ = pytz.timezone('Asia/Taipei')

    #maxPage = r.table('Gossiping').max('page')['page'].run(conn)
    #conn.close()

    def parse(self, response):
        if len(response.xpath('//div[@class="over18-notice"]')) > 0:
            if self._retries < PTTSpider.MAX_RETRY:
                self._retries += 1
                logging.warning('retry {} times...'.format(self._retries))
                yield FormRequest.from_response(response,
                                                formdata={'yes': 'yes'},
                                                callback=self.parse)
            else:
                logging.warning('you cannot pass')

        else:
            self._pages += 1
            next_page = response.xpath(
                '//div[@id="action-bar-container"]//a[contains(text(), "上頁")]/@href').extract()
            currentPageNum = int(re.search('.*index(\d+)\.html', next_page[0]).group(1)) + 1
            print(currentPageNum)
            for href in response.css('.r-ent > div.title > a::attr(href)'):
                url = response.urljoin(href.extract())
                yield scrapy.Request(url, callback=partial(self.parse_post, page=currentPageNum))

            #if self.maxPage < currentPageNum:
            if self._pages < PTTSpider.MAX_PAGES:
                url = response.urljoin(next_page[0])
                logging.warning('follow {}'.format(url))
                yield scrapy.Request(url, self.parse)
            # Update comments for the posts within 6 hours
            else: 
                #logging.warning('max pages reached')
                conn = r.connect(db='PTT')
                commentURL = list(r.table('Gossiping')
                                   .filter(lambda t: t['date'].during(r.now() - 6*3600, r.now()))['url'].run(conn))
                conn.close()
                for url in commentURL:
                    yield scrapy.Request(url, callback=partial(self.update_comment, url=url))

    def parse_post(self, response, page):
        item = PttItem()
        item['page'] = page
        item['title'] = response.xpath('//meta[@property="og:title"]/@content')[0].extract()
        author = response.xpath('//div[@class="article-metaline"]/span[text()="作者"]/following-sibling::span[1]/text()')\
                         .extract()[0].split(' ')
        item['authorID'] = author[0]
        # ocassionally user doesn't have nickname
        try: 
            item['authorNickname'] = author[1][1:-1]
        except IndexError:
            item['authorNickname'] = ""
        datetime_str = response.xpath(
            '//div[@class="article-metaline"]/span[text()="時間"]/following-sibling::span[1]/text()')[
                0].extract()
        item['date'] = datetime.strptime(datetime_str, '%a %b %d %H:%M:%S %Y').replace(tzinfo=PTTSpider.TZ)
        self.epoch = item['date']
        item['ip'] = response.xpath('//span[@class="f2"]/text()')\
                             .re('\D*(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})')[0]
        item['content'] = "\n".join(response.xpath('//div[@id="main-content"]/text()').extract())

        comments = []
        total_score = 0
        for comment in response.xpath('//div[@class="push"]'):
            push_tag = comment.css('span.push-tag::text')[0].extract()
            push_user = comment.css('span.push-userid::text')[0].extract()
            content = comment.css('span.push-content::text').re('\: (.*)')
            if content:
                push_content = content[0]
            else:
                push_content = []
            push_datetime = comment.css('span.push-ipdatetime::text')[0].extract()[1:-1]

            if '推' in push_tag:
                score = 1
            elif '噓' in push_tag:
                score = -1
            else:
                score = 0

            total_score += score

            comments.append({'user': push_user,
                             'content': push_content,
                             'score': score,
                             'datetime': push_datetime})

        item['comments'] = comments
        item['score'] = total_score
        item['url'] = response.url

        yield item

    def update_comment(self, response, url):
        # find the number of comments of the post with the url
        conn = r.connect(db='PTT')
        commentNum = r.db('PTT').table('Gossiping').filter({'url':url})['comments'].nth(0).count().run(conn)
        # append comment to db
        updateScore = 0
        for comment in response.xpath('//div[@class="push"][position() > {}]'.format(commentNum)):
            push_tag = comment.css('span.push-tag::text')[0].extract()
            push_user = comment.css('span.push-userid::text')[0].extract()
            content = comment.css('span.push-content::text').re('\: (.*)')
            if content:
                push_content = content[0]
            else:
                push_content = []
            push_datetime = comment.css('span.push-ipdatetime::text')[0].extract()[1:-1]

            if '推' in push_tag:
                score = 1
            elif '噓' in push_tag:
                score = -1
            else:
                score = 0

            updateScore += score
            newComment = {'user': push_user,
                          'content': push_content,
                          'score': score,
                          'datetime': push_datetime}
            r.db('PTT').table('Gossiping').filter({'url':url})\
             .update({'comments': r.row['comments'].append(newComment)}).run(conn)
        r.db('PTT').table('Gossiping').filter({'url': url}).update({'score': r.row['score'].add(updateScore)}).run(conn)
        conn.close()
