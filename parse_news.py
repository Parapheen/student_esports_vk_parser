import requests
import time
from secret import token, mongo_pass
import pandas as pd
from pymongo import MongoClient
import datetime
import re

token = token
schema = 'https://api.vk.com/method/{method}?{param}&access_token={token}&v=5.103'
mongo_pass = mongo_pass
client = MongoClient(
        f"mongodb+srv://Parapheen:{mongo_pass}@cluster0-gg2pa.mongodb.net/test?retryWrites=true&w=majority")
db = client.ul
posts = db['posts']

class EsportsСlub:
    def __init__(self, line):
        self.key = ''
        self.name = '' #name of the esports group
        self.owner_id = line[1:-2].split('|')[0][4:] #owner_id
        self.source = '' #vk group url
        self.title = ''
        self.body = ''
        self.short_body = ''
        self.pic = ''
        self.date = 0
        self.post_id = ''
        self.views = 0

    def recent_news(self):
        url = schema.format(method='wall.get', param=f'owner_id=-{self.owner_id}&count=1', token=token)
        r = requests.get(url)
        if r.status_code == 200:
            r = r.json()
            if 'response' in r:
                if 'text' in r['response']['items'][0]:
                    self.body = r['response']['items'][0]['text'].strip('\n')
                    self.short_body = ' '.join(r['response']['items'][0]['text'].strip('\n').split(' ')[0:20])
                    self.title = self.body.split('\n')[0]
                    if 'attachments' in r['response']['items'][0]:
                        if 'photo' in r['response']['items'][0]['attachments'][0]:
                            self.pic = r['response']['items'][0]['attachments'][0]['photo']['sizes'][0]['url']
                    self.date += r['response']['items'][0]['date']
                    self.post_id = r['response']['items'][0]['id']
                    self.source = f'vk.com/wall-{self.owner_id}_{self.post_id}'
                    self.key = f'{self.owner_id}_{self.post_id}'
                    if 'views' in r['response']['items'][0]:
                        self.views += int(r['response']['items'][0]['views']['count'])
            else:
                print(r)
        else:
            print(r)
        return

    def get_club_name(self):
        url = schema.format(method='groups.getById', param=f'group_id={self.owner_id}', token=token)
        r = requests.get(url)
        if r.status_code == 200:
            r = r.json()
            self.name = r['response'][0]['name']
        return

    def insert_res_mongo(self):
        self.key = f'{self.owner_id}_{self.post_id}'
        if self.key not in db['posts'].distinct('vk_id'):
            if len(self.body) > 0:
                result = {'vk_id': self.key,
                        'name': self.name,
                        'club_id': self.owner_id,
                        'post_id': self.post_id,
                        'source_vk': self.source,
                        'title': self.title,
                        'body': self.body,
                        'short_body': self.short_body,
                        'picture': self.pic,
                        'views': self.views,
                        'date': self.date}

                posts.insert_one(result)
        return

    def clean_body(self):
        regex = r"\[(.*?)\]"
        body = self.body
        title = self.title

        def repl(obj):
            # print(obj.string)
            vk_id = obj.group(0).split('|')[0][1::]
            name = obj.group(0).split('|')[-1][0:-1]
            return f'<a href="vk.com/{vk_id}">{name}</a>'


        self.body = re.sub(regex, repl, body)
        self.title = re.sub(regex, repl, title)
        pass


def sort_collection():
    posts.find().sort('views', -1)

# Got the list of esports groups from BAUMAN ESPORTS
# def get_list_of_groups(domain):
#     url = schema.format(method="wall.get", param=f'domain={domain}', token=token)
#
#     r = requests.get(url)
#     if r.status_code == 200:
#         r = r.json()
#         text = r['response']['items'][0]['text']
#         with open('groups.txt', 'w') as f:
#             print(text, file=f)
#     else:
#         print(r)


if __name__ == '__main__':
    with open('groups.txt') as f:
        clubs = [line.strip('\n') for line in f]
    result = dict()
    for club in clubs:
        print('Club Name', club)
        club = EsportsСlub(club)
        time.sleep(1.5)
        club.get_club_name()
        club.recent_news()
        club.clean_body()
        if club.key not in posts.distinct('vk_id'):
            if len(club.body) > 0:
                if club.body != club.title or club.short_body != club.title:
                    result[club.key] = {'vk_id': club.key,
                            'name': club.name,
                            'club_id': club.owner_id,
                            'post_id': club.post_id,
                            'source_vk': club.source,
                            'title': club.title,
                            'body': club.body,
                            'short_body': club.short_body,
                            'picture': club.pic,
                            'views': club.views,
                            'date': club.date}

    df = pd.DataFrame.from_dict(result, orient='index').sort_values(by=['views'], ascending=False)
    date_for_file = datetime.datetime.now()
    df.to_csv(f'{date_for_file}.csv', encoding='UTF-8')
    posts.insert_many(df.to_dict('records'))
    sort_collection()
