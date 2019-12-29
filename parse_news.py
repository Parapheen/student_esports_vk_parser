import requests
import time
from secret import token, mongo_pass
import pandas as pd
from pymongo import MongoClient
import argparse
import pprint

token = token
schema = 'https://api.vk.com/method/{method}?{param}&access_token={token}&v=5.103'
mongo_pass = mongo_pass

class EsportsСlub:
    def __init__(self, line):
        self.key = ''
        self.name = '' #name of the esports group
        self.owner_id = line[1:-2].split('|')[0][4:] #owner_id
        self.source = '' #vk group url
        self.title = ''
        self.body = ''
        self.pic = ''
        self.date = ''
        self.post_id = ''
        self.views = ''

    def recent_news(self):
        url = schema.format(method='wall.get', param=f'owner_id=-{self.owner_id}&count=1', token=token)
        r = requests.get(url)
        if r.status_code == 200:
            r = r.json()
            if 'response' in r:
                if 'text' in r['response']['items'][0]:
                    self.body = r['response']['items'][0]['text'].strip('\n')
                    self.title = self.body.split('\n')[0]
                    if 'attachments' in r['response']['items']:
                        if 'photo' in r['response']['items'][0]['attachments'][0]:
                            self.pic = r['response']['items'][0]['attachments'][0]['photo']['sizes'][0]['url']
                    self.date = r['response']['items'][0]['date']
                    self.post_id = r['response']['items'][0]['id']
                    self.source = f'vk.com/wall-{self.owner_id}_{self.post_id}'
                    if 'views' in r['response']['items'][0]:
                        self.views = int(r['response']['items'][0]['views']['count'])
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
        client = MongoClient(
            f"mongodb+srv://Parapheen:{mongo_pass}@cluster0-gg2pa.mongodb.net/test?retryWrites=true&w=majority")
        db = client.ul
        posts = db['posts']
        self.key = f'{self.owner_id}_{self.post_id}'
        if self.key not in db['posts'].find():
            if len(self.body) > 0:
                result = {'_id': self.key,
                        'name': self.name,
                        'club_id': self.owner_id,
                        'post_id': self.post_id,
                        'source_vk': self.source,
                        'title': self.title,
                        'body': self.body,
                        'picture': self.pic,
                        'views': self.views,
                        'date': self.date}

                posts.insert_one(result)
        return



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
    res = dict()
    for club in clubs:
        print(club)
        club = EsportsСlub(club)
        time.sleep(2)
        club.get_club_name()
        club.recent_news()
        club.insert_res_mongo()
        # res[f'{club.owner_id}_{club.post_id}'] = {'name': club.name,
        #                                         'club_id': club.owner_id,
        #                                         'post_id': club.post_id,
        #                                         'source_vk': club.source,
        #                                         'title': club.title,
        #                                         'body': club.body,
        #                                         'picture': club.pic,
        #                                         'views': club.views,
        #                                         'date': club.date}
    # df = pd.DataFrame.from_dict(res, orient='index').sort_values(by=['views']).to_csv('initial_res_28_12_19.csv', encoding='UTF-8')
    # result = df.to_json()
    # pprint.pprint(result)
