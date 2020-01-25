import requests
import time
from secret import token, mongo_pass
import pandas as pd
from pymongo import MongoClient
import datetime
import re

"""
Initializating clients
"""
token = token
schema = 'https://api.vk.com/method/{method}?{param}&access_token={token}&v=5.103'
mongo_pass = mongo_pass
client = MongoClient(
        f"mongodb+srv://Parapheen:{mongo_pass}@cluster0-gg2pa.mongodb.net/test?retryWrites=true&w=majority")
db = client.ul
posts = db['posts']


"""
A class of an esports cafe
"""
class EsportsСlub:
    def __init__(self, line):
        self.key = ''
        self.name = '' #name of the esports group
        self.owner_id = line[1:-2].split('|')[0][4:] #owner_id
        self.source = '' #vk group url
        self.title = '' #title of the news
        self.body = '' #body of the news
        self.short_body = '' 
        self.pictures = []
        self.date = ''
        self.post_id = '' #vk_post_id
        self.views = 0
        self.unixtime = 0

    """
    Getting info via VK API
    """
    def recent_news(self):
        url = schema.format(method='wall.get', param=f'owner_id=-{self.owner_id}&count=1', token=token)
        r = requests.get(url)
        if r.status_code == 200:
            r = r.json()
            if 'response' in r:
                if 'text' in r['response']['items'][0]:
                    self.body = r['response']['items'][0]['text'].replace('\n', '<br>')
                    self.short_body = ' '.join(r['response']['items'][0]['text'].strip('\n').split(' ')[0:20])
                    self.title = self.body.split('\n')[0]
                    if 'attachments' in r['response']['items'][0]:
                        pictures = list(filter(lambda x: x['type'] == 'photo', r['response']['items'][0]['attachments']))
                        self.pictures = list(map(lambda x: x['photo']['sizes'][-1]['url'], pictures))
                    self.unixtime = int(r['response']['items'][0]['date'])
                    self.date = datetime.datetime.utcfromtimestamp(r['response']['items'][0]['date']).strftime('%Y-%m-%d %H:%M:%S')
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
    """
    Convert input name from groups.txt to normal name
    """
    def get_club_name(self):
        url = schema.format(method='groups.getById', param=f'group_id={self.owner_id}', token=token)
        r = requests.get(url)
        if r.status_code == 200:
            r = r.json()
            self.name = r['response'][0]['name']
        return

    """
    regex to parse VK_format links to html tag <a>
    """
    def clean_body(self):
        regex = r"\[(.*?)\]"
        body = self.body
        short_body = self.short_body
        title = self.title

        def repl(obj):
            # print(obj.string)
            vk_id = obj.group(0).split('|')[0][1::]
            name = obj.group(0).split('|')[-1][0:-1]
            return f'<a href="https://vk.com/{vk_id}">{name}</a>'


        self.body = re.sub(regex, repl, body)
        self.short_body = re.sub(regex, repl, short_body)
        self.title = re.sub(regex, repl, title)
        pass

"""
Sorting db
"""
def sort_collection():
    db.posts.find().sort('unixtime', -1)
    pass

"""
Getting the list of esports groups from BAUMAN ESPORTS
"""
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
                            'pictures': club.pictures,
                            'views': club.views,
                            'date': club.date,
                            'unixtime': club.unixtime}
    if len(result) > 0:
        """
        Pandas sort and insert
        """
        df = pd.DataFrame.from_dict(result, orient='index').sort_values(by=['unixtime'], ascending=False)
        date_for_file = datetime.datetime.now() 
        df.to_csv(f'{date_for_file}.csv', encoding='UTF-8') #create csv with new items
        posts.insert_many(df.to_dict('records'))
        sort_collection()
    else:
        sort_collection()
        print("No new posts to be put in mongo")
