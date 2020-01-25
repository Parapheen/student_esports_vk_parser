import requests
import time
from secret import token, mongo_pass
from pymongo import MongoClient
import pprint

token = token
schema = 'https://api.vk.com/method/{method}?{param}&access_token={token}&v=5.103'
mongo_pass = mongo_pass

"""
This script scrapes info for the map of all internet cafes in Moscow
"""
class Computer_arena:
    def __init__(self, link):
        self.name = ''
        self.group_id = ''
        self.photo = ''
        self.domain = link.split('/')[-1]
        self.link = link
        self.city = ''
        self.address = ''
        self.latitude = ''
        self.longitude = ''

    def parse_vk_getById(self):
        url = schema.format(method='groups.getById', param=f'group_id={self.domain}&fields=city', token=token)
        r = requests.get(url)
        if r.status_code == 200:
            r = r.json()
            self.group_id = r['response'][0]['id']
            self.name = r['response'][0]['name']
            self.photo = r['response'][0]['photo_100']
        pass

    def get_address(self):
        url = schema.format(method='groups.getAddresses', param=f'group_id={self.group_id}', token=token)
        r = requests.get(url)
        if r.status_code == 200:
            r = r.json()
            if 'response' in r:
                if 'items' in r ['response']:
                    if len(r['response']['items']) > 0:
                        if r['response']['items'][0]['city_id'] == 1:
                            self.city = 'Moscow'
                        if 'address' in r['response']['items'][0]:
                            self.address = r['response']['items'][0]['address']
                        if 'latitude' in r['response']['items'][0]:
                            self.latitude = r['response']['items'][0]['latitude']
                        if 'longitude' in r['response']['items'][0]:
                            self.longitude = r['response']['items'][0]['longitude']
        pass

    def insert_mongo(self):
        client = MongoClient(
            f"mongodb+srv://Parapheen:{mongo_pass}@cluster0-gg2pa.mongodb.net/test?retryWrites=true&w=majority")
        db = client.ul
        arenas = db['arenas']

        if self.group_id not in arenas.distinct('group_id'):
            result = {'name': self.name,
                      'group_id': self.group_id,
                      'photo': self.photo,
                      'domain': self.domain,
                      'link': self.link,
                      'city': self.city,
                      'address': self.address,
                      'latitude': self.latitude,
                      'longitude': self.longitude}
            arenas.insert_one(result)
        pass




if __name__ == '__main__':
    with open('computer_arenas.txt') as f:
        arenas = [line.strip() for line in f]
    for a in arenas:
        print(a)
        arena = Computer_arena(a)
        time.sleep(2)
        arena.parse_vk_getById()
        arena.get_address()
        arena.insert_mongo()