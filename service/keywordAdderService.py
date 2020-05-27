from time import sleep

import jieba

from dao.mongoDbDao import MongoDbDao
from service.taskService import ProgressTask


class KeywordAdderService:
    __authorCollection = MongoDbDao.getCollection('author')
    __videoCollection = MongoDbDao.getCollection('video')
    __searchWordCollection = MongoDbDao.getCollection('search_word')
    __tracerCollection = MongoDbDao.getCollection('tracer')
    jieba.load_userdict('./resources/dict.txt')

    @classmethod
    def addOmitted(cls):
        if cls.__searchWordCollection.count_documents({}) < 100:
            return

        progressTask = ProgressTask("更新查询关键词字典", total_value=cls.__searchWordCollection.count_documents({}),
                                    collection=cls.__tracerCollection)

        with open('./resources/dict.txt', 'r', encoding='utf8').read().split('\n') as fileConnection:
            for each in cls.__searchWordCollection.find():
                if 'aid' in each and each['aid'] not in fileConnection:
                    fileConnection.append(each['aid'])
                elif 'mid' in each and each['mid'] not in fileConnection:
                    fileConnection.append(each['mid'])
                progressTask.current_value += 1

        progressTask.finished = True

        with open('./resources/dict.txt', 'w', encoding='utf8', newline='') as fileConnection:
            for each in fileConnection:
                fileConnection.write(each + '\n')

        cls.__searchWordCollection.delete_many({})
        jieba.load_userdict('./resources/dict.txt')

        cls.__refreshAllAuthor()
        cls.__refreshAllVideo()

    @classmethod
    def __refreshAllAuthor(cls):
        for each_author in cls.__authorCollection.find({}, {'_id': 0, 'mid': 1}).batch_size(100):
            mid = each_author['mid']
            print("[mid]" + str(mid))
            # 关键字从name和official中提取
            authorCollectionResult = cls.__authorCollection.find_one({'mid': mid},
                                                                     {'_id': 0, 'name': 1, 'official': 1, 'keyword': 1})
            keyWord = []
            for each_key in authorCollectionResult:
                if each_key != 'keyword':
                    keyWord.append(str(authorCollectionResult[each_key]).lower())
                else:
                    keyWord += authorCollectionResult['keyword']

            seg_list = jieba.lcut_for_search(' '.join(keyWord), True)  # 搜索引擎模式
            # 全名算作关键字
            if 'name' in authorCollectionResult and authorCollectionResult['name'].lower() not in seg_list:
                seg_list.append(authorCollectionResult['name'].lower())

            while ' ' in seg_list:
                seg_list.remove(' ')
            while '、' in seg_list:
                seg_list.remove('、')

            cls.__authorCollection.update_one({'mid': mid}, {'$set': {'keyword': list(set(seg_list))}})
            sleep(0.01)

    @classmethod
    def __refreshAllVideo(cls):
        for each_video in cls.__videoCollection.find({}, {'_id': 0, 'aid': 1}).batch_size(100):
            aid = each_video['aid']
            print("[aid]" + str(aid))
            # 关键字从name和official中提取
            videoCollectionResult = cls.__videoCollection.find_one({'aid': aid},
                                                                   {'_id': 0, 'title': 1, 'channel': 1, 'subChannel': 1,
                                                                    'author': 1,
                                                                    'tag': 1})
            keyword = []
            for each_key in videoCollectionResult:
                if each_key != 'keyword' or each_key != 'tag':
                    keyword.append(str(videoCollectionResult[each_key]).lower())
                elif each_key == 'tag':
                    keyword += videoCollectionResult['tag']
                else:
                    keyword += videoCollectionResult['keyword']
            seg_list = jieba.lcut_for_search(' '.join(keyword), True)  # 搜索引擎模式

            # 全名算作关键字
            if 'author' in videoCollectionResult and videoCollectionResult['author'].lower() not in seg_list:
                seg_list.append(videoCollectionResult['author'].lower())

            while ' ' in seg_list:
                seg_list.remove(' ')
            while '、' in seg_list:
                seg_list.remove('、')

            cls.__videoCollection.update_one({'aid': aid}, {'$set': {'keyword': list(set(seg_list))}})
            sleep(0.01)
