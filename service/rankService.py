import datetime
from venv import logger

from pymongo import DESCENDING

from dao.mongoDbDao import MongoDbDao
from service.taskService import ProgressTask


class RankService:
    @classmethod
    def computeVideoRankTable(cls):
        task_name = '计算视频排名对照表'
        videoCollection = MongoDbDao.getCollection('video')
        count = videoCollection.estimated_document_count()
        top_n = 60
        keys = ['cView', 'cLike', 'cDanmaku', 'cFavorite', 'cCoin', 'cShare']
        task = ProgressTask(task_name, top_n * len(keys), collection=MongoDbDao.getCollection('tracer'))

        videoRankTable = {}
        skip = int(count / 100)
        for each_key_index in range(len(keys)):
            each_key = keys[each_key_index]
            videoRankTable[each_key] = {}
            videoRankTable['name'] = 'video_rank'
            videoRankTable[each_key]['rate'] = []
            last_value = 9999999999
            videoCollectionResult = videoCollection.find({}, {'title': 1}).limit(200).sort(each_key,
                                                                                           DESCENDING).batch_size(200)
            top = 1
            for each_video in list(videoCollectionResult):
                videoRankTable[each_key][each_video['title']] = top
                top += 1

            for i in range(1, top_n + 1):
                task.current_value = i + top_n * each_key_index
                videoCollectionResult = list(videoCollection.find({each_key: {'$lt': last_value}}, {
                    each_key: 1}).limit(1).skip(skip).sort(each_key, DESCENDING))
                if len(videoCollectionResult) != 0:
                    videoCollectionResult = videoCollectionResult[0]
                else:
                    i += 1
                    continue
                if each_key not in videoCollectionResult:
                    break
                last_value = videoCollectionResult[each_key]
                videoRankTable[each_key]['rate'].append(last_value)
                i += 1

        videoRankTable['update_time'] = datetime.datetime.now()
        rankTableCollection = MongoDbDao.getCollection('rank_table')
        rankTableCollection.update_one({'name': 'video_rank'}, {'$set': videoRankTable}, upsert=True)

    @classmethod
    def calculateAuthorRank(cls):
        task_name = "计算作者排名数据"
        authorCollection = MongoDbDao.getCollection('author')
        keys = ['cFans', 'cArchive_view', 'cArticle_view']
        allCount = authorCollection.count_documents({keys[0]: {'$exists': 1}})
        progressTask = ProgressTask(task_name, allCount * len(keys), collection=MongoDbDao.getCollection('tracer'))
        for each_key in keys:
            logger.info("开始计算作者{}排名".format(each_key))
            authorCollectionResult = authorCollection.find({each_key: {'$exists': 1}},
                                                           {'mid': 1, 'rank': 1, each_key: 1}).batch_size(
                300).sort(each_key, DESCENDING)
            if each_key == 'cFans':
                each_rank = 'fansRank'
                each_d_rank = 'dFansRank'
                each_p_rank = 'pFansRank'
            elif each_key == 'cArchive_view':
                each_rank = 'archiveViewRank'
                each_d_rank = 'dArchiveViewRank'
                each_p_rank = 'pArchiveViewRank'
            elif each_key == 'cArticle_view':
                each_rank = 'articleViewRank'
                each_d_rank = 'dArticleViewRank'
                each_p_rank = 'pArticleViewRank'

            iTh = 1
            for each_author in authorCollectionResult:
                progressTask.current_value += 1
                logger.info("计算{}排名".format(each_author['mid']))
                if each_key in each_author:
                    if 'rank' in each_author:
                        rank = each_author['rank']
                        if each_rank in each_author['rank']:
                            rank[each_d_rank] = each_author['rank'][each_rank] - iTh
                        else:
                            rank[each_d_rank] = 0
                        rank[each_rank] = iTh
                        rank[each_p_rank] = cls.__format_p_rank(iTh, allCount)
                    else:
                        # 初始化
                        rank = {
                            each_rank: iTh,
                            each_d_rank: 0,
                            each_p_rank: cls.__format_p_rank(iTh, allCount)
                        }

                if each_author[each_key] == 0:
                    if 'rank' in each_author:
                        rank = each_author['rank']
                        rank[each_d_rank] = 0
                        rank[each_rank] = -1
                        rank[each_p_rank] = -1
                    else:
                        rank = {
                            each_rank: -1,
                            each_d_rank: 0,
                            each_p_rank: -1
                        }
                if each_key == 'cArticle_view':
                    rank['updateTime'] = datetime.datetime.now()
                authorCollection.update_one({'mid': each_author['mid']}, {
                    '$set': {
                        'rank': rank,
                    }
                })
                iTh += 1
        progressTask.current_value = progressTask.total_value
        logger.info("计算作者排名结束")

    @classmethod
    def __format_p_rank(cls, i, count):
        return round(i / count * 100, 2)
