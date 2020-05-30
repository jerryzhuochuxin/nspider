import datetime

from dao.mongoDbDao import MongoDbDao
from scipy.interpolate import interp1d


class FansWatcherService:
    __videoCollection = MongoDbDao.getCollection('video')
    __authorCollection = MongoDbDao.getCollection('author')
    __fansVariationCollection = MongoDbDao.getCollection('fans_variation')

    @classmethod
    def watchBigAuthor(cls):
        a = cls.__authorCollection.aggregate([
            {
                '$match': {'data': {'$exists': True}, 'cFans': {'$gt': 10000}}
            }, {
                '$project': {
                    "mid": 1,
                    "face": 1,
                    "name": 1,
                    "data": {
                        "$filter": {
                            "input":
                                "$data",
                            "as": "data",
                            "cond": {"$gt": ["$$data.datetime", datetime.datetime.now() - datetime.timedelta(32)]}
                        }
                    }
                }
            }, {
                "$match": {
                    "data.0": {
                        "$exists": True
                    }
                }
            }
        ])
        for each_author in a:
            cls.__judge(each_author)

    @classmethod
    def __insertEvent(cls, delta_rate, d_daily, author, info, date):
        out_data = {
            'variation': int(d_daily),
            'mid': author['mid'],
            'author': author['name'],
            'face': author['face'],
            'deltaRate': delta_rate,
            'datetime': date.strftime("%Y-%m-%d"),
            'info': info,
        }

        videos = cls.__videoCollection.find({'mid': author['mid']})
        temp_video = {}
        cause = {'type': 'video'}
        for each_v in videos:
            if type(each_v['datetime']) == str:
                pass
            elif (date - each_v['datetime']).days >= -1 and (date - each_v['datetime']).days <= 7:
                temp_video['aid'] = each_v['aid']
                temp_video['title'] = each_v['title']
                temp_video['pic'] = each_v['pic']
                temp_video['cView'] = each_v['data'][0]['view']
                temp_video['channel'] = each_v['channel']
                temp_video['subChannel'] = each_v['subChannel']
                if 'cView' not in temp_video or 'aid' not in cause or temp_video[
                    'cView'] > cause['cView']:
                    cause['aid'] = temp_video['aid']
                    cause['title'] = temp_video['title']
                    cause['pic'] = temp_video['pic']
                    cause['cView'] = temp_video['cView']
                    cause['channel'] = temp_video['channel']
                    cause['subChannel'] = temp_video['subChannel']

        if cause != {'type': 'video'}:
            out_data['cause'] = cause
        cls.__fansVariationCollection.replace_one(
            {'mid': out_data['mid'], 'datetime': out_data['datetime']}, out_data, upsert=True)

    @classmethod
    def __judge(cls, author):
        """
            一共有这样几种可能：
                1、 大量涨粉        日涨粉数超过上周平均的25倍
                2、 史诗级涨粉      日涨粉数超过上周平均的50倍或单日涨粉超过10W
                3、 传说级涨粉      日涨粉数超过上周平均的100倍或单日涨粉超过20W
                4、 急转直下        上升轨道中的UP主突然掉粉
                5、 大量掉粉        每日掉粉数突破5K
                6、 雪崩级掉粉      每日掉粉数突破2W
                7、 末日级掉粉      每日掉粉数突破5W
                8、 新星爆发         日涨粉超过粉丝总数的20%
        """

        data = sorted(author['data'], key=lambda x: x['datetime'])
        start_date = data[0]['datetime'].timestamp()
        end_date = data[-1]['datetime'].timestamp()
        x = []
        y = []
        for each in data:
            x.append(each['datetime'].timestamp())
            y.append(each['fans'])
        if len(x) <= 1:
            return
        # 线性插值
        interrupted_fans = interp1d(x, y, kind='linear')
        temp_date = datetime.datetime.fromtimestamp(start_date)
        c_date = datetime.datetime(temp_date.year, temp_date.month, temp_date.day).timestamp() + 86400 * 3
        if c_date - 86400 * 2 <= start_date:
            return
        while (c_date <= end_date):
            date = datetime.datetime.fromtimestamp(c_date)
            daily_array = interrupted_fans([c_date - 86400, c_date])
            p_daily_array = interrupted_fans([c_date - 86400 * 2, c_date - 86400])

            # 24小时前涨粉数
            pd_daily = p_daily_array[1] - p_daily_array[0]

            # 每日涨粉数
            d_daily = daily_array[1] - daily_array[0]

            if (d_daily >= 5000 or d_daily <= -2000):
                delta_rate = round(d_daily / pd_daily * 100, 2)
                if (d_daily >= daily_array[1] * 0.50):
                    cls.__insertEvent(round(d_daily / daily_array[1] * 100, 2), d_daily, author, '新星爆发', date)
                if (d_daily <= 0 and pd_daily >= 0):
                    cls.__insertEvent('-', d_daily, author, '急转直下', date)
                    c_date += 86400
                    continue
                if (d_daily <= -50000):
                    cls.__insertEvent(delta_rate, d_daily, author, '末日级掉粉', date)
                elif (d_daily <= -20000):
                    cls.__insertEvent(delta_rate, d_daily, author, '雪崩级掉粉', date)
                elif (d_daily <= -5000):
                    cls.__insertEvent(delta_rate, d_daily, author, '大量掉粉', date)

                if (c_date >= start_date * 86400 * 8 and d_daily > 0):
                    weekly_array = interrupted_fans([c_date - 86400 * 8, c_date - 86400])
                    weekly_mean = (weekly_array[1] - weekly_array[0]) / 7
                    # 上周平均涨粉数
                    delta_rate = round(d_daily / weekly_mean * 100, 2)
                    if delta_rate >= 10000 or d_daily >= 200000:
                        # 日涨粉数超过上日的100倍
                        cls.__insertEvent(delta_rate, d_daily, author, '传说级涨粉', date)
                    elif delta_rate >= 5000 or d_daily >= 100000:
                        # 日涨粉数超过上日的50倍
                        cls.__insertEvent(delta_rate, d_daily, author, '史诗级涨粉', date)
                    elif delta_rate >= 2500:
                        # 日涨粉数超过上日的25倍
                        cls.__insertEvent(delta_rate, d_daily, author, '大量涨粉', date)

            c_date += 86400


if __name__ == "__main__":
    FansWatcherService.watchBigAuthor()
