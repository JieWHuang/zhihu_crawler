from zhihu_crawler_best_answer import ZhiHuCommon


class ZhiHuDataCleaning(object):
    def __init__(self):
        self.ZhiHuPeople = ZhiHuCommon.ZhiHuInfo['people_backup']

    def data_backup(self):
        for item in ZhiHuCommon.ZhiHuInfo['people'].find():
            self.ZhiHuPeople.insert(item)

    def clean_locations_data(self):
        usual_area = [
            '湖南', '湖北', '广东', '广西', '河南', '河北', '山东', '山西', '澳门', '香港', '西藏', '台湾', '贵州', '云南', '四川',
            '北京', '上海', '天津', '重庆', '宁夏', '辽宁', '甘肃', '青海', '陕西', '海南', '内蒙古', '江苏', '江西', '浙江', '黑龙江',
            '新疆', '福建', '吉林', '安徽', '哈尔滨', '长春', '沈阳', '大连', '济南', '青岛', '南京', '杭州', '宁波', '厦门', '广州',
            '深圳', '武汉', '成都', '西安', '美国', '英国', '俄罗斯', '澳大利亚', '日本']
        for i in self.ZhiHuPeople.find():
            for index in range(len(i['locations'])):
                if '省' in i['locations'][index]:
                    i['locations'][index] = i['locations'][index][:(i['locations'][index].index('省'))]
                if '市' in i['locations'][index]:
                    i['locations'][index] = i['locations'][index][:(i['locations'][index].index('市'))]
                for area in usual_area:
                    if area in i['locations'][index]:
                        i['locations'][index] = area
            locations = i['locations']
            self.ZhiHuPeople.update_one({'_id': i['_id']}, {'$set': {'locations': locations}})

    def clean_major_data(self):
        for i in self.ZhiHuPeople.find():
            for index in range(len(i['educations'])):
                if '计算机' in i['educations'][index]['major']:
                    i['educations'][index]['major'] = '计算机科学与技术'

class ZhiHuDataAnalysis(object):
    def __init__(self):
        self.ZhiHuPeople = ZhiHuCommon.ZhiHuInfo['people_backup']

    def gender_ratio(self):
        pipeline = [
            {'$match': {'gender': {'$ne': '暂无数据'}}},
            {'$group': {'_id': '$gender', 'counts': {'$sum': 1}}},
        ]
        for data in self.ZhiHuPeople.aggregate(pipeline):
            gender_data = [data['_id'], data['counts']]
            yield gender_data

    def locations_data_gen(self):
        pipeline = [
            {'$unwind': '$locations'},
            {'$match': {'locations': {'$ne': '暂无数据'}}},
            {'$group': {'_id': '$locations', 'counts': {'$sum': 1}}},
            {'$sort': {'counts': -1}},
            {'$limit': 10}
        ]
        for data in self.ZhiHuPeople.aggregate(pipeline):
            locations_data = {
                'name': data['_id'],
                'data': [data['counts']]
            }
            yield locations_data

    def college_data_gen(self):
        pipeline = [
            {'$unwind': '$educations'},
            {'$match': {'$and': [
                {'educations.school': {'$ne': '暂无数据'}},
                {'educations.school': {'$ne': '大学'}},
                {'educations.school': {'$ne': '大学本科'}},
                {'educations.school': {'$ne': '本科'}}
            ]}},
            {'$group': {'_id': '$educations.school', 'counts': {'$sum': 1}}},
            {'$sort': {'counts': -1}},
            {'$limit': 10}
        ]
        for data in self.ZhiHuPeople.aggregate(pipeline):
            college_data = {
                'name': data['_id'],
                'data': data['counts']
            }
            yield college_data

    def major_data_gen(self):
        pipeline = [
            {'$unwind': '$educations'},
            {'$match': {'educations.major': {'$ne': '暂无数据'}}},
            {'$group': {'_id': '$educations.major', 'counts': {'$sum': 1}}},
            {'$sort': {'counts': -1}},
            {'$limit': 10}
        ]
        for data in self.ZhiHuPeople.aggregate(pipeline):
            major_data = {
                'name': data['_id'],
                'data': data['counts']
            }
            yield major_data

    def business_data_gen(self):
        pipeline = [
            {'$match': {'business': {'$ne': '暂无数据'}}},
            {'$group': {'_id': '$business', 'counts': {'$sum': 1}}},
            {'$sort': {'counts': -1}},
            {'$limit': 10}
        ]
        for data in self.ZhiHuPeople.aggregate(pipeline):
            business_data = {
                'name': data['_id'],
                'data': data['counts']
            }
            yield business_data

    def company_data_gen(self):
        pipeline = [
            {'$unwind': '$employments'},
            {'$match': {'$and': [
                {'employments.company': {'$ne': '暂无数据'}},
                {'employments.company': {'$ne': '学生'}},
                {'employments.company': {'$ne': '自由职业'}},
                {'employments.company': {'$ne': '互联网'}},
                {'employments.company': {'$ne': '无'}}
            ]}},
            {'$group': {'_id': '$employments.company', 'counts': {'$sum': 1}}},
            {'$sort': {'counts': -1}},
            {'$limit': 10}
        ]
        for data in self.ZhiHuPeople.aggregate(pipeline):
            company_data = {
                'name': data['_id'],
                'data': data['counts']
            }
            yield company_data

    def job_data_gen(self):
        pipeline = [
            {'$unwind': '$employments'},
            {'$match': {'$and': [
                {'employments.job': {'$ne': '暂无数据'}},
                {'employments.job': {'$ne': '学生'}}
            ]}},
            {'$group': {'_id': '$employments.job', 'counts': {'$sum': 1}}},
            {'$sort': {'counts': -1}},
            {'$limit': 10}
        ]
        for data in self.ZhiHuPeople.aggregate(pipeline):
            job_data = {
                'name': data['_id'],
                'data': data['counts']
            }
            yield job_data

    def best_author_data(self):
        pipeline = [
            {'$sort': {'follower_count': -1}},
            {'$limit': 100}
        ]
        for data in self.ZhiHuPeople.aggregate(pipeline):
            yield data['author_name']

    def data_analysis(self):
        gender_ratio_data = [data for data in self.gender_ratio()]
        locations_data = [data for data in self.locations_data_gen()]
        college_data = [data for data in self.college_data_gen()]
        major_data = [data for data in self.major_data_gen()]
        business_data = [data for data in self.business_data_gen()]
        company_data = [data for data in self.company_data_gen()]
        job_data = [data for data in self.job_data_gen()]
        best_author100 = [data for data in self.best_author_data()]
        print(job_data)


if __name__ == '__main__':
    data_analysis = ZhiHuDataAnalysis()
    data_analysis.data_analysis()
