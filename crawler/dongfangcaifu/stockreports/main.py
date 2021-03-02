import requests
import directory
import numpy as np
import datetime
import pandas as pd
import os
import json
import shutil
from tqdm import tqdm
import multiprocessing
from retrying import retry


class StockReports(object):
    def __init__(self):
        self.__session = requests.Session()  # 复用session

        self.__session.keep_alive = False

        self.__session.trust_env = False

    @retry(stop_max_attempt_number=5)
    def get_name_to_code(self):
        """
        1. 从东方财富网抓包获取沪深A股(包含科创板)的原始数据的URL；
        2. 由于原始数据记录的是当天的信息，即当天涨跌幅、成交量等，故筛选出"代码 - 名称 - 市场"三个属性.
        :return: 代码 - 股票 - 指数的DataFrame
        """
        print("获取所有股票的名称和代码...")

        headers = {
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; '
                          'Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/88.0.4324.182 Mobile Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;'
                      'q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
        }

        raw_info_url = "http://1.push2.eastmoney.com/api/qt/clist/get"

        raw_info_param = (
            ('cb', 'jQuery112403133152782214761_1614156180619'),
            ('pn', '1'),
            ('pz', '5000'),
            ('po', '0'),
            ('np', '1'),
            ('ut', 'bd1d9ddb04089700cf9c27f6f7426281'),
            ('fltt', '2'),
            ('invt', '2'),
            ('fid', 'f12'),
            ('fs', 'm:0 t:6,m:0 t:13,m:0 t:80,m:1 t:2,m:1 t:23'),
            ('fields',
             'f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,'
             'f22,f11,f62,f128,f136,f115,f152'),
            ('_', '1614156180684'),
        )

        """
        连接超时是指在实现客户端到远端机器端口的连接时，Request等待的秒数，即发起请求连接到连接建立之间的最大时长；
        读取超时是指客户端等待服务器返回响应的时间，即连接成功开始到服务器返回响应之间等待的最大时长.
        元组：(连接超时, 读取超时)
        """
        req = self.__session.get(url=raw_info_url, params=raw_info_param, headers=headers, timeout=(3, 7),
                                 verify=False)

        req.encoding = 'UTF-8'

        raw_data = req.text

        # 截断源文件最开始的jQuery请求字符
        jquery_len = len(raw_data.split('(')[0])

        all_data = raw_data[jquery_len + 1: -2]

        # 所有股票的当天信息
        all_data_json = json.loads(all_data)

        # 股票个数
        stock_num = all_data_json["data"]["total"]

        # 股票信息列表
        info_list = all_data_json["data"]["diff"]

        # 获取"代码 - 名称 - 市场"三个属性，写入表格
        name_code_df = pd.DataFrame(columns=('代码', '名称', '市场'))

        for i in tqdm(range(stock_num)):
            info = info_list[i]

            # 加竖线防止读表格时开头的0被隐藏，读取时去掉竖线即可，表格开头两行为上证&深证
            name_code_df.loc[i, "代码"] = '|' + info["f12"]
            name_code_df.loc[i, "名称"] = '|' + info["f14"]
            name_code_df.loc[i, "市场"] = '|' + ("上证" if (info["f13"] == 1) else "深证")

        name_code_df.to_csv(directory.name_code_path, index=False, encoding='gbk')

        print("完成！")

        return pd.read_csv(directory.name_code_path, encoding='gbk')

    def get_reports_params_no_page(self, name_code_df, begin_date, end_date):
        """
        1. 根据股票所在指数，及其股票代码，修改请求的参数；
        2. 添加到能够请求到股票两年内的媒体报道的参数集合中(除页码).
        :param name_code_df: 代码 - 名称 - 指数的DataFrame
        """
        print("获取所有股票的请求参数...")

        # 股票指数，以及代码组成的请求媒体报道的参数
        reports_params = []

        # 需要请求的股票个数
        stock_num = len(name_code_df)

        for i in tqdm(range(stock_num)):
            info = name_code_df.iloc[i]

            code = info["代码"].split("|")[1]

            # 能够请求到每支股票的媒体报道的参数(除页码)，添加进集合中
            reports_params.append((
                ('cb', 'datatable5756981'),
                ('pageSize', '5000'),
                ('code', '%s' % (code)),
                ('industryCode', '*'),
                ('industry', '*'),
                ('rating', '*'),
                ('ratingchange', '*'),
                ('beginTime', '%s' % (begin_date)),
                ('endTime', '%s' % (end_date)),
                ('fields', ''),
                ('qType', '0'),
                ('p', '3'),
                ('pageNum', '1'),
                ('_', '1614450561704'),
            ))

        np.save(directory.reports_params_path, reports_params)

        print("完成！")

        return reports_params

    @retry(stop_max_attempt_number=5)
    def get_reports(
            self, reports_url, reports_param_no_page, reports_df,
            begin_date, end_date, progress, urls_num
    ):
        """
        根据URL及参数，请求得到每支股票发行年份至今的媒体报道，筛选出时间范围内的数据并保存至csv文件.
        :param reports_url: 每支股票的URL
        :param reports_param_no_page: 还没有加上页数的每支股票的请求参数
        :param reports_df: 待更新的时间范围内的媒体报道DataFrame
        :param progress: 已经处理的URL个数
        :param urls_num: 总共需要处理的URL个数
        :return: 已更新的时间范围内的媒体报道DataFrame
        """
        assert begin_date < end_date, "开始日期需小于结束日期"

        print(
            "\r获取第 {} / {} 支股票的媒体报道...当前进度 {:.2f}%".format(
                progress, urls_num, float(progress / urls_num) * 100
            ), end=""
        )

        headers = {
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.190 Mobile Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;'
                      'q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
        }

        page_no = 1

        while True:
            # 加上页码实现翻页
            page_param = ('pageNo', '%d' % (page_no))

            reports_params = reports_param_no_page

            reports_params += (page_param,)

            req = self.__session.get(url=reports_url, params=reports_params, headers=headers, timeout=(3, 7),
                                     verify=False)

            req.encoding = 'UTF-8'

            reports_raw_data = req.text

            # 截断源文件最开始的标识符
            identifier_len = len(reports_raw_data.split('(')[0])

            reports_data = reports_raw_data[identifier_len + 1: -1]

            reports_data_json = json.loads(reports_data)

            # 报道相关数据字段
            data = reports_data_json["data"]

            # 媒体报道条目数
            reports_len = len(data)

            # 没有数据就跳出
            if (reports_len == 0):
                break

            # 股票代码、名称
            code = data[0]["stockCode"]
            name = data[0]["stockName"]

            # 已经记录的条目个数
            pos = len(reports_df)

            for i in range(reports_len):
                # 媒体报道相关信息
                reports_info = data[i]

                # 发布日期
                publish_date = reports_info["publishDate"].split(" ")[0]

                if (begin_date < publish_date and publish_date < end_date):
                    pos += 1

                    reports_df.loc[pos, "代码"] = '|' + code
                    reports_df.loc[pos, "名称"] = '|' + name
                    reports_df.loc[pos, "标题"] = '|' + reports_info["title"]
                    reports_df.loc[pos, "日期"] = '|' + publish_date
                    reports_df.loc[pos, "机构"] = '|' + reports_info["orgSName"]
                    reports_df.loc[pos, "评级"] = '|' + reports_info["emRatingName"]

            del reports_params

            page_no += 1

        return reports_df

    def multiprocessing_crawl(self, get_reports_func, reports_params_no_page, begin_date, end_date):
        print('多进程爬取所有股票的媒体报道...')

        # 创建DataFrame
        reports_df = pd.DataFrame(columns=('代码', '名称', '标题', '日期', '机构', '评级'))

        # 已完成请求的URL个数
        progress = 0

        # 总共需要请求的媒体报道的URL个数
        urls_num = len(reports_params_no_page)

        # 多进程
        pool = multiprocessing.Pool(multiprocessing.cpu_count())

        pool_ret_list = []

        reports_url = "http://reportapi.eastmoney.com/report/list"

        for reports_param in reports_params_no_page:
            progress += 1

            """
            不同的URL得到的apply_async的结果也不同，即处理的不同的URL的返回结果；
            把所有不同的处理结果加进列表；
            将列表中的每一个子部分取出，不断迭代拼接在原始的csv文件后面.
            PS. 不能在处理过程中用apply_async.get()获取返回值，会导致阻塞，运行速度不会变快.
            """
            pool_ret_list.append(pool.apply_async(
                func=get_reports_func,
                args=(
                    reports_url, reports_param, reports_df, begin_date, end_date, progress, urls_num
                )
            ))

        pool.close()
        pool.join()

        reports_list = []

        for temp_df_obj in tqdm(pool_ret_list):
            # 不要使用DataFrame的append方法，df.append不会改变原来的对象，而是创建一个新的对象，会使效率变低且占用更多内存，
            reports_list.append(temp_df_obj.get())

        reports_df = reports_df.append(reports_list)

        """
        BOM(Byte Order Mark)是用来判断文本文件是哪一种Unicode编码的标记，位于文本文件头部.
        1、"utf-8"以字节为编码单元，它的字节顺序在所有系统中都是一样的，没有字节序问题，因此它不需要BOM.
           所以，当用"utf-8"编码方式读取带有BOM的文件时，它会把BOM当做是文件内容来处理, 也就会发生错误；
        2、"utf-8-sig"中sig全拼为signature，也就是"带有签名的utf-8”，因此"utf-8-sig"读取带有BOM的"utf-8文件时"会把BOM单独处理，
           与文本内容隔离开.
        """
        reports_df.to_csv(directory.reports_path, index=False, encoding='utf_8_sig')

        print("完成！")


def main():
    if (os.path.exists(directory.data_dir)):
        shutil.rmtree(directory.data_dir)

    os.mkdir(directory.data_dir)

    stock_reports = StockReports()

    # 获取所有股票的名称和代码
    name_code_df = stock_reports.get_name_to_code()

    # 获取所有股票的请求参数(不含页数)
    begin_date = (datetime.datetime.now() + datetime.timedelta(days=-3650)).strftime("%Y-%m-%d")

    end_date = (datetime.datetime.now() + datetime.timedelta(days=-730)).strftime("%Y-%m-%d")

    reports_params_no_page = stock_reports.get_reports_params_no_page(name_code_df, begin_date, end_date)

    # 多进程爬取所有股票的媒体报道
    stock_reports.multiprocessing_crawl(
        get_reports_func=stock_reports.get_reports, reports_params_no_page=reports_params_no_page,
        begin_date=begin_date, end_date=end_date
    )


if __name__ == '__main__':
    main()