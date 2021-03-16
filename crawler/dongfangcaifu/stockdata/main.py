import requests
import json
import os
import directory
from tqdm import tqdm
import pandas as pd
import multiprocessing
import numpy as np
import shutil
import datetime
from retrying import retry


class StockData(object):
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

        # 股票个数(包含上证指数&深证指数)
        stock_num = all_data_json["data"]["total"] + 2

        # 股票信息列表(除上证指数&深证指数)
        info_list = all_data_json["data"]["diff"]

        # 获取"代码 - 名称 - 市场"三个属性，写入表格
        name_code_df = pd.DataFrame(columns=('代码', '名称', '市场'))

        # 上证、深证单独加入
        name_code_df.loc[0] = ["|000001", "|上证指数", "|上证"]
        name_code_df.loc[1] = ["|399001", "|深证指数", "|深证"]

        # i: 除上证指数&深证指数的迭代索引
        for i in tqdm(range(stock_num - 2)):
            # 除上证指数&深证指数的个股
            info = info_list[i]

            # 加竖线防止读表格时开头的0被隐藏，读取时去掉竖线即可，表格开头两行为上证&深证
            name_code_df.loc[i + 2, "代码"] = '|' + info["f12"]
            name_code_df.loc[i + 2, "名称"] = '|' + info["f14"]
            name_code_df.loc[i + 2, "市场"] = '|' + ("上证" if (info["f13"] == 1) else "深证")

        name_code_df.to_csv(directory.name_code_path, index=False, encoding='gbk')

        print("完成！")

        return pd.read_csv(directory.name_code_path, encoding='gbk')

    def get_weekly_params(self, name_code_df):
        """
        1. 根据股票所在指数，及其股票代码，修改请求的参数；
        2. 添加到能够请求到股票自发行至今的每周交易数据的参数集合中.
        :param name_code_df: 代码 - 名称 - 指数的DataFrame
        :return: 所有股票的请求参数
        """
        print("获取所有股票的请求参数...")

        # 股票指数，以及代码组成的请求每周交易数据的参数
        weekly_params = []

        # 需要请求的股票个数(包含上证指数&深证指数)
        stock_num = len(name_code_df)

        # i: 不包含上证指数&深证指数的迭代索引
        for i in tqdm(range(stock_num)):
            # 除上证指数&深证指数的个股
            info = name_code_df.iloc[i]

            code = info["代码"].split("|")[1]

            market_flag = 1 if (info["市场"].split("|")[1] == "上证") else 0

            # 能够请求到每支股票的每周交易数据的URL，添加进集合中
            # PS. cURL转换工具不会把%2C转义成逗号，要自己手动修改成逗号，否则URL错误
            weekly_params.append((
                ('cb', 'jQuery11240545288405910809_1614185893652'),
                ('fields1', 'f1,f2,f3,f4,f5,f6'),
                ('fields2', 'f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61'),
                ('ut', '7eea3edcaed734bea9cbfc24409ed989'),
                ('klt', '102'),
                ('fqt', '1'),
                ('secid', '%d.%s' % (market_flag, code)),
                ('beg', '0'),
                ('end', '20500000'),
                ('_', '1614185893682')
            ))

        np.save(directory.weekly_params_path, weekly_params)

        print("完成！")

        return weekly_params

    @retry(stop_max_attempt_number=5)
    def get_weekly_info(self, weekly_url, weekly_param, weekly_df, begin_date, end_date, progress, urls_num):
        """
        根据URL及参数，请求得到每支股票发行年份至今的每周交易数据，筛选出时间范围内的数据并保存至csv文件.
        :param weekly_url: 每支股票的URL
        :param weekly_param: 每支股票的请求参数
        :param weekly_df: 待更新的时间范围内的每周交易数据DataFrame
        :param begin_date: 从哪一天开始统计
        :param end_date: 从哪一天结束统计
        :param progress: 已经处理的URL个数
        :param urls_num: 总共需要处理的URL个数
        :return: 已更新的时间范围内的每周交易数据DataFrame
        """
        assert begin_date < end_date, "开始日期需小于结束日期"

        print(
            "\r获取第 {} / {} 支股票的每周交易数据...当前进度 {:.2f}%".format(
                progress, urls_num, float(progress / urls_num) * 100
            ), end=""
        )

        headers = {
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.182 Mobile Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;'
                      'q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
        }

        req = self.__session.get(url=weekly_url, params=weekly_param, headers=headers, timeout=(3, 7), verify=False)

        req.encoding = 'UTF-8'

        weekly_raw_data = req.text

        # 截断源文件最开始的jQuery请求字符
        jquery_len = len(weekly_raw_data.split('(')[0])

        weekly_data = weekly_raw_data[jquery_len + 1: -2]

        weekly_data_json = json.loads(weekly_data)

        # 股票代码、名称、指数
        code = weekly_data_json["data"]["code"]
        name = weekly_data_json["data"]["name"]
        market = weekly_data_json["data"]["market"]

        # 每支股票自发行至今的周K线数据
        klines = weekly_data_json["data"]["klines"]

        # 每周交易数据条目数
        klines_len = len(klines)

        # 已经记录的条目个数
        pos = len(weekly_df)

        # i: 每一周交易数据的条目迭代索引
        for i in range(klines_len):
            # 每周交易数据
            trading_info = klines[i]

            # 交易日期
            date = trading_info.split(",")[0]

            # 选择时间范围内的数据
            if (begin_date < date and date < end_date):
                pos += 1

                weekly_df.loc[pos, "代码"] = '|' + code
                weekly_df.loc[pos, "名称"] = '|' + name
                weekly_df.loc[pos, "市场"] = '|' + ("上证" if (market == 1) else "深证")
                weekly_df.loc[pos, "时间"] = '|' + trading_info.split(",")[0]
                weekly_df.loc[pos, "开盘"] = '|' + trading_info.split(",")[1]
                weekly_df.loc[pos, "收盘"] = '|' + trading_info.split(",")[2]
                weekly_df.loc[pos, "最高"] = '|' + trading_info.split(",")[3]
                weekly_df.loc[pos, "最低"] = '|' + trading_info.split(",")[4]
                weekly_df.loc[pos, "成交量"] = '|' + trading_info.split(",")[5]
                weekly_df.loc[pos, "成交额"] = '|' + trading_info.split(",")[6]
                weekly_df.loc[pos, "振幅"] = '|' + trading_info.split(",")[7]
                weekly_df.loc[pos, "涨跌幅"] = '|' + trading_info.split(",")[8]
                weekly_df.loc[pos, "涨跌额"] = '|' + trading_info.split(",")[9]
                weekly_df.loc[pos, "换手率"] = '|' + trading_info.split(",")[10]

        return weekly_df

    def multiprocessing_crawl(self, get_weekly_info_func, weekly_params, begin_date, end_date):
        print('多进程爬取所有股票的每周交易数据...')

        # 创建DataFrame
        weekly_df = pd.DataFrame(columns=(
            '代码', '名称', '市场', '时间', '开盘', '收盘',
            '最高', '最低', '成交量', '成交额', '振幅', '涨跌幅', '涨跌额', '换手率'
        ))

        # 已完成请求的URL个数
        progress = 0

        # 总共需要请求的每周交易数据的URL个数
        urls_num = len(weekly_params)

        # 多进程
        pool = multiprocessing.Pool(multiprocessing.cpu_count())

        pool_ret_list = []

        weekly_url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"

        for weekly_param in weekly_params:
            progress += 1

            """
            解读为什么如果在这里打印progress，会先一下子把progress加到最大，之后才开始进行真正的请求？

            主进程创建了8个子进程(CPU为8核)，8个子进程都会执行这段代码(父进程创建子进程，即fork之后，子进程会进行fork处后面的代码).
            CPU在这8个进程之间的切换非常快，所以如果在这里打印progress，一下子就能把progress加满.

            而为什么即便用了非阻塞的apply_async，要等到打印progress之后才开始执行真正的网页请求？

            从网页请求到返回也是需要一点时间的，所以没办法很快就返回.同时，查看apply_async源码可知，apply_async将所有的子进程一股
            脑地放进一个队列里，之后由一个负责这个队列的thread把它们分配到可用的pool.换句话说，apply_async先把子进程放到队列里，而它
            们并不是立马执行，而是要等别人来调用它们才开始执行.

            所以显而易见的是，修改progress非常轻松，所以很快就能完成.而真正执行网页请求的函数，并没有那么快返回.这样子造成的"假象"
            就是，以为progress打印完才能轮到请求返回.其实不是的，修改progress特别快，快到第一个请求还没有返回的时候，progress就修改
            完了，然后才轮到第一个向网页的请求返回.
            """

            """
            不同的URL得到的apply_async的结果也不同，即处理的不同的URL的返回结果；
            把所有不同的处理结果加进列表；
            将列表中的每一个子部分取出，不断迭代拼接在原始的csv文件后面.
            PS. 不能在处理过程中用apply_async.get()获取返回值，会导致阻塞，运行速度不会变快.
            """
            pool_ret_list.append(pool.apply_async(
                func=get_weekly_info_func,
                args=(weekly_url, weekly_param, weekly_df, begin_date, end_date, progress, urls_num)
            ))

        pool.close()
        pool.join()

        weekly_info_list = []

        for temp_df_obj in tqdm(pool_ret_list):
            # 不要使用DataFrame的append方法，df.append不会改变原来的对象，而是创建一个新的对象，会使效率变低且占用更多内存，
            weekly_info_list.append(temp_df_obj.get())

        weekly_df = weekly_df.append(weekly_info_list)

        weekly_df.to_csv(directory.weekly_info_path, index=False, encoding='gbk')

        print("完成！")


def main():
    if (os.path.exists(directory.data_dir)):
        shutil.rmtree(directory.data_dir)

    os.mkdir(directory.data_dir)

    stock_data = StockData()

    # 获取所有股票的名称和代码
    name_code_df = stock_data.get_name_to_code()

    # 获取所有股票的请求参数
    weekly_params = stock_data.get_weekly_params(name_code_df)

    # 多进程爬取所有股票的每周交易数据
    begin_date = (datetime.datetime.now() + datetime.timedelta(days=-3650)).strftime("%Y-%m-%d")

    end_date = (datetime.datetime.now() + datetime.timedelta(days=-730)).strftime("%Y-%m-%d")

    stock_data.multiprocessing_crawl(
        get_weekly_info_func=stock_data.get_weekly_info, weekly_params=weekly_params,
        begin_date=begin_date, end_date=end_date
    )


if __name__ == '__main__':
    main()
