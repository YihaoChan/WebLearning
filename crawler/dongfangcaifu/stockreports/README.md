# 东方财富 - 股票的机构报道

# 1 股票列表

[股票列表](http://quote.eastmoney.com/center/gridlist.html#hs_a_board)中可以看到股票的列表，但注意到该网站是动态网站，不能通过HTML文件获取股票列表。因此，通过Network抓包发现，[交易数据](http://24.push2.eastmoney.com/api/qt/clist/get?cb=jQuery11240277183312100171_1614275380088&pn=1&pz=20&po=0&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f12&fs=m:0+t:6,m:0+t:13,m:0+t:80,m:1+t:2,m:1+t:23&fields=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152&_=1614275380103)为实际后台数据，且xx.push2处，有多种数字可能，即，这些数据放在了不同的服务器上。但为了统一，总不能一个个去试，因此最终确定，1.push2……能够准确访问到后台数据。此外，pz参数是一页所展示的词条总量。修改为5000之后，可以看到所有股票的数据。故最终能确定，[交易数据](http://1.push2.eastmoney.com/api/qt/clist/get?cb=jQuery112403133152782214761_1614156180619&pn=1&pz=5000&po=0&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f12&fs=m:0+t:6,m:0+t:13,m:0+t:80,m:1+t:2,m:1+t:23&fields=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f22f11,f62,f128,f136,f115,f152&_=1614156180684)可以访问到所有的股票当天的信息，且已经按照股票代码从小到大排序，以json文件保存。之后通过筛选data字段下的数据，将股票代码、股票名称、所在上证/深证，写入csv文件保存。

# 2 机构报道

点开任意一支股票，再点开个股研报，通过Network抓包获取点击"下一页"时响应的后台数据，可知[机构报道](http://reportapi.eastmoney.com/report/list?cb=datatable2935314&pageNo=1&pageSize=50&code=000001&industryCode=*&industry=*&rating=*&ratingchange=*&beginTime=2019-02-28&endTime=2021-02-28&fields=&qType=0&p=2&pageNum=2&_=1614490930533)为机构对每支股票的报道时间、标题、机构名称等信息。参数code为不同股票的代码，pageNo为页数。因此，通过之前保存的csv文件，按顺序取出每支股票，根据股票代码组合成对应的参数，并且不断更新页码，作为每一次请求的参数。将不同股票对应的机构报道信息的URL都放到一个集合里，用多进程加速爬取，请求得到它的机构报道，直到某一页数据为空为止，最后遍历拼接并写入csv文件。

