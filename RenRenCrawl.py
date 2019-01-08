#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2018/12/9 19:33
# @Author : Condi
# @File : RenRenCrawl.py
# @Software IDE: PyCharm

# 加载模块
import json, time, re, requests
from jsonpath import jsonpath
from datetime import datetime
import pymongo
from queue import Queue  # 导入关于线程的队列
from threading import current_thread
from retrying import retry
from random import choice
from UserAgentSet import USER_AGENTS  # 导入浏览器头
from ProxiesSet import PROXIES  # 导入ip池列表
# from multiprocessing.dummy import Pool  # 导入线程池对象
from gevent.pool import Pool  # 导入协程池对象
import gevent.monkey  # 协程池
gevent.monkey.patch_all()  # 打补丁


class RenRenCrawl(object):

    def __init__(self):
        # 列表信息url
        self.__info_detail = 'https://www.renrendai.com/loan-{}.html'

        # 创建url队列, 用于存放url
        self.__url_list_queue = Queue()
        # 创建线程池对象
        self.__thread_pool = Pool()

        # 打开mongodb连接
        self.__mongo_client = pymongo.MongoClient('127.0.0.1', 27017)
        # 创建db_renren数据库
        self.db = self.__mongo_client.db_renren

    def __del__(self):
        """
        程序结束前关闭mongodb连接
       :return:
        """
        self.__mongo_client.close()

    def get_url_list(self):
        """生产url"""
        for uid in range(1, 809597):  # 2785540
            url = self.__info_detail.format(uid)
            # 将生成的url存放到队列中
            self.__url_list_queue.put(url)

    @retry(stop_max_attempt_number=5)
    def _parse_url_retry(self, url):
        """
        超时重试。
        消费url，获取html源码。
        :param url:
        :return:
        """
        # 自定义请求头
        headers = {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'User-Agent': choice(USER_AGENTS),  # 随机选择一个浏览器头
            'Origin': 'https://www.renrendai.com/',
            'Referer': 'https://www.renrendai.com/loan.html',
        }
        # 从ip代理池中随机选择一个ip
        proxies = {
            "https": "https://" + choice(PROXIES['https']),
        }
        # 超时会报错
        response = requests.get(url, headers=headers, proxies=proxies, timeout=2)
        return response

    def parse_url(self, url):
        """
        消费url，获取html源码
        :param url:
        :return:
        """

        try:
            response = self._parse_url_retry(url)
            print('线程号为：%s, url为：%s, 状态码为：%s' % (current_thread().getName(), url, response.status_code))
            content = response.content.decode('utf-8')
            # 返回内容及状态码
            return content, response.status_code
        except Exception as e:
            content = ''
            status_code = 408
            print('%s Timeout... 状态码为：%s' % (url, status_code))
            return content, status_code

    @staticmethod
    def parse_html(content, url, status_code):
        """
        消费html，生产item
        :return:
        """

        # 如果页面不存在，捕获异常，并保存该链接
        try:
            # 通过正则匹配获取用户信息
            user_info = re.search(r"var info = \'(.*)\'", content).group(1)
            # 将unicode编码进行替换回utf-8编码
            user_info_clean = user_info.replace('\\u0022', '"').replace('\\u005C', '\\').replace('\\u002D', '-')
            # 将json数据转换为dict
            res_dict = json.loads(user_info_clean)
        except Exception as e:
            return {
                'url': url,
                'http_status_code': status_code
            }

        # 将获取的item存放在字典中
        info = {
            'loan': {},
            'borrower': {},
            'userLoanRecord': {},
            'describe': {}
        }
        # ------------------ 借款信息 -------------------- #
        try:
            info['loan']['amount'] = str(jsonpath(res_dict, '$.loan.amount')[0])  # 标的总额
            info['loan']['interest'] = '%.2f' % jsonpath(res_dict, '$.loan.interest')[0] + '%'  # 年利率
            info['loan']['months'] = str(jsonpath(res_dict, '$.loan.months')[0]) + '个月'  # 还款期限
            # 起息日
            interest_date_timestamps = jsonpath(res_dict, '$.interestDate')[0]
            info['loan']['interest_date'] = time.strftime('%Y-%m-%d', time.localtime(
                int(interest_date_timestamps) / 1000)) if interest_date_timestamps else '放款日当日'
            # 提前还款费率
            monthly_min_interest = jsonpath(res_dict, '$.loan.monthlyMinInterest')[0]
            info['loan']['inrepay_penal_fee'] = '%.2f' % int(
                re.search('"inRepayPenalFee":"(.*?)"', monthly_min_interest).group(1)) + '%'
            info['loan']['credit_level'] = jsonpath(res_dict, '$.borrower.creditLevel')[0]  # 风险等级
            info['loan']['repay_type'] = '按月还款/等额本息' if jsonpath(res_dict, '$.loan.repayType')[
                                                            0] == 0 else ''  # 还款方式
            info['loan']['repay_source'] = jsonpath(res_dict, '$.repaySource')[0]  # 还款来源

            # ------------------ 借贷人信息 -------------------- #
            info['borrower']['nick_name'] = jsonpath(res_dict, '$.borrower.nickName')[0]  # 昵称
            info['borrower']['real_name'] = jsonpath(res_dict, '$.borrower.realName')[0]  # 姓名
            info['borrower']['id_no'] = jsonpath(res_dict, '$.borrower.idNo')[0]  # 身份证号
            info['borrower']['gender'] = jsonpath(res_dict, '$.borrower.gender')[0]  # 性别
            info['borrower']['age'] = str(datetime.now().year - int(jsonpath(res_dict, '$.borrower.birthDay')[0][:4]))  # 年龄=当前时间-出生年月
            info['borrower']['graduation'] = jsonpath(res_dict, '$.borrower.graduation')[0]  # 学历
            info['borrower']['marriage'] = '已婚' if jsonpath(res_dict, '$.borrower.marriage')[0] == 'MARRIED' else '未婚'  # 婚姻
            info['borrower']['salary'] = jsonpath(res_dict, '$.borrower.salary')[0]  # 收入
            info['borrower']['has_hose'] = '有房产' if jsonpath(res_dict, '$.borrower.hasHouse')[0] else '无房产'  # 房产
            info['borrower']['house_loan'] = '有房贷' if jsonpath(res_dict, '$.borrower.houseLoan')[0] else '无房贷'  # 房贷
            info['borrower']['has_car'] = '有车产' if jsonpath(res_dict, '$.borrower.hasCar')[0] else '无车产'  # 车产
            info['borrower']['car_loan'] = '有车贷' if jsonpath(res_dict, '$.borrower.carLoan')[0] else '无车贷'  # 车贷
            info['borrower']['office_domain'] = jsonpath(res_dict, '$.borrower.officeDomain')[0]  # 公司行业
            info['borrower']['office_scale'] = jsonpath(res_dict, '$.borrower.officeScale')[0]  # 公司规模
            info['borrower']['position'] = jsonpath(res_dict, '$.borrower.position')[0]  # 岗位职位
            info['borrower']['province'] = jsonpath(res_dict, '$.borrower.province')[0]  # 工作职位
            info['borrower']['work_years'] = jsonpath(res_dict, '$.borrower.workYears')[0]  # 工作时间
            info['borrower']['car_loan'] = jsonpath(res_dict, '$.hasOthDebt')[0] if jsonpath(res_dict, '$.hasOthDebt')[0] else '无'  # 其他负债

            # ------------------ 信用信息 -------------------- #
            info['userLoanRecord']['total_count'] = str(jsonpath(res_dict, '$.userLoanRecord.totalCount')[0]) + '笔'  # 申请借款
            info['userLoanRecord']['available_credits'] = str(jsonpath(res_dict, '$.borrower.availableCredits')[0]) + '元'  # 信用额度
            info['userLoanRecord']['overdue_total_amount'] = str(jsonpath(res_dict, '$.userLoanRecord.overdueTotalAmount')[0]) + '元'  # 逾期金额
            info['userLoanRecord']['success_count'] = str(jsonpath(res_dict, '$.userLoanRecord.successCount')[0]) + '笔'  # 成功借款
            info['userLoanRecord']['borrow_mount'] = str(jsonpath(res_dict, '$.userLoanRecord.borrowAmount')[0]) + '元'  # 借款总额
            info['userLoanRecord']['overdue_count'] = str(jsonpath(res_dict, '$.userLoanRecord.overdueCount')[0]) + '次'  # 逾期次数
            info['userLoanRecord']['already_pay_count'] = str(jsonpath(res_dict, '$.userLoanRecord.alreadyPayCount')[0]) + '笔'  # 还清笔数
            info['userLoanRecord']['notpay_total_amount'] = str(jsonpath(res_dict, '$.userLoanRecord.notPayTotalAmount')[0]) + '元'  # 待还本息
            info['userLoanRecord']['failed_count'] = str(jsonpath(res_dict, '$.userLoanRecord.failedCount')[0]) + '笔'  # 严重逾期

            # ------------------ 贷款描述 -------------------- #
            info['describe']['description'] = jsonpath(res_dict, '$.loan.description')[0]  # 贷款描述

            # ------------------ 其他相关信息 -------------------- #

            return info
        except Exception as e:
            # 错误则返回空
            return info

    def save_to_mongodb(self, info):
        """
        保存数据
        :return:
        """
        # 创建多个集合保存数据(info_set1, info_set2, ...)
        # 保存一条记录使用insert_one, 多条则insert_many
        self.db.info_set2.insert_one(info)

    def exec_task(self):
        """
        执行任务方法
        :return:
        """
        # 从队列中获取url
        time.sleep(1.5)
        url_ = self.__url_list_queue.get()

        # 消费url，获取响应html源码
        time.sleep(1.2)
        content, status_code = self.parse_url(url_)

        # 消费html，生产item
        time.sleep(1.1)
        info = self.parse_html(content, url_, status_code)

        # 保存结果到数据库
        time.sleep(1.3)
        self.save_to_mongodb(info)

        # 通知系统当前任务已完成
        self.__url_list_queue.task_done()

    def exec_task_finished(self, result):
        """
        执行任务完成后的回调方法
        :param result:
            注意，必须要有一个参数接收, 否则会报错。
        :return:
        """
        self.__thread_pool.apply_async(self.exec_task, callback=self.exec_task_finished)

    def run(self):
        # 调用方法,生成url到队列中
        self.get_url_list()

        # 分配任务执行(最多只用到12个线程, 而协程可以用n个)
        for _ in range(500):
            # 执行任务, 执行完之后回调
            self.__thread_pool.apply_async(self.exec_task, callback=self.exec_task_finished)

        # 监控url队列, 直到队列为空, 主线程结束
        self.__url_list_queue.join()


if __name__ == '__main__':
    print('程序开始...\n当前时间为：%s' % time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
    start_time = time.time()
    spider = RenRenCrawl()
    spider.run()
    end_time = time.time()
    print('程序结束...\n当前时间为：%s' % time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
    print('一共耗时：%s 秒，%s 分钟' % (end_time-start_time, (end_time-start_time)//60))
