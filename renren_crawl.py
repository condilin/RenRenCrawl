#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2018/12/8 11:45 
# @Author : Condi 
# @File : renren_crawl.py 
# @Software IDE: PyCharm

# 加载模块
import json
import time
import re
import requests
from jsonpath import jsonpath
from datetime import datetime

# 列表信息url
info_detail = 'https://www.renrendai.com/loan-{}.html'

# 自定义请求头
headers = {
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,la;q=0.7',
    'Connection': 'keep-alive',
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3554.0 Safari/537.36',
    'Referer': 'https://www.renrendai.com/',
    #'Cookie': '__jsluid=fabc0a72205fa2fbd21bbd687d775c46; Hm_lvt_a00f46563afb7c779eef47b5de48fcde=1544239966; gr_user_id=cdd0aeaa-9648-4645-9e1d-eb9f23fb454a; _ga=GA1.2.345666235.1544239966; _gid=GA1.2.884023153.1544239966; renrendaiUsername=185229409%40qq.com; loginMethod=password; mediav=%7B%22eid%22%3A%22301358%22%2C%22ep%22%3A%22%22%2C%22vid%22%3A%22ZC(9Q4H6QC%3Amt%25*F6T(Q%22%2C%22ctn%22%3A%22%22%7D; IS_MOBLIE_IDPASS=true-true; jforumUserInfo=NqFuhg89Mcdx8zFZctukfvTMPat0IEHu%0A; Qs_lvt_181814=1544239966%2C1544269350; gr_cs1_b75151c0-1e92-4aab-bd6f-b688f791e635=user_id%3Anull; activeTimestamp=272527; we_token=MWk5Y3RLUHA4cUJpNTNIS2lvUlhlaDFvbWloZy01bUg6MjcyNTI3OmMwNmFlNzFhYTJhOTMzY2YyYTljMTE1OTQ5ZWY3NWNhNjgxZGMwM2I%3D; we_sid=s%3ASLrdLXN2_D7a99RRbpqqHndkI9yc3d7h.Vr19Z0%2BvF%2FNWv%2BGz4cO8Ol4LvTDgOSIOwSwJIHWmiTE; gr_session_id_bf0acacc0a738790=726be337-7a2e-468e-9b54-8a5a798a7240; gr_cs1_726be337-7a2e-468e-9b54-8a5a798a7240=user_id%3A272527; gr_session_id_bf0acacc0a738790_726be337-7a2e-468e-9b54-8a5a798a7240=true; _zg=%7B%22uuid%22%3A%20%221678bdfe783b5b-03ac962842cabf-75133b4f-144000-1678bdfe78439a%22%2C%22sid%22%3A%201544272281194%2C%22updated%22%3A%201544273533205%2C%22info%22%3A%201544239966087%2C%22superProperty%22%3A%20%22%7B%7D%22%2C%22platform%22%3A%20%22%7B%7D%22%2C%22utm%22%3A%20%22%7B%7D%22%2C%22referrerDomain%22%3A%20%22%22%2C%22cuid%22%3A%20%22272527%22%7D; _gat=1; Qs_pv_181814=4223112285012950000%2C2773992662922008600%2C172168673127607040%2C2702268938200234500%2C651516636937499100; Hm_lpvt_a00f46563afb7c779eef47b5de48fcde=1544273533; JSESSIONID=52112EA84FBCE8925C3DA2E1AC381A07'
}

# 循环获取内容
for i in range(1, 2):  # 2785540
    # 发送请求，获取响应
    response = requests.get(
        # 定义请求 url
        info_detail.format(i),
        # 定义请求头
        headers=headers
    )

    # 获取响应内容
    content = response.content.decode('utf-8')

    # 通过正则匹配获取用户信息
    user_info = re.search(r"var info = \'(.*)\'", content).group(1)

    # 将unicode编码进行替换回utf-8编码
    user_info_clean = user_info.replace('\\u0022', '"').replace('\\u005C', '\\').replace('\\u002D', '-')

    # 将json数据转换为dict
    res_dict = json.loads(user_info_clean)

    # 获取data中的数据, 将结果存放在Info字典中
    info = {'loan': {}, 'borrower': {}, 'userLoanRecord': {}, 'describe': {}}

    # ------------------ 借款信息 -------------------- #
    info['loan']['amount'] = jsonpath(res_dict, '$.loan.amount')[0]  # 标的总额
    info['loan']['interest'] = '%.2f' % jsonpath(res_dict, '$.loan.interest')[0] + '%'  # 年利率
    info['loan']['months'] = str(jsonpath(res_dict, '$.loan.months')[0]) + '个月'  # 还款期限
    # 起息日
    interest_date_timestamps = jsonpath(res_dict, '$.interestDate')[0]
    info['loan']['interest_date'] = time.strftime('%Y-%m-%d', time.localtime(int(interest_date_timestamps)/1000)) if interest_date_timestamps else '放款日当日'
    # 提前还款费率
    monthly_min_interest = jsonpath(res_dict, '$.loan.monthlyMinInterest')[0]
    info['loan']['inrepay_penal_fee'] = '%.2f' % int(re.search('"inRepayPenalFee":"(.*?)"', monthly_min_interest).group(1)) + '%'
    info['loan']['credit_level'] = jsonpath(res_dict, '$.borrower.creditLevel')[0]  # 风险等级
    info['loan']['repay_type'] = '按月还款/等额本息' if jsonpath(res_dict, '$.loan.repayType')[0] == 0 else ''  # 还款方式
    info['loan']['repay_source'] = jsonpath(res_dict, '$.repaySource')[0]  # 还款来源

    # ------------------ 借贷人信息 -------------------- #
    info['borrower']['nick_name'] = jsonpath(res_dict, '$.borrower.nickName')[0]  # 昵称
    info['borrower']['real_name'] = jsonpath(res_dict, '$.borrower.realName')[0]  # 姓名
    info['borrower']['id_no'] = jsonpath(res_dict, '$.borrower.idNo')[0]  # 身份证号
    info['borrower']['gender'] = jsonpath(res_dict, '$.borrower.gender')[0]  # 性别
    info['borrower']['age'] = datetime.now().year - int(jsonpath(res_dict, '$.borrower.birthDay')[0][:4])  # 年龄=当前时间-出生年月
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
    info['userLoanRecord']['failed_count'] = str(jsonpath(res_dict, '$.userLoanRecord.failedCount')[0]) + '笔'  # 严重逾期????????????????

    # ------------------ 贷款描述 -------------------- #
    info['describe']['description'] = jsonpath(res_dict, '$.loan.description')[0]  # 贷款描述

    # ------------------ 其他相关信息 -------------------- #
    # ？？？

    # 打印列表
    print(info)

