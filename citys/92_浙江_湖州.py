
# -*- utf-8 -*-
'''
在爬虫过程中，由于目录结构与他人不同，用python脚本合并了文件。
时间仓促，合并代码并没有做的多好看，可读性很差。
如需修改（或者查看）代码，需要得到我合并前的文件，再合并一次。
'''

import time


def get_tasks_with_cat(province_id=0, city_id=0, category=0):
    """
    获取tasks表中初始化完毕的任务，参数中包含对应条件
    :param province_id: 0表示所有省份
    :param city_id: 0表示所有市
    :param categor
    y: 0表示所有类别
    :return: 返回对应的url和省市id和对应类别任务
    """
    conn = RawMysql()

    conn.sql = """SELECT * FROM tasks WHERE category_id IS NOT NULL AND flag IS NULL """
    if province_id:
        conn.sql += "AND province_id = %d" % province_id
    if city_id:
        conn.sql += " AND city_id = %d" % city_id
    if category:
        conn.sql += " AND category_id = %d" % category
    return conn.query(0)


def get_tasks_without_cat(province_id=0, city_id=0):
    """
    获取tasks表中所有没有初始化完成的任务来进行类型判断
    :param province_id: 0表示所有省份
    :param city_id: 0表示所有市
    :return: 返回类型ID
    """
    conn = RawMysql()

    conn.sql = """SELECT * FROM tasks WHERE category_id IS NULL AND flag IS NULL """
    if province_id:
        conn.sql += " AND province_id = %d" % province_id
    if city_id:
        conn.sql += " AND city_id = %d" % city_id
    return conn.query(0)


def create_task(url, title, province_id, city_id, create_time, category_id=None, flag=None):
    """
    将抓取的任务链接和标题名等信息存入tasks表
    :return: 
    """
    if '%' in url:
        url = url.replace('%', '%%')
    if '%' in title:
        title = title.replace('%', '%%')
    conn = RawMysql()
    conn.sql = """INSERT INTO tasks(url, title, flag, create_time, category_id, province_id, city_id)
                  VALUES ('%s', '%s ', %s, %s, %s, %d, %d)""" % (
        url,
        title,
        flag if flag is not None else 'NULL',
        '\'{}\''.format(create_time) if create_time else 'NULL',  # 如果输入值是字符串，必须存在单引号
        category_id if category_id is not None else 'NULL',
        province_id,
        city_id
    )
    conn.update()


def judge_task(url, province_id, city_id):
    """
    判断当前任务是否存在与数据库中,不存在为TRUE
    :param url:
    :param province_id:
    :param city_id:
    :return: exist_flag: true or false
    """
    conn = RawMysql()
    conn.sql = """SELECT * FROM tasks WHERE url = '%s' AND province_id = %d AND city_id = %d""" \
               % (url, province_id, city_id)
    return True if len(conn.query(0)) == 0 else False


def save_data(table_name, args=None):
    """
    将页面解析后的数据存入数据库
    :param table_name: 数据库表名
    :param args: 插入值列表或者其他容器
    :return: 
    """
    conn = RawMysql()

    args_string_list = []
    for col in args:
        if isinstance(col, str):
            col = "'%s'" % col
        elif isinstance(col, int):
            col = "%d" % col

        args_string_list.append(col)

    args_string = ",".join(args_string_list)
    conn.sql = """INSERT INTO %s VALUES(id, %s)""" % (table_name, args_string)
    conn.update()


def update_city(city_id, flag):
    """
    更新爬取过的城市状态
    :param city_id: 城市id
    :param flag: 爬取状态，0未爬，1已爬
    :return:
    """
    conn = RawMysql()
    conn.sql = """UPDATE city SET flag=%d WHERE id=%d""" % (flag, city_id)
    conn.update()


def update_task_with_id(task_id, flag):
    """
    更新任务的相关字段
    :flag: 任务完成状态
    :task_id: 任务ID
    :return:
    """
    conn = RawMysql()
    conn.sql = """UPDATE tasks SET flag=%d WHERE id=%d""" % (flag, task_id)
    conn.update()


def update_task(task_id, flag, category_id):
    """
    更新任务的相关字段
    :flag: 任务完成状态
    :category_id: 类型ID
    :task_id: 任务ID
    :return: 
    """
    conn = RawMysql()
    conn.sql = """UPDATE tasks SET flag=%d, category_id=%d WHERE id=%d""" % (flag, category_id, task_id)
    conn.update()


def get_func_path_by_province_city(province_id, city_id):
    """
    根据省市ID获取对应的函数路径
    :param province_id: 省份ID
    :param city_id: 市ID
    :return: 返回拼接之后的接口路径字符串
    """
    conn = RawMysql()
    conn.sql = "SELECT path_name FROM province WHERE id = %d" % province_id
    province_path_name = conn.query()[0]
    conn.sql = "SELECT path_name FROM city where id = %d" % city_id
    city_path_name = conn.query()[0]
    return ".".join([province_path_name, city_path_name])


def update_city_task(city_id, flag=1):
    """
    某个城市任务全部创建完毕后，更新某个城市对应的flag
    :param city_id: 城市ID
    :param flag: 1表示该城市任务已经爬取完毕，0表示未爬取完毕
    :return:
    """
    conn = RawMysql()
    conn.sql = """UPDATE tasks SET flag = %d WHERE city_id = %d""" % (flag, city_id)
    conn.update()


def save_punishment(data):
    """
    存储企业处罚信息
    :param data: 信息元组
    :return:
    """
    conn = RawMysql()
    conn.sql = "INSERT INTO punishment(title, cf_number, reason, basis, category, xz_name, credit_number, org_number, " \
               "business_registration, tax_registration, id_card, legal_person_name, cf_result, cf_date, cf_department, " \
               "cf_state, place_number, update_time, note)" \
               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    conn.update(data)


def get_permit_task(table_name, city_id):
    """
    获取排污许可证任务
    :param table_name: 表名
    :param city_id: 城市
    :return:
    """
    conn = RawMysql()
    conn.sql = """SELECT * FROM %s WHERE flag = 0 AND city_id = %d""" % (table_name, city_id)
    return conn.query(0)


def get_weilan_task(table_name):
    """
    获取蔚蓝地图任务
    :param table_name: 表名
    :param city_id: 城市
    :return:
    """
    conn = RawMysql()
    conn.sql = """SELECT * FROM %s WHERE flag = 0""" % table_name
    return conn.query(0)


def judge_weilan_task(table_name, company_id):
    """
    判断当前任务是否存在与数据库中,不存在为TRUE
    :param table_name:
    :param company_id:
    :return: exist_flag: true or false
    """
    conn = RawMysql()
    conn.sql = """SELECT * FROM %s WHERE company_id = %s""" % (table_name, company_id)
    return True if len(conn.query(0)) == 0 else False


def get_count(table_name):
    """
    返回表单数量
    :param table_name:
    :return:
    """
    conn = RawMysql()
    conn.sql = """SELECT count(*) FROM %s""" % table_name
    return conn.query(1)


def update_weilan_task(table_name, company_id=None, flag=1):
    """
    更新任务状态
    :param table_name: 表名
    :param company_id: 公司ID
    :param flag: 状态标志（1为访问过，默认为NULL）
    :return:
    """
    conn = RawMysql()
    if company_id:
        conn.sql = """UPDATE %s SET flag = %d WHERE company_id = %s""" % (table_name, flag, company_id)
    else:
        conn.sql = """UPDATE %s SET flag = %d""" % (table_name, flag)
    conn.update()


def update_weilan_info(table_name, ol_data, mjs_data, company_id):
    """
    更新蔚蓝地图，超标数据
    :param table_name:
    :param ol_data:
    :param mjs_data:
    :param company_id:
    :return: 
    """
    conn = RawMysql()
    conn.sql = """UPDATE %s SET online_data = '%s', month_jiance_situation = '%s' 
    WHERE company_id = '%s'""" % (table_name, ol_data, mjs_data, company_id)
    conn.update()


def get_jiance_latest_date(table_name, company, point):
    """
    获得该排放口最后更新的时间（超过30天会有数据断层！）
    :param table_name:
    :param company:
    :param point: 排放口
    :return:
    """
    conn = RawMysql()
    conn.sql = """SELECT MAX(release_time) from %s where company = '%s' and paifangkou = '%s'""" % (
        table_name, company, point)
    dtime = conn.query(1)[0]
    return 0 if dtime is None else int(time.mktime(dtime.timetuple()))


def update_permit_task(table_name, id, flag=1):
    """
    更新任务状态
    :param table_name: 表名
    :param id: 公司ID
    :param flag: 状态标志（1为访问过，默认为NULL）
    :return:
    """
    conn = RawMysql()
    conn.sql = """UPDATE %s SET flag = %d WHERE id = %s""" % (table_name, flag, id)
    conn.update()


def transform_date():
    conn = RawMysql()
    conn.sql = "SELECT id, release_time FROM jiance"
    datas = conn.query(0)
    for data in datas:
        release_time = str(data[1])
        temp = release_time.split(" ")[0].replace("/", "-")
        temp = time.strptime(temp, "%Y-%m-%d")
        release_time = time.strftime("%Y-%m-%d", temp)
        conn.sql = """UPDATE jiance SET release_time = %s WHERE id = %d""" % (release_time, data[0])
        conn.update()
        print("========%d========" % data[0])


def get_latest_time(province_id, city_id, category=0):
    conn = RawMysql()
    try:
        if int(category) == 0:
            conn.sql = "select max(create_time) from tasks where (province_id={} and city_id={} and category_id in (0,1,2,3))".format(
                province_id, city_id)
            return conn.query(0)[0][0]
        else:
            conn.sql = "select max(create_time) from tasks where (province_id={} and city_id={} and category_id={})".format(
                province_id, city_id, category)
            return conn.query(0)[0][0]
    except Exception as e:
        print(e)



# -*- coding: utf-8 -*-
import os
import json
import pymysql

DEBUG = True


db_config = {"host": "192.168.1.204", "port": 3306, "user": "root", "passwd": "123", "database": "test_huanping"}
class RawMysql(object):
    def __init__(self, host="", port=3306, user="", passwd="", db="", dbConfig=""):
        try:
            self.sql = u""
            if host != "":
                self.host = host
                self.port = port
                self.user = user
                self.passwd = passwd
                self.db = db
                self.connect = pymysql.connect(host=host, port=port, user=user, passwd=passwd, db=db, charset='utf8')
            else:


                self.host = db_config['host']
                self.port = db_config['port']
                self.user = db_config['user']
                self.passwd = db_config['passwd']
                self.db = db if db != "" else db_config['database']
                self.connect = pymysql.connect(host=db_config['host'], port=db_config['port'], user=db_config['user'],
                                               passwd=db_config['passwd'], db=db if db != "" else db_config['database'],
                                               charset='utf8')
            self.cursor = self.connect.cursor()
        except pymysql.Error as e:
            pass

    def __del__(self):
        self.connect.close()

    def query(self, size=1):
        """
        执行sql，返回记录数由size指定，默认为1，设置成0则全部返回
        :param size:返回的记录数，默认为1，为0时全部返回
        :return:返回查询结果记录集
        """
        self.cursor.execute(self.sql)
        if size == 1:
            return self.cursor.fetchone()
        else:
            return self.cursor.fetchmany(size=size) if size != 0 else self.cursor.fetchall()

    def update(self, args_tuple=()):
        """
        执行数据库更新操作，如插入删除等
        """
        try:
            self.cursor.execute(self.sql, args_tuple)
            self.connect.commit()
            return True
        except Exception as e:
            if DEBUG:
                print(e)
                print(self.sql)
            self.connect.rollback()
            exit(1)
            return False

    def update_many(self, args_list=None):
        """
        执行数据库批量DML操作，如批量插入
        :param args_list:需要传入一个列表，列表中的元素为包含传入参数的元组，sql为带占位符的sql
        """
        if args_list is None:
            args_list = []
        try:

            self.cursor.executemany(self.sql, args_list)
            self.connect.commit()
            return True
        except Exception as e:
            if DEBUG:
                print(e)
            self.connect.rollback()
            return False

    def re_connect(self):
        self.connect = pymysql.connect(host=self.host, port=self.port, user=self.user,
                                       passwd=self.passwd, db=self.db, charset='utf8')
        self.cursor = self.connect.cursor()



import re
import traceback
from urllib.parse import urljoin
import requests
import random
from lxml import etree
import time
from pprint import pprint


# 装饰器: 跳过错误， 记录运行时间
def exception_log(des):
    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                print('错误：', des, e)
                traceback.print_exc()

        return wrapper

    return decorator


# 装饰器: 尝试三次--针对于request
def exception_request(des):
    def decorator(func):
        def wrapper(*args, **kwargs):
            tries = 10
            while tries > 0:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    tries -= 1
                    print('错误：', des, e)
                    print('再次尝试第%d次' % (10 - tries))

        return wrapper

    return decorator


# 方法：request
@exception_request("request")
def request_text(url, enconding='utf-8'):
    # return: 网页文本信息
    # 直接把request加上装饰器就可以避免重复写装饰器或者try
    res = requests.get(url, headers=get_user_agent(), timeout=5)
    res.encoding = enconding
    return res.text


def get_headers(header_raw):
    """
    通过原生请求头获取请求头字典
    :param header_raw: {str} 浏览器请求头
    :return: {dict} headers
    """
    header_raw = header_raw.strip()  # 处理可能的空字符
    header_raw = header_raw.split("\n")  # 分割每行
    header_raw = [line.split(":", 1) for line in header_raw]  # 分割冒号
    header_raw = dict((k.strip(), v.strip()) for k, v in header_raw)  # 处理可能的空字符
    return header_raw


# 方法：post
@exception_request("request.post")
def post_request_text(url, data, enconding='utf-8'):
    # return: 网页文本信息
    # 直接把request加上装饰器就可以避免重复写装饰器或者try
    res = requests.post(url, data=data, headers=get_user_agent(), timeout=5)
    res.encoding = enconding
    return res.text


# 直接根据url获取xpth对象
def html_xpath(url, enconding='utf-8'):
    # return: lxml.HTML.etree对象（可以用于xpath）
    res = request_text(url, enconding)
    return etree.HTML(res)


# 直接根据url获取xpth对象
def post_html_xpath(url, data, enconding='utf-8'):
    # return: lxml.HTML.etree对象（可以用于xpath）
    res = post_request_text(url, data, enconding)
    return etree.HTML(res)


# 获取header
def get_user_agent():
    """
    功能：随机获取HTTP_User_Agent
    """
    user_agents = [
        "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; AcooBrowser; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
        "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; Acoo Browser; SLCC1; .NET CLR 2.0.50727; Media Center PC 5.0; .NET CLR 3.0.04506)",
        "Mozilla/4.0 (compatible; MSIE 7.0; AOL 9.5; AOLBuild 4337.35; Windows NT 5.1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
        "Mozilla/5.0 (Windows; U; MSIE 9.0; Windows NT 9.0; en-US)",
        "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 2.0.50727; Media Center PC 6.0)",
        "Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 1.0.3705; .NET CLR 1.1.4322)",
        "Mozilla/4.0 (compatible; MSIE 7.0b; Windows NT 5.2; .NET CLR 1.1.4322; .NET CLR 2.0.50727; InfoPath.2; .NET CLR 3.0.04506.30)",
        "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN) AppleWebKit/523.15 (KHTML, like Gecko, Safari/419.3) Arora/0.3 (Change: 287 c9dfb30)",
        "Mozilla/5.0 (X11; U; Linux; en-US) AppleWebKit/527+ (KHTML, like Gecko, Safari/419.3) Arora/0.6",
        "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.2pre) Gecko/20070215 K-Ninja/2.1.1",
        "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN; rv:1.9) Gecko/20080705 Firefox/3.0 Kapiko/3.0",
        "Mozilla/5.0 (X11; Linux i686; U;) Gecko/20070322 Kazehakase/0.4.5",
        "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.8) Gecko Fedora/1.9.0.8-1.fc10 Kazehakase/0.5.6",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/535.20 (KHTML, like Gecko) Chrome/19.0.1036.7 Safari/535.20",
        "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; fr) Presto/2.9.168 Version/11.52",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.11 TaoBrowser/2.0 Safari/536.11",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER",
        "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; LBBROWSER)",
        "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E; LBBROWSER)",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.84 Safari/535.11 LBBROWSER",
        "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)",
        "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; QQBrowser/7.0.3698.400)",
        "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E)",
        "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; SV1; QQDownload 732; .NET4.0C; .NET4.0E; 360SE)",
        "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E)",
        "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)",
        "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1",
        "Mozilla/5.0 (iPad; U; CPU OS 4_2_1 like Mac OS X; zh-cn) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8C148 Safari/6533.18.5",
        "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:2.0b13pre) Gecko/20110307 Firefox/4.0b13pre",
        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:16.0) Gecko/20100101 Firefox/16.0",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11",
        "Mozilla/5.0 (X11; U; Linux x86_64; zh-CN; rv:1.9.2.10) Gecko/20100922 Ubuntu/10.10 (maverick) Firefox/3.6.10"
    ]
    user_agent = random.choice(user_agents)
    header = {
        "User-Agent": user_agent
    }
    return header


# huangping数据集
def get_form():
    d = {
        'company': None,
        'project': None,
        'project_location': None,
        'eia_institution': None,
        'publicity_time': None,
        'project_overview': None,
        'main_effect_and_solutions': '',
        'public_participation': None,
        'snapshot': None,
        'remark': None,
        'attachment': None,
        'risk_rank': 0,
        'crawl_time': None,
        'update_time': None,
        'company_linkman': None,
        'company_contact_number': None,
        'eia_institution_linkman': None,
        'eia_institution_contact_number': None,
        'approval_number': None,
        'approval_time': None,
        'approval_unit': None,
        'province_id': None,
        'city_id': None,
        'type_id': None,
    }
    return d


# 打印字典部分数据
def print_dict(d, ign=('snapshot', 'remark'), err=('company', 'project')):
    for i in err:
        if not d[i]:
            print('@数据异常: 无{}'.format(i))
    for i in d.keys():
        if i in ign:
            continue
        if d[i]:
            print('{:20}:{}'.format(i, d[i]))


def save_huanping(data, province_id, city_id, type_id, task_id):
    def less_than_255(x):
        if x:
            return x if len(x) < 255 else x[:255]
        else:
            return None

    if len(data) != 24:
        raise Exception("数据字典，数目异常")
    if data['project'] and data['company'] and len(data['company']) < 30:
        # 不能存在中文括号
        data['company'] = re.sub('\(', '（', data['company'])
        data['company'] = re.sub('\)', '）', data['company'])
        res = (
            data['company'],
            data['project'],
            less_than_255(data['project_location']),
            less_than_255(data['eia_institution']),
            data['publicity_time'],
            data['project_overview'],
            data['main_effect_and_solutions'],
            data['public_participation'],
            data['snapshot'],
            data['remark'],
            data['attachment'],
            data['risk_rank'] if data['risk_rank'] else 0,
            time.strftime("%Y-%m-%d"),
            time.strftime("%Y-%m-%d"),
            data['company_linkman'],
            data['company_contact_number'],
            data['eia_institution_linkman'],
            data['eia_institution_contact_number'],
            data['approval_number'],
            data['approval_time'],
            data['approval_unit'],
            province_id,
            city_id,
            type_id,
        )
        res = tuple(map(handle_none_text, res))
        res = tuple(map(str, res))
        res = tuple(map(lambda x: x.replace('%', '%%'), res))
        res = tuple(map(lambda x: x.replace('\\', '/'), res))
        res = tuple(map(lambda x: x.replace("'", '"'), res))
        save_data('huanping', res)
        update_task_with_id(task_id, 1)
    else:
        if not data['company']:
            print('无company')
        if not data['project']:
            print('无project')


# 处理空数据
def handle_none_text(text):
    if text == None:
        return ''
    else:
        return text


# etree_Element 转 sting,给快照使用
def etree_to_string(ele):
    return etree.tostring(ele, encoding='utf-8').decode('utf-8')


# # 找到网页的表格
# class Table():
#     def __init__(self, html, des='//tbody/tr'):
#         self.table_tr = html.xpath(des)
#         self.data = self.get_data()
#         self.title = self.get_title()
#     def get_title(self):
#         if self.table_tr:
#             return self.table_tr[0].xpath('.//text()')
#
#     def get_data(self):
#         if self.table_tr:
#             tr_data = []
#             for i in map(lambda x: x.xpath('./td'), self.table_tr[1:]):  # 二维数组: 所有行，所有行的列
#                 i = map(lambda x: ''.join(x.xpath('.//text()')), i)  # 一维数组：每一行的所有列的字符串相拼
#                 tr_data.append(list(i))
#             return tr_data
#     def get_url(self):
#         return
#

# 找到网页的表格
def parse_table_with_url(html, des='//tbody/tr'):
    '''
    解析网页的表格数据
    :param html: 可用xpath解析
    :param des: 定位到table的tr
    :return: (标题列表， 其余列的列表（二维）, href)
    '''
    table = html.xpath(des)
    if table:
        #  如果存在列表，获取标题与内容
        # 标题
        tr_title = []
        for i in table[0].xpath('./td'):
            title = i.xpath('.//text()')
            title = list(map(lambda x: x.strip(), title))
            title = list(filter(None, title))
            title = ''.join(title)
            tr_title.append(title)
        # 附件
        tr_href = []
        # 内容
        tr_data = []
        for i in map(lambda x: x.xpath('./td'), table[1:]):  # 二维数组: 所有行，所有行的列
            tr_href.append(i[0].xpath('./..//@href'))
            i = map(lambda x: ''.join(list(map(lambda x1: x1.strip(), x.xpath('.//text()')))), i)  # 一维数组：每一行的所有列的字符串相拼
            tr_data.append(list(i))
    else:
        tr_title, tr_data, tr_href = None, None, None
    return tr_title, tr_data, tr_href


# 处理竖着的表格
def parse_vertical_table(html, des=''):
    '''
    传入html,以及描述对象的对象, 返回字典
    '''
    # 每一行会有两列，所以形成二维数组
    des += '//table/tbody[count(child::tr)>8]/tr'
    table = html.xpath(des)
    t_data = {}
    td = map(lambda x: x.xpath('./td[position()>{}]'.format(0)), table)
    for i in td:
        if len(i) < 2:
            continue
        # 分组
        td_1 = i[0]
        td_2 = i[1]
        # 拿到字符
        td_1 = td_1.xpath('.//text()')
        td_2 = td_2.xpath('.//text()')
        # 去除/n之类
        td_1 = list(map(lambda x: x.strip(), td_1))
        td_2 = list(map(lambda x: x.strip(), td_2))
        # 拼接
        td_1 = ''.join(td_1)
        td_2 = ''.join(td_2)
        # 去除td间的空
        td_1 = re.sub('\s', '', td_1)
        t_data[td_1] = td_2
    td = map(lambda x: x.xpath('./td[position()>{}]'.format(2)), table)
    for i in td:
        if len(i) < 2:
            continue
        # 分组
        td_1 = i[0]
        td_2 = i[1]
        # 拿到字符
        td_1 = td_1.xpath('.//text()')
        td_2 = td_2.xpath('.//text()')
        # 去除/n之类
        td_1 = list(map(lambda x: x.strip(), td_1))
        td_2 = list(map(lambda x: x.strip(), td_2))
        # 拼接
        td_1 = ''.join(td_1)
        td_2 = ''.join(td_2)
        # 去除td间的空
        td_1 = re.sub('\s', '', td_1)
        t_data[td_1] = td_2
    return t_data


def parse_p(html, des='//p'):
    p = html.xpath(des)  # 定位所有的p
    p = list(map(lambda x: x.xpath('.//text()'), p))  # 获取所有字符串
    p = list(map(''.join, p))  # 拼接所有字符
    p = [re.sub('\xa0|\u3000', '', i) for i in p]
    p = list(map(lambda x: x.strip(), p))  # 删除多余的空格
    p = list(filter(None, p))  # 删除空数据
    return p


def parse_div(html, des='//div'):
    div = html.xpath(des)  # 定位所有的div
    div = list(map(lambda x: x.xpath('.//text()'), div))  # 获取所有字符串
    div = list(map(''.join, div))  # 拼接所有字符
    div = [re.sub('\xa0|\u3000', '', i) for i in div]
    div = list(map(lambda x: x.strip(), div))  # 删除多余的空格
    div = list(filter(None, div))  # 删除空数据
    return div


# 格式化时间
def get_time(text):
    '''
    解决时间格式不统一的情况
    :param text:文本
    :return:str
    '''
    if text:
        mo = re.compile('(\d{4}).(\d+).(\d+)').findall(text)
        if mo:
            y = int(mo[0][0])
            m = int(mo[0][1])
            d = int(mo[0][2])
            result = '{:4}-{:02}-{:02}'.format(y, m, d)
        else:
            mo_2 = re.compile('(\d{4}).(\d+)').findall(text)
            if mo_2:
                y = int(mo_2[0][0])
                m = int(mo_2[0][1])
                result = '{:4}-{:02}-01'.format(y, m)
            else:
                print(text, '无法判别时间')
                result = None
        return result
    else:
        return None


def get_risk_rank(title):
    '''
    判断风险等级
    :param text:文本
    :return: str
    '''
    risk_rank = 0
    if title:
        if "报告书" in title:
            risk_rank = 1
        elif "报告表" in title:
            risk_rank = 2
        elif "登记表" in title:
            risk_rank = 3
    return risk_rank


# 返回字典数据
def get_dict_values(key, dict):
    '''
    匹配键值
    :param key: 键
    :param dict: 字典
    :return: str
    '''
    if key in dict.keys():
        return dict[key]
    else:
        return None


def get_all_kinds_of_project(text):
    '''
    从标题中获取proje
    :param text:
    :return:
    '''
    text = re.sub('关于|关于受理|.*?《|环境影响.*|》.*|环评.*|环境.*|环保.*', '', text)
    return text


# 模糊区别公司
def get_all_kinds_of_company(text):
    '''
    从text中获取公司（特别实在title中）
    :param text: 识别的内容
    :return: str
    '''
    # 初始化
    company = None
    # 公司样式， 后续不断添加
    text = re.sub('关于|关于受理|.*?《|环境影响.*|》.*', '', text)
    all_company = ['公司', '研究院', '实验室', '委员会', '管理局', '农场', '总厂', '医院', '屠宰场', '石料厂', '铸件厂', '采摘园', '酒楼', '餐厅', '小区', '公园']
    for i in all_company:
        company_regex = re.compile('.*?' + i).search(text)
        if company_regex:
            company = company_regex.group()
            break
    if company:
        return company


# def match_dicts(data, from_data):
#     '''
#     在字典里找到数据,可能因为个别字符匹配度不高,就会错过数据
#     采用不完全匹配的规则,来匹配数据
#     :param data:
#     :param from_data:
#     :return:
#     '''

# def parse_datatime(t):
#     '''格式化datatime'''
#     t = datatime.

def get_xpath(x):
    '''
    获取元素x的xpath路径
    :param x: etree._Element对象
    :return: str
    '''
    return x.getroottree().getpath(x)


def time_bigger(latest_time, now_time):
    '''
    比较时间大小：如果now>latest, true；否则，false；
    :param latest_time: 时间戳
    :param now_time: 时间戳
    :return: Boolean
    '''
    if now_time > latest_time:
        return True
    else:
        return False


def __parse_table_to_dic(html, xpath):
    '''
    将列表返回为字典
    :param html: HTML对象
    :param xpath: xpatn路径
    :return: 字典的列表
    '''
    res = []  # 最后返回的对象
    ttitle, tdata, thref = parse_table_with_url(html, xpath)
    for i in tdata:
        #  tdata是二维数组
        if len(i) == len(ttitle):
            # 去除空列表
            if ''.join(i):
                res.append(dict(zip(ttitle, i)))
        else:
            print('表头数目与表格数据不对应')
    return res


def __parse_table_dic(d, k, kDict=None):
    '''
    将由表格生成的字典，根据规则，返回特定的值。
    :param d: 字典
    :param k: 类型
    :return: 特定的值
    '''
    if not kDict:
        kDict = {
            'company': ('建设单位', '建设机构', '建设单位（个人）', '施工单位', '规划实施单位'),
            'project': ('项目名称', '建设项目名称', '规划名称'),
            'project_location': ('建设地点', '地点', '施工地址'),
            'eia_institution': ('环评单位', '环评机构', '评价单位名称'),
            'publicity_time': None,
            'project_overview': ('项目概况', '建设内容与规模', '建设内容、规模', '主要建设内容', '建设项目概要', '建设项目概况', '建设内容及规模', '规划定位'),
            'main_effect_and_solutions': ('主要环境影响及预防或者减轻不良环境影响的对策和措施', '施工期的主要环境影响为'),
            'public_participation': ('公众参与情况',),
            'snapshot': None,
            'remark': None,
            'attachment': None,
            'crawl_time': None,
            'update_time': None,
            'risk_rank': None,
            'company_linkman': None,
            'company_contact_number': None,
            'eia_institution_linkman': None,
            'eia_institution_contact_number': None,
            'approval_number': ("审批文号", '文号', '许可证编号', '批复文号'),
            'approval_time': ("审批时间",),
            'approval_unit': None,
            'province_id': None,
            'city_id': None,
            'type_id': None,
        }
    # 拿到对应的集合，看是否有交集
    k = kDict[k]
    if k:
        # print(d.keys(), k)
        i = d.keys() & k  # 交集
        # 有交集，则返回值，否则None
        return None if not i else d[list(i)[0]]
    else:
        return None


def dict_to_data(data, d, kDic=None):
    '''
    将字典的数据存入data
    :param data: data字典
    :param d: 表格字典
    :param html: HTML对象
    :param xpath: xpath路径
    :return: 字典
    '''
    pprint(d)
    for key in data.keys():
        # 当i没有对应值的时候
        if not data[key]:
            data[key] = __parse_table_dic(d, key, kDic)
    if '文件名称' in d.keys():
        if not data['company']:
            data['company'] = get_all_kinds_of_company(d['文件名称'])
        if not data['project']:
            data['project'] = get_all_kinds_of_project(d['文件名称'])
    if '批复名称' in d.keys():
        if not data['company']:
            data['company'] = get_all_kinds_of_company(d['批复名称'])
        if not data['project']:
            data['project'] = get_all_kinds_of_project(d['批复名称'])
    return data


def handle_regex(dscrp, text):
    '''
    对正则加以处理,有信息返回内容,无则NONE
    :param dscrp: 正则描述的字段, 并分好组
    :param text: 识别打文本
    :return: 返回组的第一个 or None
    '''
    mo = re.compile(dscrp)
    mo = mo.search(text)
    if mo:
        return mo.group(1)
    else:
        return None


def handle_compile_dict(namem, d):
    '''
    查看关键字是否在字典中， 并做出返回
    :param namem: 关键字
    :param d: 字典
    :return: str
    '''
    if namem in d.keys():
        return d[namem]
    else:
        return None


def handle_xpath_l(l):
    '''
    将xpath返回的列表，除去无效的空数据， 并链接所有数据
    :param l: 列表
    :return: str
    '''
    l = [i.strip() for i in l]
    l = list(filter(None, l))
    return ''.join(l)


def handle_tag_text(path, html):
    '''
    整合所有的标签内容
    :param path: xpath
    :param html: etree.html
    :return: str列表
    '''
    result_l = []
    results = html.xpath(path)
    for i in results:
        result = i.xpath('.//text()')
        result = list(map(lambda x: x.strip(), result))
        result = list(filter(None, result))
        result = ''.join(result)
        result_l.append(result)
    result_l = list(filter(None, result_l))
    return result_l


def spilt_long_text_by_word(l, text):
    '''
    分割字符串
    :param l: 关键字列表
    :param text: 文本
    :return: list
    '''
    # 剔除不存在的关键字
    l = [i for i in l if i in text]
    # 创建字典储存位置
    key_dict = {}
    for i in l:
        i_location = text.index(i)
        key_dict[i] = i_location
    # 从大到小排序
    key_dict = list(key_dict.items())
    key_dict = list(sorted(key_dict, key=lambda x: x[1]))
    # 储存结果
    result_dict = {}
    for i in range(len(key_dict)):
        if i != len(key_dict) - 1:
            start = key_dict[i][1]
            stop = key_dict[i + 1][1]
            result = text[start:stop]
            result = re.sub(u'\u3000', '', result)
        else:
            start = key_dict[i][1]
            result = text[start:]
            result = re.sub(u'\u3000', '', result)
        result_dict[key_dict[i][0]] = result[len(key_dict[i][0]):]
    return result_dict


@exception_log("批量保存表格时出错=》save_table_to_huanping")
def save_table_to_huanping(province_id, city_id, kind, task_id, url, title, public_time, html, xpath="", to_save=False,
                           data=None):
    '''
    用表格存储的数据
    :param data: ~
    :param province_id: ~
    :param city_id: ~
    :param kind: ~
    :param task_id: ~
    :param title: ~
    :param html: ~
    :param xpath: 主要内容的地方
    :param to_save: 是否保存
    :return:
    '''
    # 删除数据表格的部分单数的行
    for bad in html.xpath(
            xpath + "//table/tbody[count(child::tr) > 2]" + "//tr[count(child::td) < 2]"):
        bad.getparent().remove(bad)
    # 删除隐藏的表格
    for bad in html.xpath(xpath + "//table[contains(@style, 'hidden')]"):
        bad.getparent().remove(bad)
    # 横向表格
    if html.xpath(xpath + "//table/tbody[count(child::tr/child::td) div count(child::tr) > 4]"):
        print('@类型：table_p')
        # 处理xpath路径
        xpath = xpath + "//table/tbody/tr[count(child::td) > 4]"
        # 处理有表格的情况
        table = __parse_table_to_dic(html, xpath)  # 表格数据的字典 所形成的列表
        if not data:
            flag = True
        else:
            flag = False
        for i in table:
            if flag:
                # 每个i是一个字典
                data = get_form()
            data['publicity_time'] = public_time
            data['snapshot'] = etree_to_string(html.xpath(xpath)[0])
            data['risk_rank'] = get_risk_rank(title)
            data['remark'] = url
            data['attachment'] = get_all_attachment(html, xpath, url)
            data = dict_to_data(data, i)  # 将每个字典的数据存入data
            # 最后再确定一遍title
            if not data['project']:
                data['project'] = get_all_kinds_of_project(title)
            if not data['company']:
                data['company'] = get_all_kinds_of_company(title)
            if not data['company'] and data['project']:
                data['company'] = get_all_kinds_of_company(data['project'])
            print_dict(data)
            if to_save:
                save_huanping(data, province_id, city_id, kind, task_id)
    # 纵向表格
    else:
        print('@类型：table_v')
        table = parse_vertical_table(html, xpath)
        if not data:
            # 每个i是一个字典
            data = get_form()
        data['publicity_time'] = public_time
        data['snapshot'] = etree_to_string(html.xpath(xpath)[0])
        data['risk_rank'] = get_risk_rank(title)
        data['remark'] = url
        data['attachment'] = get_all_attachment(html, xpath, url)
        # 将每个字典的数据存入data
        data = dict_to_data(data, table)
        # 最后再确定一遍title
        if not data['project']:
            data['project'] = get_all_kinds_of_project(title)
        if not data['company']:
            data['company'] = get_all_kinds_of_company(title)
        if not data['company'] and data['project']:
            data['company'] = get_all_kinds_of_company(data['project'])
        print_dict(data)
        if to_save:
            save_huanping(data, province_id, city_id, kind, task_id)


def save_aline_to_huanping(province_id, city_id, kind, task_id, url, title, public_time, html, xpath="", min_num=8,
                           to_save=False, data=None):
    '''
    数据一行一行且用分号分割。
    :param num: aline标签最小需要多少个
    :param data: data数据
    :param province_id: ~
    :param city_id: ~
    :param kind: ~
    :param task_id: ~
    :param html: ~
    :param xpath: 主要内容的xpath
    :param to_save: 是否保存
    :return:
    '''

    def save_p_to_huanping(data, html, xpath="//*[count(child::p)>8]/p", num=min_num, kDic=None):
        '''
        aline主要是由p构成
        :param data: data数据
        :param html: html对象
        :param xpath: 主要内容的xpath地址
        :param num: div最小的数量
        :return: 保存好信息的data
        '''
        # 获取每个div的所有文本
        div = parse_div(html, xpath)
        # 根据特性分号分割
        # pprint(div)
        div = [re.split("[:：]", i, maxsplit=1) for i in div]
        # pprint(div)
        div = [i for i in div if len(i) == 2]
        div = dict(div)  # 变化为字典
        pprint(div)
        data = dict_to_data(data, div, kDic)
        return data

    def save_div_to_huanping(data, html, xpath, num=min_num, kDic=None):
        '''
        aline主要是由div构成
        :param data: data数据
        :param html: html对象
        :param xpath: 主要内容的xpath地址
        :param num: div最小的数量
        :return: 保存好信息的data
        '''
        print('@类型：aline-div')
        # 获取每个div的所有文本
        div = parse_div(html, xpath)
        # 根据特性分号分割
        # pprint(div)
        div = [re.split("[:：]", i, maxsplit=1) for i in div]
        # pprint(div)
        div = [i for i in div if len(i) == 2]
        div = dict(div)  # 变化为字典
        pprint(div)
        data = dict_to_data(data, div, kDic)
        return data

    if not data:
        data = get_form()
    data['publicity_time'] = public_time
    data['snapshot'] = etree_to_string(html.xpath(xpath)[0])
    data['risk_rank'] = get_risk_rank(title)
    data['remark'] = url
    data['attachment'] = get_all_attachment(html, xpath, url)
    if html.xpath("{1}[count(child::p)>{0}]/p|{1}//*[count(child::p)>{0}]/p".format(min_num, xpath)):
        data = save_p_to_huanping(data, html,
                                  "{1}[count(child::p)>{0}]/p|{1}//*[count(child::p)>{0}]/p".format(min_num, xpath))
    elif html.xpath("{1}[count(child::div)>{0}]/div|{1}//*[count(child::div)>{0}]/div".format(min_num, xpath)):
        data = save_div_to_huanping(data, html,
                                    "{1}[count(child::div)>{0}]/div|{1}//*[count(child::div)>{0}]/div".format(min_num,
                                                                                                              xpath))

    # 最后再确定一遍title
    if not data['project']:
        data['project'] = get_all_kinds_of_project(title)
    if not data['company']:
        data['company'] = get_all_kinds_of_company(title)
    if not data['company'] and data['project']:
        data['company'] = get_all_kinds_of_company(data['project'])
    print_dict(data)
    if to_save:
        save_huanping(data, province_id, city_id, kind, task_id)


def save_para_to_huanping(province_id, city_id, kind, task_id, url, title, public_time, html, xpath="", min_num=8,
                          to_save=False, data=None):
    '''
    数据为整段用编号分割
    :param data: ~
    :param province_id: ~
    :param city_id: ~
    :param kind: ~
    :param task_id: ~
    :param title: ~
    :param html: ~
    :param xpath: 定位到主要内容
    :param min_num: 标签的最小数量，用于筛选
    :param to_save: 是否保存
    :return:
    '''
    print('@类型：para')
    if not data:
        data = get_form()
    data['publicity_time'] = public_time
    data['snapshot'] = etree_to_string(html.xpath(xpath)[0])
    data['risk_rank'] = get_risk_rank(title)
    data['remark'] = url
    data['attachment'] = get_all_attachment(html, xpath, url)
    # 获取段落文字
    para = parse_div(html,
                     des='{1}[count(child::div)>{0}]/div|{1}//*[count(child::div)>{0}]/div'.format(min_num, xpath))
    # 为大标题设置好flag
    para = ['!@#' + i if re.search('^[一二三四五六七八九]', i) else i for i in para]
    # 分出段落层次
    para = '\n'.join(para)  # 先合并
    para = para.split('!@#')  # 再分割
    para = [i for i in para if re.search('^[一二三四五六七八九]', i)]  # 删除没有标序号的部分
    para = dict([i.strip().split('\n', maxsplit=1) for i in para])  # 变换成字典
    # 从中获取数据
    for i in para.keys():
        if re.search('名称', i) and re.search('概要', i):
            data['project'] = handle_regex('名称.(.*)', para[i])
            data['project_overview'] = handle_regex('概要.(.*)', para[i])
        if re.search('影响', i) or re.search('措施|对策', i):
            data['main_effect_and_solutions'] += para[i]
        if re.search('环评|评价', i):
            data['eia_institution'] = handle_regex('(?:单位|名称).*?[:：](.*?)\s', para[i])
            data['eia_institution_linkman'] = handle_regex('联系人.*?[:：](.*?)\s', para[i])
            data['eia_institution_contact_number'] = handle_regex('([1-9-])', para[i])
        if re.search('建设|项目基本情况', i) and re.search('单位|机构', i):
            data['company'] = handle_regex('(?:单位|机构|公司).*?[:：](.*?)\s', para[i])
            data['company_linkman'] = handle_regex('联系人.*?[:：](.*?)\s', para[i])
            data['company_contact_number'] = handle_regex('([1-9-])', para[i])
    # 最后再确定一遍title
    if not data['project']:
        data['project'] = get_all_kinds_of_project(title)
    if not data['company']:
        data['company'] = get_all_kinds_of_company(title)
    if not data['company'] and data['project']:
        data['company'] = get_all_kinds_of_company(data['project'])
    # 保存数据
    print_dict(data)
    if to_save:
        save_huanping(data, province_id, city_id, kind, task_id)


def save_title_to_huanping(province_id, city_id, kind, task_id, url, title, public_time, html, xpath="", to_save=False,
                           data=None):
    print('@类型：title')
    if not data:
        data = get_form()
    data['publicity_time'] = public_time
    data['snapshot'] = etree_to_string(html.xpath(xpath)[0])
    data['risk_rank'] = get_risk_rank(title)
    data['remark'] = url
    data['attachment'] = get_all_attachment(html, xpath, url)
    # 获取数据
    data['project'] = get_all_kinds_of_project(title)
    data['company'] = get_all_kinds_of_company(title)
    # 保存数据
    print_dict(data)
    if to_save:
        save_huanping(data, province_id, city_id, kind, task_id)


def data_is_table(html, xpath=""):
    '''
    数据是否以表格存储
    :param html:
    :param xpath:
    :return:
    '''
    # 横向的表格
    xpath = xpath + "//table/tbody/tr[count(child::node()) > 4]"
    # 纵向的表格
    xpath_v = xpath + '//table/tbody[count(child::tr)>8]/tr'
    return True if html.xpath(xpath) or html.xpath(xpath_v) else False


def data_is_aline(html, xpath='', min_num=8):
    '''
    是否为aline类型
    :param html: ~
    :param xpath: 定位到主要内容
    :return:
    '''
    if html.xpath("{1}[count(child::p)>{0}]/p|{1}//*[count(child::p)>{0}]/p".format(min_num, xpath)) or html.xpath(
            "{1}[count(child::div)>{0}]/div|{1}//*[count(child::div)>{0}]/div".format(min_num, xpath)):
        return True
    else:
        return False


def data_is_para(html, xpath='', min_num=8):
    '''
    判断是否为para类型
    :param html: ~
    :param xpath: 定位到主要内容
    :return:
    '''
    # 获取段落文字
    para = parse_div(html,
                     des='{1}[count(child::div)>{0}]/div|{1}//*[count(child::div)>{0}]/div'.format(min_num, xpath))
    # 为大标题设置好flag
    count = 0
    for i in para:
        if re.search('^[一二三四五六七八九]', i):
            count += 1
            print(i)
    return True if count > 3 else False


def get_all_attachment(html, xpath, url):
    '''
    获取所有附件的连接
    :param html: ~
    :param xpath: 主要内容的xpath
    :return: attachment的字符串
    '''
    href = html.xpath(xpath + "//@href")
    href = [urljoin(url, i) for i in href if not re.search('mailto|javascript', i)]
    href = ','.join(href)
    return href

# -*- coding:utf-8 -*-
'''
__author__ = 'XD'
__mtime__ = 2019/1/21
__project__ = cube
Fix the Problem, Not the Blame.
'''
# 解析网页
from lxml import etree
import time
import re
from urllib.parse import urljoin

# 计数
global count
count = 0
# 城市数据
# TODO: 城市数据以及url
province_id = 11  # 省份
city_id = 92  # 市
enconding = 'utf-8'  # 网页编码
# 更新的时间判断
latest_time = get_latest_time(province_id, city_id)
if latest_time:
    latest_time = time.mktime(latest_time.timetuple())  # 获取最大时间
else:
    latest_time = 0


# @exception_log('每个类别')
def parse_kind(kind):
    '''
    收集数据统一入口
    - 获取类型
    - 获取页数
    - 循环页数获取数据
    '''
    # 获取page类型
    page_type = kind[1]
    # 获取数据
    url_page = kind[0]
    page = int(get_page(url_page))
    for i in range(1, page + 1):
        # TODO: 设置url
        url = 'http://hbj.huzhou.gov.cn/hzgov/openapi/info/ajaxpagelist.do?pagesize=15&channelid=26535&pageno=' + str(i) # 生产确定的url, 第一页特殊处理
        # print(url)
        html = request_text(url)  # 可以被xpath解析的对象
        # TODO: 解析xpath
        href = re.findall('"url":"(.*?)"', html)  # url列表
        title = re.findall('"title":"(.*?)"', html)  # 标题列表
        time_c = re.findall('"daytime":"(.*?)"', html)  # 发布日期
        if (len(href) == len(title)) and (len(href) == len(time_c)):
            for j in range(len(href)):
                global count
                count += 1
                # 判断类型
                page_type = 0
                if re.search('竣工', title[j]):
                    continue
                if re.search('拟.*?审批', title[j]):
                    page_type = 2
                elif re.search('.*?审批', title[j]):
                    page_type = 3
                elif re.search('受理', title[j]):
                    page_type = 1
                print(count, page_type, time_c[j], href[j], end='\t')
                # 创建任务的时候判断一下是否存在
                if judge_task(href[j], province_id, city_id) and time_bigger(latest_time, time.mktime(
                        time.strptime(time_c[j], '%Y-%m-%d'))):
                    # TODO: 入库
                    print("获取成功", title[j])
                    create_task(href[j], title[j].strip(), province_id, city_id, time_c[j], category_id=page_type)
                else:
                    if not time_bigger(latest_time, time.mktime(time.strptime(time_c[j], '%Y-%m-%d'))):
                        print('不符合更新要求，停止！')
                    else:
                        print('已经存在，停止！')
                    return

        else:
            print("title:{}, url:{}, time:{}".format(len(title), len(href), len(time_c)))
            raise RuntimeError('标题，url，时间数目不匹配')


def get_page(url):
    '''
    获取界面页数
    :param url: 三大类url
    :return: int
    '''
    #  TODO: 获取页码
    # 初始化url,获取response
    res = request_text(url)
    res = re.search('"pageTotal":(\d+)', res)
    res = res.group(1)
    return res


def run():
    # 记录运行时间
    start = time.time()

    kinds = [
        # 三类网址的第一个界面的url
        ['http://hbj.huzhou.gov.cn/xzspxkgs/ffhpsp/sj/index.html', None],  # 受理
        # ['', 2],  # 拟批
        # ['', 3]  # 已批
    ]
    for i in kinds:
        # 依次获取数据
        parse_kind(i)
    run_time = time.time() - start
    print('运行时间：%ss' % run_time)


# -*- coding:utf-8 -*-
'''
__author__ = 'XD'
__mtime__ = 2019/1/21
__project__ = cube
Fix the Problem, Not the Blame.
'''
# -*- coding:utf-8 -*-
'''
__author__ = 'XD'
__mtime__ = 2019/1/21
__project__ = cube
Fix the Problem, Not the Blame.
'''
'''
流程：
1. parse_task: 获取task，并将数据传给get_data
2. get_data: 解析页面数据，把数据存入data字典，然后传入save_huanping(data, province_id, city_id, type_id)保存数据
'''
from urllib.parse import urljoin
from pprint import pprint
import re

# 计数
count = 0
# 城市信息
city_id = 92
province_id = 11


def parse_task_1():
    # 获取本市未爬取的url
    tasks = get_tasks_with_cat(province_id, city_id, 1)
    for task in tasks:
        # 实时播报
        global count
        count += 1
        # 时间
        p_time = task[4]
        p_time = p_time.isoformat()[:10]
        print(count, p_time, task[0], task[1], task[2])
        # 处理数据
        title = task[2]
        url = task[1]
        task_id = task[0]
        get_data_1(url, title, p_time, task_id)
    print("======city_id:{}, type_id{}======".format(city_id, 1))
    print("======完成======")


@exception_log(('city_id: ' + str(city_id) + 'type_id:' + str(1)))
def get_data_1(url, title, public_time, task_id):
    html = html_xpath(url)
    # 主要内容的xpath
    main_xpath = "//td[@class='content']"
    is_save = False
    if data_is_table(html, main_xpath):
        save_table_to_huanping(province_id, city_id, 1, task_id, url, title, public_time, html, xpath=main_xpath,
                                     to_save=is_save)
    elif data_is_para(html, main_xpath):
        save_para_to_huanping(province_id, city_id, 1, task_id, url, title, public_time, html, xpath=main_xpath,
                                    min_num=8, to_save=is_save)
    elif data_is_aline(html, main_xpath):
        save_aline_to_huanping(province_id, city_id, 1, task_id, url, title, public_time, html, xpath=main_xpath,
                                     min_num=8, to_save=is_save)
    else:
        save_title_to_huanping(province_id, city_id, 1, task_id, url, title, public_time, html, xpath=main_xpath,
                                     to_save=is_save)


def parse_task_2():
    # 获取本市未爬取的url
    tasks = get_tasks_with_cat(province_id, city_id, 2)
    for task in tasks:
        # 实时播报
        global count
        count += 1
        # 时间
        p_time = task[4]
        p_time = p_time.isoformat()[:10]
        print(count, p_time, task[0], task[1], task[2])
        # 处理数据
        title = task[2]
        url = task[1]
        task_id = task[0]
        get_data_2(url, title, p_time, task_id)
    print("======city_id:{}, type_id{}======".format(city_id, 2))
    print("======完成======")


@exception_log(('city_id: ' + str(city_id) + 'type_id:' + str(2)))
def get_data_2(url, title, public_time, task_id):
    html = html_xpath(url)
    # 主要内容的xpath
    main_xpath = "//td[@class='content']"
    is_save = False
    if data_is_table(html, main_xpath):
        save_table_to_huanping(province_id, city_id, 2, task_id, url, title, public_time, html, xpath=main_xpath,
                                     to_save=is_save)
    elif data_is_para(html, main_xpath):
        save_para_to_huanping(province_id, city_id, 2, task_id, url, title, public_time, html, xpath=main_xpath,
                                    min_num=8, to_save=is_save)
    elif data_is_aline(html, main_xpath):
        save_aline_to_huanping(province_id, city_id, 2, task_id, url, title, public_time, html, xpath=main_xpath,
                                     min_num=8, to_save=is_save)
    else:
        save_title_to_huanping(province_id, city_id, 2, task_id, url, title, public_time, html, xpath=main_xpath,
                                     to_save=is_save)


def parse_task_3():
    # 获取本市未爬取的url
    tasks = get_tasks_with_cat(province_id, city_id, 3)
    for task in tasks:
        # 实时播报
        global count
        count += 1
        # 时间
        p_time = task[4]
        p_time = p_time.isoformat()[:10]
        print(count, p_time, task[0], task[1], task[2])
        # 处理数据
        title = task[2]
        url = task[1]
        task_id = task[0]
        get_data_3(url, title, p_time, task_id)
    print("======city_id:{}, type_id{}======".format(city_id, 3))
    print("======完成======")


@exception_log(('city_id: ' + str(city_id) + 'type_id:' + str(3)))
def get_data_3(url, title, public_time, task_id):
    html = html_xpath(url)
    # 主要内容的xpath
    main_xpath = "//td[@class='content']"
    is_save = True
    if data_is_table(html, main_xpath):
        save_table_to_huanping(province_id, city_id, 3, task_id, url, title, public_time, html, xpath=main_xpath,
                                     to_save=is_save)
    elif data_is_para(html, main_xpath):
        save_para_to_huanping(province_id, city_id, 3, task_id, url, title, public_time, html, xpath=main_xpath,
                                    min_num=8, to_save=is_save)
    elif data_is_aline(html, main_xpath):
        save_aline_to_huanping(province_id, city_id, 3, task_id, url, title, public_time, html, xpath=main_xpath,
                                     min_num=8, to_save=is_save)
    else:
        save_title_to_huanping(province_id, city_id, 3, task_id, url, title, public_time, html, xpath=main_xpath,
                                     to_save=is_save)


if __name__ == '__main__':
	run()
	parse_task_1()
	parse_task_2()
	parse_task_3()

	update_city_task(city_id)