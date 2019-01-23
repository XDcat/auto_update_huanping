import os  # 运行程序
# 获取py文件的绝对目录
import sys
from atexit import register
from threading import Thread
from time import ctime

dir_path = os.path.abspath('./citys/ext/zlj')
sys.path.append(dir_path)

# py文件绝对路径
citys = os.listdir('./citys')  # 找到文件目录
citys = list(map(lambda x: os.path.join(os.getcwd(), 'citys', x), citys))
citys = list(filter(os.path.isfile, citys))  # 过滤目录
# 运行文件
# 完全模拟正式环境，必须要os.chdir，否则会出现各种未知问题
os.chdir(os.path.join(os.getcwd(), 'citys'))


def run_py(path):
    print(path)
    os.system('py ' + path)


@register
def _atexit():
    print('End at:', ctime())


if __name__ == '__main__':
    print('Start at:', ctime())
    # # 创建线程
    # threads = [Thread(target=run_py, args=(i,)) for i in citys]
    # # 开始线程
    # [i.start() for i in threads]
    # # [print(i) for i in citys]
    [run_py(i) for i in citys]