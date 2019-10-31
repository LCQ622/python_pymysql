# encoding=utf-8
# 基于pymysql的数据库访问层
# Date： 2019-10-31
# Version： 1.1
# 新增功能：
#       1.新增更新数据方法upadte_data()
#       2.新增判断如果是 int 、complex 、float 类型的数据再拼接SQL语句时将不会添加引号
# 已知bug:
#       1.未测试异常情况
#       2.未做异常处理
import os
import pymysql
import configparser


def config():
    '''
    该方法用于操作配置文件
    :return:
    '''
    if os.path.isfile("db.ini"):
        cf=configparser.ConfigParser()
        cf.read("db.ini")
        if cf.has_option('mysql', 'check') :
            print("请在当前目录下db.ini文件进行配置！！！")
        else:
            data=dict()
            for key in cf.options("mysql"):
                value=""
                if key=="port":
                   value= cf.getint("mysql",key)
                else:
                    value=cf.get("mysql",key)
                data.update({key:value})
            return data
    else:
        cf = configparser.ConfigParser()
        cf.add_section("mysql")
        cf.set("mysql", "host", "数据库地址,一般为'localhost'或者'127.0.0.1'")
        cf.set("mysql", "port", "端口，一般为'3306'")
        cf.set("mysql", "user", "用户名")
        cf.set("mysql", "password", "密码")
        cf.set("mysql", "db", "要连接的库")
        cf.set("mysql", "table", "要进行配置的表")
        cf.set("mysql", "charset", "utf8")

        cf.set("mysql", "check", "配置完成请务必删除我这一行！")
        with open("db.ini", "w+") as f:
            cf.write(f)
        print("请先对当前目录下db.ini文件进行配置")
        return False

cf = config()

def get_connect():
    '''
    该方法用于获取数据库连接
    :return:
    '''
    conn =pymysql.connect(host=cf['host'],port=cf['port'],user=cf['user'],password=cf['password'],charset=cf['charset'],db=cf['db'])
    return conn

def get_data(**kwargs):
    '''
    该方法用于获取数据
    传参 get_data(id=2,name="abc")
    :param kwargs:
    :return: 返回查询到的数据
    '''
    conn=get_connect()
    cursor=conn.cursor()
    sql = f"select * from {cf['table']} where 1=1"
    for i in kwargs:
        if type(kwargs[i])==int or type(kwargs[i])==complex or type(kwargs[i])==float:
            sql=sql+f" and {i}={kwargs[i]}"
        else :
            sql = sql + f" and {i}='{kwargs[i]}'"
    cursor.execute(sql)
    data=cursor.fetchall()
    conn.close()
    return data

def add_data(*args,**kwargs):
    '''
    该方法用于添加数据
    :param kwargs:  传参：add_data(id=1,name="abc")
    :return:
    '''

    if args!=():
        print("参数传递有误！！！")
        return False
    else:
        if kwargs!={}:
            conn = get_connect()
            cursor = conn.cursor()
            sql = f"INSERT INTO {cf['table']} ("
            for i in kwargs:
                sql = sql + f"{i},"
            # 删除最后一个字符
            sql = sql[:-1]+") VALUES ("
            for j in kwargs:
                if type(kwargs[j]) == int or type(kwargs[j])==complex or type(kwargs[j])== float:
                    sql=sql+f"{kwargs[j]},"
                else:
                    sql = sql + f"'{kwargs[j]}',"
            sql = sql[:-1] + ")"
            cursor.execute(sql)
            row=cursor.rowcount
            conn.commit()
            conn.close()
            return True if row>0 else False
        else:
            print("插入的数据不能为空！！")
            return False


def delete_data(*args,**kwargs):
    '''
    删除指定条件的数据
    :param args: 不能传参
    :param kwargs:  delete_data(id=1,name="abc")
    :return:
    '''
    if args != ():
        print("参数传递有误！！！")
        return False
    else:
        if kwargs != {}:
            conn = get_connect()
            cursor = conn.cursor()
            sql = f"DELETE FROM {cf['table']} WHERE 1=1 "
            for i in kwargs:
                if type(kwargs[i])==int or type(kwargs[i])==complex or type(kwargs[i])==float:
                    sql = sql + f" AND {i}={kwargs[i]}"
                else:
                    sql=sql+f" AND {i}='{kwargs[i]}'"
            cursor.execute(sql)
            row = cursor.rowcount
            conn.commit()
            conn.close()
            return True if row > 0 else False
        else:
            print("条件不能为空！！！")
            return False



def clear_table_data():
    '''
    清空表的内容，注意这将是毁灭性的，请谨慎操作
    :return:
    '''
    conn = get_connect()
    cursor = conn.cursor()
    sql = f" TRUNCATE TABLE {cf['table']} "
    cursor.execute(sql)
    conn.commit()
    conn.close()





def upadte_data(old_data,new_data):
    '''
    该方法用于更新数据操作
    :param old_data:  传参请参考upadte_data(dict(id=1),dict(name="傻强",job="打工仔",salary=2500))
    :param new_data:
    :return: 成功返回True
    '''
    if type(old_data)==dict and type(new_data)==dict:
        if old_data!={} and new_data!={}:
            sql=f"UPDATE {cf['table']} SET "
            for i in new_data:
                # 如果是 int 、complex 、float 类型的数据将不会添加引号
                if type(new_data[i])==int or  type(new_data[i])==complex or  type(new_data[i])==float:
                    sql=sql+f"{i}={new_data[i]},"
                else:
                    sql = sql + f"{i}='{new_data[i]}',"
            sql=sql[:-1]+" WHERE 1=1"
            for j in old_data:
                #如果是 int 、complex 、float 类型的数据将不会添加引号
                if type(old_data[j])==int or type(old_data[j])==complex or type(old_data[j])==float:
                    sql=sql+f" AND {j}={old_data[j]}"
                else:
                    sql = sql + f" AND {j}='{old_data[j]}'"
            conn = get_connect()
            cursor = conn.cursor()
            cursor.execute(sql)
            row=cursor.rowcount
            conn.commit()
            conn.close()
            return  True if row>0 else False
        else:
            print("传入的字典不能为空")
    else:
        print("数据类型不正确，请传入字典类型的数据！")

