# encoding=utf-8
# 基于pymysql的数据库访问层
# Date： 2019-11-17
# Version： 1.3
# 新增功能：
#       1.get_all_databases()       获取当前连接的所有库名
#       2.get_tables(database_name) 获取指定数据库的所有表名
#       3.get_columns(table_name)   该方法用于获取指定表中所有字段的名称
# 已知bug:
#       1.未测试异常情况
#       2.未做异常处理
#       3.未做数据库数据表灵活功能切换


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


def get_data(row=1000,**kwargs):
    '''
    该方法用于获取数据
    Version 1.2
    更新内容：
            1.新增限制符合条件返回的行数功能
            2.SQL语句拼接使用字符串格式化方法" ".format()兼容旧版本
    传参实例：
            result=get_data(row=2000,id=2,name="abc")
    :param row:  限制符合条件返回的行数，默认为1000行
    :param kwargs: 要查询的条件
    :return: 返回查询到的数据
    '''
    conn=get_connect()
    cursor=conn.cursor()
    sql = "SELECT * FROM {0} WHERE 1=1".format(cf['table'])
    for i in kwargs:
        if type(kwargs[i])==int or type(kwargs[i])==complex or type(kwargs[i])==float:
            sql=sql+" AND {0}={1}".format(i,kwargs[i])
        else :
            sql = sql + " AND {0}='{1}'".format(i,kwargs[i])
    sql = sql + " LIMIT 0,{0}".format(row)
    cursor.execute(sql)
    data=cursor.fetchall()
    conn.close()
    return data

def add_data(*args,**kwargs):
    '''
    该方法用于添加数据
    Version 1.2
    更新内容：
            1.SQL语句拼接使用字符串格式化方法" ".format()兼容旧版本
    :param kwargs:  传参：add_data(id=1,name="abc")
    :return: 添加成功返回True 反之则返回False
    '''

    if args!=():
        print("参数传递有误！！！")
        return False
    else:
        if kwargs!={}:
            conn = get_connect()
            cursor = conn.cursor()
            sql = "INSERT INTO {0} (".format(cf['table'])
            for i in kwargs:
                sql = sql + "{0},".format(i)
            # 删除最后一个字符
            sql = sql[:-1]+") VALUES ("
            for j in kwargs:
                if type(kwargs[j]) == int or type(kwargs[j])==complex or type(kwargs[j])== float:
                    sql=sql+"{0},".format(kwargs[j])
                else:
                    sql = sql + "'{0}',".format(kwargs[j])
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
    传参示例：delete_data(id=1,name="abc")
    Version 1.2
    更新内容：
            1.SQL语句拼接使用字符串格式化方法" ".format()兼容旧版本
    :param args: 不能传参
    :param kwargs:  要删除的条件
    :return: 删除成功返回True 反之则返回False
    '''
    if args != ():
        print("参数传递有误！！！")
        return False
    else:
        if kwargs != {}:
            conn = get_connect()
            cursor = conn.cursor()
            sql = "DELETE FROM {0} WHERE 1=1 ".format(cf['table'])
            for i in kwargs:
                if type(kwargs[i])==int or type(kwargs[i])==complex or type(kwargs[i])==float:
                    sql = sql + " AND {0}={1}".format(i,kwargs[i])
                else:
                    sql=sql+" AND {0}='{1}'".format(i,kwargs[i])
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
    清空表的内容
    注意这将是毁灭性的，请谨慎操作!!!
    Version 1.2
    更新内容：
            1.SQL语句拼接使用字符串格式化方法" ".format()兼容旧版本

    :return:
    '''
    conn = get_connect()
    cursor = conn.cursor()
    sql = " TRUNCATE TABLE {0} ".format(cf['table'])
    cursor.execute(sql)
    conn.commit()
    conn.close()





def upadte_data(old_data,new_data):
    '''
    该方法用于更新数据操作
    传参示例:
    upadte_data(dict(id=1),dict(name="傻强",job="打工仔",salary=2500))
    Version 1.2
    更新内容：
            1.SQL语句拼接使用字符串格式化方法" ".format()兼容旧版本
    :param old_data: 旧数据作为条件，以字典的方式传参
    :param new_data: 旧数据以字典的方式传参
    :return:  更新成功返回True 反之则返回False
    '''
    if type(old_data)==dict and type(new_data)==dict:
        if old_data!={} and new_data!={}:
            sql="UPDATE {0} SET ".format(cf['table'])
            for i in new_data:
                # 如果是 int 、complex 、float 类型的数据将不会添加引号
                if type(new_data[i])==int or  type(new_data[i])==complex or  type(new_data[i])==float:
                    sql=sql+"{0}={1},".format(i,new_data[i])
                else:
                    sql = sql + "{0}='{1}',".format(i,new_data[i])
            sql=sql[:-1]+" WHERE 1=1"
            for j in old_data:
                #如果是 int 、complex 、float 类型的数据将不会添加引号
                if type(old_data[j])==int or type(old_data[j])==complex or type(old_data[j])==float:
                    sql=sql+" AND {0}={1}".format(j,old_data[j])
                else:
                    sql = sql + " AND {0}='{1}'".format(j,old_data[j])
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



def get_all_databases():
    '''
    该方法用于获取该服务器所有库的名称
    Version 1.0
    :return:返回包含所有库名称的列表
    '''
    conn = get_connect()
    cursor = conn.cursor()
    sql="SHOW DATABASES ;"
    cursor.execute(sql)
    data =list()
    for i in  cursor.fetchall():
        data.append(i[0])
    conn.close()
    return data

def get_tables(database_name):
    '''
    该方法用于获取指定库中所有表的名称
    Version 1.0
    :param database_name: 指定库名称
    :return: 返回包含指定库中所有表名称的列表
    '''
    conn = get_connect()
    cursor = conn.cursor()
    sql1=" USE {0} ;".format(database_name)
    sql2="SHOW TABLES ;"
    cursor.execute(sql1)
    cursor.execute(sql2)
    data =list()
    for i in  cursor.fetchall():
        data.append(i[0])
    conn.close()
    return data

def get_columns(table_name):
    '''
    该方法用于获取指定表中所有字段的名称
    Version 1.0
    :param table_name: 表名称
    :return: 返回包含指定表中所有字段的列表
    '''
    conn = get_connect()
    cursor = conn.cursor()
    sql1="SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE table_name = '{0}';".format(table_name)
    cursor.execute(sql1)
    data =list()
    for i in  cursor.fetchall():
        data.append(i[0])
    conn.close()
    return data
