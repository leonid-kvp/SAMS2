#!/usr/bin/python3
import os
from pprint import pprint
import configparser
import mysql.connector
from mysql.connector import Error
import datetime
import string
import random

# параметры из конфигурационного файла
global CFG_FILE_PARAMS
CFG_FILE_PARAMS = {}

global CFG_SQL_PARAMS
CFG_SQL_PARAMS=''

global conn
conn=''



def randStr(chars = string.ascii_uppercase + string.digits + string.ascii_lowercase, N=10): 
    return ''.join(random.choice(chars) for _ in range(N))

def Read_SAMS_cfg_file(filename='/etc/sams2.conf', section='default'):

    # прочитаем файл конфигурации SAMS2.conf  чтобы он правильно читался
    # в начале файла надо добавить [default]
    config = configparser.ConfigParser(interpolation=None)
    config.read(filename)


    if config.has_section(section):
        items = config.items(section)
        for item in items:
            CFG_FILE_PARAMS[item[0]] = item[1]
    else:
        raise Exception('{0} not found in the {1} file'.format(section, filename))

def Read_SAMS_cfg_DB():
    # Подключимся к базе с обработкой исключения
    try:
        global conn
        conn = mysql.connector.connect(host=CFG_FILE_PARAMS['db_server'],
                                       database=CFG_FILE_PARAMS['sams_db'],
                                       user=CFG_FILE_PARAMS['db_user'],
                                       password=CFG_FILE_PARAMS['db_password'],)

        if not conn.is_connected():
            raise('Connected to MySQL database')

    except Error as e:
        print(e)
        exit

    # група и шаблон  куда будут добавляться нейзвестные пользователи
    #DEFAULT_ADD_GROUP=`echo "select s_autogrp from proxy LIMIT 1 " | $MYSQL`
    #DEFAULT_ADD_TPL=`echo "select s_autotpl from proxy LIMIT 1" | $MYSQL`

    #dictionary=True чтобы был список с именами
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM proxy LIMIT 1")
    global CFG_SQL_PARAMS
    CFG_SQL_PARAMS = cursor.fetchone()

# ищем пользователя в базе, если нет то создаем
def Seek_User(ip, login ='-', create = False):
    seek_cursor = conn.cursor(dictionary=True)

    if login != '-':
#        seek_cursor.execute('SELECT s_user_id,s_nick,s_shablon_id FROM squiduser WHERE s_nick="' + login + '" LIMIT 1')
        seek_cursor.execute('SELECT * FROM squiduser WHERE s_nick="' + login + '" LIMIT 1')
    else:
#        seek_cursor.execute('SELECT s_user_id,s_nick,s_shablon_id FROM squiduser WHERE s_ip="' + ip + '" LIMIT 1')
        seek_cursor.execute('SELECT * FROM squiduser WHERE s_ip="' + ip + '" LIMIT 1')

    raw = seek_cursor.fetchone()
#    raw = seek_cursor.fetchall()
    if raw is None:
        if create == True:
            sql = 'INSERT INTO squiduser (s_user_id,s_group_id,s_shablon_id,s_nick,s_ip,s_enabled) VALUES (NULL,%s,%s,%s,%s,1)'
            args = (CFG_SQL_PARAMS['s_autogrp'],CFG_SQL_PARAMS['s_autotpl'],ip,ip)
            seek_cursor.execute(sql,args)
            conn.commit()
            return Seek_User(ip)
        else:
            return None

    return raw.copy()


def main():
    Read_SAMS_cfg_file()
    Read_SAMS_cfg_DB()
#    offset = Check_Pos(CFG_SQL_PARAMS['s_mb_last_num'], CFG_SQL_PARAMS['s_mb_last_str'])
#    print( offset )
#    Parse_Log(offset)
#    pprint(CFG_FILE_PARAMS)
#    pprint(CFG_SQL_PARAMS)

if __name__ == '__main__':
    main()


