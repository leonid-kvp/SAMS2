#!/usr/bin/python3
import os
import sys
from pprint import pprint
import datetime

import sams2libs
#from sams2libs import conn
#from sams2libs import CFG_FILE_PARAMS, CFG_SQL_PARAMS


def Parse_Log(offset):
    try:
       f = open(sams2libs.CFG_FILE_PARAMS['squidlogdir']+'/'+sams2libs.CFG_FILE_PARAMS['squidcachefile'], 'r',encoding='cp866')
    except:
        print ( 'Error open log file for psrsing : ' + sams2libs.CFG_FILE_PARAMS['squidlogdir']+'/'+sams2libs.CFG_FILE_PARAMS['squidcachefile'] )

    try:
        f.seek(offset)
    except:
        f.seek(0)

    for line in f.readlines():

        try:
            dt = str( datetime.datetime.fromtimestamp( float( line.split( )[0][0:10] ) ) )
        except:
            dt = u'Error'

        try:
            ip = line.split( )[2]
        except:
            ip = u'Error'
    
        try:
            bytes = line.split( )[4]
        except:
            bytes = u'Error'
    
        try:
            result = line.split( )[3].split('/')[0]
            if result == 'NONE':
                dt = u'Error'
                query = line.split( )[6]
                method = u'NONE'
                login =  u'NONE'

#                query = line.split( )[3]
#                method = line.split( )[5]
#                login = line.split( )[7]
            else:
                query = line.split( )[6]
                method = line.split( )[5]
                login =  line.split( )[7]
        except:
            result = u'Error'
            query = line.split( )[3]
            login = u'Error'
            method = u'Error'


        if dt != 'Error' and ip != 'Error' and bytes != 'Error' and login != 'Error':
            print ( dt + ' - ' + ip + ' - ' + bytes + ' - ' + query + ' - ' + login )
            user_data = sams2libs.Seek_User( ip,login )

#            pprint(user_data)

            cursor = sams2libs.conn.cursor(dictionary=True)
            # запишем строку лока в базу для пользователя
            sql = 'INSERT INTO squidcache (s_cache_id,s_proxy_id,s_date_time,s_date,s_time,s_user_id,s_user,s_domain,s_size,s_hit,s_ipaddr,s_period,s_method,s_url,s_result)' \
                  ' VALUES (NULL,1,%s,%s,%s,%s,%s,"",%s,0,%s,0,%s,%s,%s)'
            args = (dt,dt.split( )[0], dt.split( )[1], user_data['s_user_id'],user_data['s_nick'], bytes, ip, method, query, result)
            cursor.execute(sql,args)

            # обновим счетчик статистики у пользователя
            sql = 'UPDATE squiduser SET s_size=s_size+%s WHERE s_user_id=%s'
            args = (bytes, user_data['s_user_id'])
            cursor.execute(sql,args)

            # запомним смещение в файле и последнюю считаную строку
            sql = 'INSERT INTO proxy (s_proxy_id,s_mb_last_num,s_mb_last_str) VALUES (1,%s,%s) ON DUPLICATE KEY UPDATE s_mb_last_num=%s,s_mb_last_str=%s'
            args = (offset,line[:255], offset,line[:255])
            cursor.execute(sql,args)
            sams2libs.conn.commit()
            #print( 'save ' + str(offset) )
        else:
            print ("error")
            print ( line )
            print ( result + ' = ' + dt + ' - ' + ip + ' - ' + bytes + ' - ' + query + ' - ' + login )


        # запоминаем смещение в файле перед прочтением следующей строки
        offset = offset + len(line)

    f.close()

# если по запомненному смещению находим запомненную строку, то начинаем со следующей строки иначе считаем, что это другой файл и начинаем сначала
def Check_Pos(offset, str_seek):

    try:
        f = open(sams2libs.CFG_FILE_PARAMS['squidlogdir']+'/'+sams2libs.CFG_FILE_PARAMS['squidcachefile'], 'r',encoding='cp866')
    except:
        print ( 'Error open log file for check pos : ' + sams2libs.CFG_FILE_PARAMS['squidlogdir']+'/'+sams2libs.CFG_FILE_PARAMS['squidcachefile'] )

    # перейдем в конец файла и сравним запомненное смещение, если они равны значит это конец файла
    f.seek(0,2)
    if offset == f.tell():
        f.close()
        return offset
    # если размерфайла меньше чем сохраненное смещение, то это тоже новый файл
    if offset > f.tell():
        f.close()
        return 0

    # идем по сохраненному смещению, считываем строку и сравниваем
    f.seek(offset)
    line = f.readline()
    print ("saved " + str(offset) + ' = ' + str_seek)
    print ("read " + str(f.tell()) + ' = ' + line)
    if line[:255] != str_seek[:255]:
#        print("NEW")
        f.close()
        return 0

    # если ни одно из верхних условий не выполнилось, значит продолжаем читать файл
#    print("RESUME")
    offset = f.tell()
    f.close()
    return offset



#pprint(sams2libs.CFG_FILE_PARAMS)
#pprint(sams2libs.sams2libs.CFG_SQL_PARAMS)


def main():
#    Read_SAMS_cfg_file()
#    Read_SAMS_cfg_DB()
    sams2libs.Read_SAMS_cfg_file()
    sams2libs.Read_SAMS_cfg_DB()

    offset = Check_Pos(sams2libs.CFG_SQL_PARAMS['s_mb_last_num'], sams2libs.CFG_SQL_PARAMS['s_mb_last_str'])

    try:
        if sys.argv[1] == 'clean':
            offset = 0
    except:
        a = 0

#    print( offset )
    Parse_Log(offset)
#    pprint(CFG_FILE_PARAMS)
#    pprint(CFG_SQL_PARAMS)

if __name__ == '__main__':
    main()

