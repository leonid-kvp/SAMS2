#!/usr/bin/python3 -u

#   Строка #!/usr/bin/python2.5 -u отключает буферизацию вывода в stdout, без этого необходимо после каждого вывода писать дополнительно sys.stdout.flush() иначе работать ничего не будет.
#   Переодически на stdin валятся пустые строки, поэтому без обработки ввода программа будет постоянно вылетать, сквид постоянно делать рестарт и как результат не работать.
#   Всё, записанное в stderr будет отображено в cache.log сквида, очень удобно во время отладки.

# external_acl_type SAMS2ACL %SRC %LOGIN %PROTO %DST /root/SAMS_PY/sams2acl.py

import os
import sys
from pprint import pprint
import datetime
import argparse
import crypt
import time

import sams2libs

def log(UUID, s):
    sys.stderr.write("PID={0} LOG UUID=({1}) = {2}".format(os.getpid(), UUID, s))


def main(arg):

    sams2libs.Read_SAMS_cfg_file()
    sams2libs.Read_SAMS_cfg_DB()

    parser = createParser()
    namespace = parser.parse_args (arg)

    #pprint ( namespace.type == 'ip' )

    cursor = sams2libs.conn.cursor(dictionary=True)
    #pprint( cursor)

    # установим начальные значения переменных
    start_t = time.perf_counter()
    UUID = ""
    instr = ""

    while 1:
        # засечем окончание выполнения цикла, значение будет актуально на начало 2-го цикла
        stop_t = time.perf_counter() 
        if instr != "" and instr != "\n":
            log(UUID, "Время выполнения цикла {0:0.4f} секунд \n\n\n".format( stop_t - start_t ) )

        instr = sys.stdin.readline()

        # засечем начало
        start_t = time.perf_counter()

        UUID=sams2libs.randStr()

        if len(instr) < 3:
            continue

        try:
            sams2libs.conn.commit()
        except:
            a=0

        l = instr.split()
        log(UUID, "type = %s, input data = %s" % (namespace.type, instr))
#       для отладки
#        l='192.168.99.2 kasperskiy http ya.ru'.split()
#        l='192.168.99.9 - http miass.ru'.split()
#           192.168.99.9 - https vk.com
#        l='192.168.99.2 kasperskiy http sex.ru'.split()
#        l='1.1.1.1 - http sex.ru'.split()
#        l='1.1.1.1 - http miass.ru'.split()
#        l='192.168.133.101 - http ya.ru'.split()

        try:
            # при авторизации по IP
            if namespace.type == 'ip' or namespace.type == 'ip_url':
                ip = l[0]
                proto = l[1]
                url = l[2]
                login = '-'
            # при авторизации по ЛОГИНУ
            elif  namespace.type == 'login':
                ip = l[0]
                login = l[1]
                proto = l[2]
                url = l[3]
            # а это сама авторизация по логину и паролю  type=auth
            elif namespace.type == 'auth':
                login = l[0]
                passw = l[1]
#            else:
            elif namespace.type == 'rewrite':
#dc1.ksn.kaspersky-labs.com:443 192.168.99.15/SERVER-SHTRIH-N.zaomm.loc - CONNECT myip=192.168.99.1 myport=3128
                ip = l[1].split("/")[0]
                login = l[2]
                url = l[0].split(":")[0]
                proto = "80"
                if len(l[0].split(":")) > 1 : proto = l[0].split(":")[1]

        except:
            continue




        if namespace.type == 'auth':
            # если параметр AUTH то передаются только логин и пароль без урла, поэтому только проверяем наличие юзера
            # получим дынные пользователя
            user_data = sams2libs.Seek_User( '-', login, False )
            if user_data != None:
                if login == user_data['s_nick'] and crypt.crypt(passw, passw[:2]) == user_data['s_passwd']:
                    sys.stdout.write('OK\n')
                    log(UUID, "User data is found\n")
#                    permit = 1
                else:
                    sys.stdout.write('ERR Wrong password\n')
                    log(UUID, 'ERR Wrong password\n')
#                    continue
            else:
                sys.stdout.write('ERR Wrong user\n')
                log(UUID, 'ERR Wrong user\n')

            continue

        elif  namespace.type == 'ip':
            # получим дынные пользователя по IP
            user_data = sams2libs.Seek_User( ip,login )
            if user_data != None:
                sys.stdout.write('OK\n')
                log(UUID, "User data is found {0}\n".format( user_data ))
            else:
                sys.stdout.write('ERR ip not found\n')
                log(UUID, 'ERR Wrong user\n')

            continue

        elif  namespace.type == 'ip_url':
            # получим дынные пользователя по IP
            user_data = sams2libs.Seek_User( ip,login )
            if user_data is None:
                sys.stdout.write('OK\n')
                log(UUID, 'OK ip_url, ip address not found\n')
                continue
            else:
                log(UUID, "User(IP) data is found {0}\n".format( user_data ))

        elif  namespace.type == 'login':
            # получим дынные пользователя по логину
            user_data = sams2libs.Seek_User( "-",login )

            if user_data is None:
#                sys.stdout.write('OK\n')
#                log(UUID, "User data is found {0}\n".format( user_data ))
#            else:
                sys.stdout.write('ERR Wrong user\n')
                log(UUID, 'ERR Wrong user\n')
                continue

#        namespace.type == 'rewrite':
# type = rewrite
#dc1.ksn.kaspersky-labs.com:443 192.168.99.15/SERVER-SHTRIH-N.zaomm.loc - CONNECT myip=192.168.99.1 myport=3128
#vk.com 192.168.99.15/SERVER-SHTRIH-N.zaomm.loc - CONNECT myip=192.168.99.1 myport=3128
#dc1.ksn.kaspersky-labs.com:443 192.168.99.206/192.168.99.206 - CONNECT myip=192.168.99.1 myport=3128
        else:
            if login != "-":
                user_data = sams2libs.Seek_User( "-", login )
            else:
                user_data = sams2libs.Seek_User( ip, login )

            if user_data is None:
                sys.stdout.write('ERR\n')
                log(UUID, 'ERR Wrong user or ip\n')
                continue



        # получим параметры шаблона
        sql = 'SELECT * FROM shablon WHERE s_shablon_id=' + str( user_data['s_shablon_id']  )
        cursor.execute(sql)
        shablon_data = cursor.fetchone()

        # проверяем включен ли пользователь
        if user_data['s_enabled'] != 1:
            permit = 0
            log(UUID, '%s %s -> %s: %s , shablon = %s, type = %s\n' % (ip, login, url, 'USER OFF', shablon_data['s_name'], shablon_data['s_alldenied']))
            sys.stdout.write('ERR message="user disabled"\n')
            log(UUID, 'ERR  message="user disabled"\n')
            continue

        # если у пользователя есть квота и он ее превысил, по запрещаем ему работать
        #pprint ( user_data['s_quote'] > 0 )
        if user_data['s_quote'] > 0:
            if user_data['s_size'] > ( user_data['s_quote'] * 1024 *1024 ):
                permit = 0
                log(UUID, '%s %s -> %s: %s , shablon = %s, type = %s\n' % (ip, login, url, 'QUOTE LIMITED (' + str(user_data['s_size']) + ')', shablon_data['s_name'], shablon_data['s_alldenied']))
                sys.stdout.write('ERR message="quote expired"\n')
                log(UUID, 'ERR message="quote expired"\n')
                continue

        # если шаблон по дефолту все блокирует, то ищем разрешенный урл
        if shablon_data['s_alldenied'] == 1:
            # определим разрешен ли УРЛ
            sql='SELECT url.s_url,"' + url + '"  REGEXP url.s_url AS FIND FROM shablon,sconfig,redirect,url' \
                ' WHERE shablon.s_shablon_id=' + str( user_data['s_shablon_id'] ) + \
                ' AND shablon.s_shablon_id=sconfig.s_shablon_id' \
                ' AND (sconfig.s_redirect_id=redirect.s_redirect_id  OR redirect.s_type="local")' \
                ' AND  url.s_redirect_id=redirect.s_redirect_id' \
                ' AND (redirect.s_type="allow" OR redirect.s_type="local")' \
                ' HAVING FIND=1 LIMIT 1;'

            #print (sql)

            cursor.execute(sql)
            res = cursor.fetchone()

            #pprint ( res )

            if res is None:
                permit = 0
            else:
                permit = 1
        else:
            sql = 'SELECT url.s_url,"^' + url + '"  REGEXP url.s_url AS FIND FROM shablon,sconfig,redirect,url' \
                  ' WHERE shablon.s_shablon_id=' + str( user_data['s_shablon_id'] )  + \
                  ' AND shablon.s_shablon_id=sconfig.s_shablon_id' \
                  ' AND sconfig.s_redirect_id=redirect.s_redirect_id' \
                  ' AND  url.s_redirect_id=redirect.s_redirect_id' \
                  ' AND redirect.s_type="denied" HAVING FIND=1 LIMIT 1;'

            cursor.execute(sql)
            res = cursor.fetchone()
#            log(UUID, "{0}\n".format(sql) )
#            log(UUID, "{0}\n".format(res) )

            if res is None:
                permit = 1
            else:
                permit = 0


        log(UUID, '%s %s -> %s: %s , shablon = %s, type = %s\n' % (ip, login, url, permit, shablon_data['s_name'], shablon_data['s_alldenied']))
        if permit:
            sys.stdout.write('OK\n')
            log(UUID, 'OK\n')
        else:
            sys.stdout.write('ERR message="url blocked"\n')
#            sys.stdout.write('status=30N url="http://192.168.99.1"\n')
            log(UUID, 'ERR message="url blocked"\n')

#    cursor.close()

def createParser ():
    parser = argparse.ArgumentParser()
#    parser.add_argument ('type', nargs='?', default='rewrite')
    parser.add_argument ('type', nargs='?', default='ip_url')
 
    return parser

if __name__ == '__main__':
    sys.exit(main(arg=sys.argv[1:]))
#    main(1)