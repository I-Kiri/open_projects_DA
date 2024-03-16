'''
    ROADMAP:
-- Функции mart_fraud можно слить в одну функцию mart_fraud
-- Кодогенерация витрин с принимаемым скриптом-аргументом (выполняющим тело витрины)
-- Кодогенерация create table fact форматы
-- Оптимизация sql.запросов
-- Оптимизация py.кода
'''
import os
import psycopg2
import pandas

import sys
import datetime
sys.path.insert(0, 'D:/-- MFTI --/Block I (основы хранилищ БД)/Project/libs')
import myDBLib as my


# Настраиваем коннект для вывода
conn_out = psycopg2.connect( database = "xxxx" ,
                             host =     "xxxx" ,
                             user =     "xxxx" ,
                             password = "xxxx" ,
                             port =     "xxxx")
conn_out.autocommit = True
cursor_out = conn_out.cursor()

# Настраиваем коннект для ввода
conn_in = psycopg2.connect( database = "xxxx" ,
                         host =        "xxxx" ,
                         user =        "xxxx" ,
                         password =    "xxxx" ,
                         port =        "xxxx")

conn_in.autocommit = False
cursor_in = conn_in.cursor()




print(f'started at {str(datetime.datetime.now())[11:19]} :: {str(datetime.datetime.now())[:10]}')
print('processing...')


# Создание папки для отработанных файлов
path = r'D:\-- MFTI --\Block I (основы хранилищ БД)\Project\! к сдаче\archive'  # snippet taken from the Internet
if not os.path.exists(path):
    os.makedirs(path) #
    print('''archive folder created
            ''')


# Создание скриптов всех всех сущностей и загрузок кроме витрин  # (сначала текст скрипта генерировался функциями, затем индивидуально дополнялся вручную (дополнительные очистки stg/stg_del/stg_source в loads))
#factentity_script_generator( schema_name='demipt3', entity_name='passport_blacklist', target_fields_names_and_types=[ 'passport_num char(11)', 'entry_dt date' ] )
#factentity_script_generator( schema_name='demipt3', entity_name='transactions', target_fields_names_and_types=[ 'trans_id char(11)', 'trans_date timestamp(0)', 'card_num varchar(19)', 'oper_type varchar(10)', 'amt decimal(10,2)', 'oper_result varchar(10)', 'terminal char(5)' ] )
#my.scd2entity_script_generator( schema_name='demipt3', entity_name='terminals', target_fields_names_and_types=[ 'terminal_id char(5)', 'terminal_type char(3)', 'terminal_city varchar(20)', 'terminal_address varchar(60)' ] )
#my.scd2entity_script_generator( schema_name='demipt3', entity_name='clients', target_fields_names_and_types=[ 'clientd_id varchar(8)', 'last_name varchar(20)', 'first_name varchar(20)', 'patronymic varchar(20)', 'date_of_birth date', 'passport_num char(11)', 'passport_valid_to date', 'phone char(16)' ] )
#my.scd1entity_script_generator( schema_name='demipt3', entity_name='accounts', target_fields_names_and_types=[ 'account_num varchar(20)', 'valid_to date', 'client varchar(8)' ] )
#my.scd1entity_script_generator( schema_name='demipt3', entity_name='cards', target_fields_names_and_types=[ 'card_num char(19)', 'account_num varchar(20' ] )
#
#factload_script_generator( schema_name='demipt3', entity_name='passport_blacklist', fields=[ 'passport_num', 'entry_dt' ] )
#factload_script_generator( schema_name='demipt3', entity_name='transactions', fields=[ 'trans_id', 'trans_date', 'card_num', 'oper_type', 'amt', 'oper_result', 'terminal' ] )
#my.scd2load_script_generator( schema_name='demipt3', entity_name='terminals', fields=[ 'terminal_id', 'terminal_type', 'terminal_city', 'terminal_address' ] )
#my.scd2load_script_generator( schema_name='demipt3', entity_name='clients', fields=[ 'client_id', 'last_name', 'first_name', 'patronymic', 'date_of_birth', 'passport_num', 'passport_valid_to', 'phone', ] )
#my.scd1load_script_generator( schema_name='demipt3', entity_name='accounts', fields=[ 'account_num', 'valid_to', 'client' ] )
#my.scd1load_script_generator( schema_name='demipt3', entity_name='cards', fields=[ 'card_num', 'account_num' ] )


# Создание meta-таблицы для корректной заливки данных в витрину
try:
    cursor_in.execute( """ create table demipt3.kzmn_meta_rep_fraud (
                                schema_name varchar(20) ,
                                table_name varchar(60) ,
                                last_update_dt date ) ; """ )
    cursor_in.execute( """ insert into demipt3.kzmn_meta_rep_fraud
                                    ( schema_name, table_name, last_update_dt )
                                values
                                    ( 'demipt3','kzmn_dwh_fact_passport_blacklist', to_date('2021-03-01','YYYY-MM-DD') ) ; """ )
    cursor_in.execute( """ insert into demipt3.kzmn_meta_rep_fraud
                                    ( schema_name, table_name, last_update_dt )
                                values
                                    ( 'demipt3','kzmn_dwh_dim_terminals_hist', to_date('2021-03-01','YYYY-MM-DD') ) ; """ )
    cursor_in.execute( """ insert into demipt3.kzmn_meta_rep_fraud
                                    ( schema_name, table_name, last_update_dt )
                                values
                                    ( 'demipt3','kzmn_dwh_fact_transactions', to_date('2021-03-01','YYYY-MM-DD') ) ; """ )
    conn_in.commit()
except psycopg2.errors.DuplicateTable:
    conn_in.rollback()




# Создание сущностей БД
my.passport_blacklist_entity( conn=conn_in, cursor=cursor_in )
my.terminals_entity( conn=conn_in, cursor=cursor_in )
my.clients_entity( conn=conn_in, cursor=cursor_in )
my.accounts_entity( conn=conn_in, cursor=cursor_in )
my.cards_entity( conn=conn_in, cursor=cursor_in )
my.transactions_entity( conn=conn_in, cursor=cursor_in )
    
# Создание витрины БД
my.mart_tables( conn=conn_in, cursor=cursor_in )

# Фиксация транзакции (разовое создание хранилища)  # commits are made in the functions
#conn_in.commit()




#for i in range(13):  # использованный генератор словарика дат (сам словарик составлен в нотпаде)
#    if i >= 1 and i <= 9:
#        for j in range(1,32):
#            if j <= 9:
#                s = f'0{j}0{i}2021'
#                print(s)
#            elif j >= 10:
#                s = f'{j}0{i}2021'
#                print(s)
#    elif i >= 10 and i <= 12:
#        for j in range(1,32):
#            if j <= 9:
#                s = f'0{j}{i}2021'
#                print(s)
#            elif j >= 10:
#                s = f'{j}{i}2021'
#                print(s)

#dates = [ '01012021', '02012021', '03012021', '04012021', '05012021', '06012021', '07012021', '08012021', '09012021',
#          '10012021', '11012021', '12012021', '13012021', '14012021', '15012021', '16012021', '17012021', '18012021',
#          '19012021', '20012021', '21012021', '22012021', '23012021', '24012021', '25012021', '26012021', '27012021',
#          '28012021', '29012021', '30012021', '31012021', '01022021', '02022021', '03022021', '04022021', '05022021',
#          '06022021', '07022021', '08022021', '09022021', '10022021', '11022021', '12022021', '13022021', '14022021',
#          '15022021', '16022021', '17022021', '18022021', '19022021', '20022021', '21022021', '22022021', '23022021',
#          '24022021', '25022021', '26022021', '27022021', '28022021', '29022021', '30022021', '31022021', '01032021',
#          '02032021', '03032021', '04032021', '05032021', '06032021', '07032021', '08032021', '09032021', '10032021',
#          '11032021', '12032021', '13032021', '14032021', '15032021', '16032021', '17032021', '18032021', '19032021',
#          '20032021', '21032021', '22032021', '23032021', '24032021', '25032021', '26032021', '27032021', '28032021',
#          '29032021', '30032021', '31032021', '01042021', '02042021', '03042021', '04042021', '05042021', '06042021',
#          '07042021', '08042021', '09042021', '10042021', '11042021', '12042021', '13042021', '14042021', '15042021',
#          '16042021', '17042021', '18042021', '19042021', '20042021', '21042021', '22042021', '23042021', '24042021',
#          '25042021', '26042021', '27042021', '28042021', '29042021', '30042021', '31042021', '01052021', '02052021',
#          '03052021', '04052021', '05052021', '06052021', '07052021', '08052021', '09052021', '10052021', '11052021',
#          '12052021', '13052021', '14052021', '15052021', '16052021', '17052021', '18052021', '19052021', '20052021',
#          '21052021', '22052021', '23052021', '24052021', '25052021', '26052021', '27052021', '28052021', '29052021',
#          '30052021', '31052021', '01062021', '02062021', '03062021', '04062021', '05062021', '06062021', '07062021',
#          '08062021', '09062021', '10062021', '11062021', '12062021', '13062021', '14062021', '15062021', '16062021',
#          '17062021', '18062021', '19062021', '20062021', '21062021', '22062021', '23062021', '24062021', '25062021',
#          '26062021', '27062021', '28062021', '29062021', '30062021', '31062021', '01072021', '02072021', '03072021',
#          '04072021', '05072021', '06072021', '07072021', '08072021', '09072021', '10072021', '11072021', '12072021',
#          '13072021', '14072021', '15072021', '16072021', '17072021', '18072021', '19072021', '20072021', '21072021',
#          '22072021', '23072021', '24072021', '25072021', '26072021', '27072021', '28072021', '29072021', '30072021',
#          '31072021', '01082021', '02082021', '03082021', '04082021', '05082021', '06082021', '07082021', '08082021',
#          '09082021', '10082021', '11082021', '12082021', '13082021', '14082021', '15082021', '16082021', '17082021',
#          '18082021', '19082021', '20082021', '21082021', '22082021', '23082021', '24082021', '25082021', '26082021',
#          '27082021', '28082021', '29082021', '30082021', '31082021', '01092021', '02092021', '03092021', '04092021',
#          '05092021', '06092021', '07092021', '08092021', '09092021', '10092021', '11092021', '12092021', '13092021',
#          '14092021', '15092021', '16092021', '17092021', '18092021', '19092021', '20092021', '21092021', '22092021',
#          '23092021', '24092021', '25092021', '26092021', '27092021', '28092021', '29092021', '30092021', '31092021',
#          '01102021', '02102021', '03102021', '04102021', '05102021', '06102021', '07102021', '08102021', '09102021',
#          '10102021', '11102021', '12102021', '13102021', '14102021', '15102021', '16102021', '17102021', '18102021',
#          '19102021', '20102021', '21102021', '22102021', '23102021', '24102021', '25102021', '26102021', '27102021',
#          '28102021', '29102021', '30102021', '31102021', '01112021', '02112021', '03112021', '04112021', '05112021',
#          '06112021', '07112021', '08112021', '09112021', '10112021', '11112021', '12112021', '13112021', '14112021',
#          '15112021', '16112021', '17112021', '18112021', '19112021', '20112021', '21112021', '22112021', '23112021',
#          '24112021', '25112021', '26112021', '27112021', '28112021', '29112021', '30112021', '31112021', '01122021',
#          '02122021', '03122021', '04122021', '05122021', '06122021', '07122021', '08122021', '09122021', '10122021',
#          '11122021', '12122021', '13122021', '14122021', '15122021', '16122021', '17122021', '18122021', '19122021',
#          '20122021', '21122021', '22122021', '23122021', '24122021', '25122021', '26122021', '27122021', '28122021',
#          '29122021', '30122021', '31122021' ]
dates = [ '01032021', '02032021', '03032021', '04032021', '05032021', ]
flags = [ 'Y', 'Y', 'Y' ]

while len(dates) > 0:  # не учитывает ситуаций пропуска через день ( пр.: все за 1-ое число, два за 2-ое, все за 3-е число )
    # Обработка дат
    date = dates[0]

    part_day = f"""{f'{date}'[0:2]}"""
    part_month = f"""{f'{date}'[2:4]}"""
    part_year = f"""{f'{date}'[4:]}"""
    date_reversed = f'{part_year}{part_month}{part_day}'


    # Выгрузка сущности 'passport_blacklist'
    try:
        # Выгрузка данных
        df = pandas.read_excel(f'data_out_zip/passport_blacklist_{date}.xlsx', sheet_name='blacklist', header=0, index_col=None)
        cursor_in.executemany(""" insert into demipt3.kzmn_stg_passport_blacklist
                                        ( entry_dt, passport_num )
                                        values( %s, %s ) ; """, df.values.tolist())

        my.passport_blacklist_fact_load( conn=conn_in, cursor=cursor_in )  # commits are off from the function

        # Загрузка метаданных в таблицу витрины
        cursor_in.execute( f""" update demipt3.kzmn_meta_rep_fraud
                                set last_update_dt = cast('{date_reversed}' as date)
                                where schema_name = 'demipt3'
                                    and table_name = 'kzmn_dwh_fact_passport_blacklist' """ )

        # Фиксация транзакции  # commits are off from the function
        conn_in.commit()

        # Отработка файла
        #os.replace( f'data_out_zip/passport_blacklist_{date}.xlsx', f'! к сдаче/archive/passport_blacklist_{date}.xlsx.backup' )
        print( f'passport_blacklist_{date}.xlsx is renamed' )

        # Смена флага
        flags[0] = 'Y'

    except FileNotFoundError:
        print( f"""there's no passport_blacklist file for the date {date} -- kzmn_meta_rep_fraud won't be updated !""" )

        # Смена флага
        flags[0] = 'N'

    except PermissionError:
        print( f"""passport_blacklist_{date}.xlsx wasn't renamed -- 'PermissionError'""" )

        # Смена флага
        flags[0] = 'N'




    # Выгрузка сущности 'terminals'
    try:
        # Выгрузка данных
        df = pandas.read_excel( f'data_out_zip/terminals_{date}.xlsx', sheet_name='terminals', header=0, index_col=None )
        df['dt'] = date_reversed  # ! такая подстановка даты ресурсоемкая

        cursor_in.executemany(""" insert into demipt3.kzmn_stg_source_terminals
                                        ( terminal_id, terminal_type, terminal_city, terminal_address, dt )
                                        values( %s, %s, %s, %s, %s ) ; """, df.values.tolist() )

        my.terminals_scd2_load( conn=conn_in, cursor=cursor_in, date=date )   # commits are off from the function

        # Загрузка метаданных в таблицу витрины
        cursor_in.execute( f""" update demipt3.kzmn_meta_rep_fraud
                                set last_update_dt = cast('{date_reversed}' as date)
                                where schema_name = 'demipt3'
                                    and table_name = 'kzmn_dwh_dim_terminals_hist' """ )

        # Фиксация транзакции  # commits are off from the function
        conn_in.commit()

        # Отработка файла
        #os.replace(f'data_out_zip/terminals_{date}.xlsx', f'! к сдаче/archive/terminals_{date}.xlsx.backup')
        print(f'terminals_{date}.xlsx is renamed')

        # Смена флага
        flags[1] = 'Y'

    except FileNotFoundError:
        print( f"""there's no terminals file for the date {date} -- kzmn_meta_rep_fraud won't be updated !""" )

        # Смена флага
        flags[1] = 'N'

    except PermissionError:
        print(f"""terminals_{date}.xlsx wasn't renamed -- 'PermissionError'""")

        # Смена флага
        flags[1] = 'N'




    # Выгрузка сущности 'transactions'
    try:
        # Выгрузка данных
        df = pandas.read_csv( fr"data_out_zip\\transactions_{date}.txt", encoding='utf8', header=0, delimiter=';', decimal='.' )
        amount_column = df['amount'].str.replace(',', '.').astype(float)
        df['amount'] = amount_column
        df = df[['transaction_id', 'transaction_date', 'amount', 'card_num', 'oper_type', 'oper_result', 'terminal']]

        tuples = [tuple(x) for x in df.to_numpy()]  # snippet taken from the Internet
        values = [cursor_in.mogrify("( %s,%s,%s,%s,%s,%s,%s )", tuple).decode('utf8') for tuple in tuples]

        table = 'demipt3.kzmn_stg_transactions'
        columns = 'trans_id, trans_date, amt, card_num, oper_type, oper_result, terminal'

        query = "insert into %s(%s) values " % (table, columns) + ",".join(values)  #
        cursor_in.execute(query, tuples) ##

        my.transactions_fact_load( conn=conn_in, cursor=cursor_in )   # commits are off from the function

        # Загрузка метаданных в таблицу витрины
        cursor_in.execute( f""" update demipt3.kzmn_meta_rep_fraud
                                set last_update_dt = cast('{date_reversed}' as date)
                                where schema_name = 'demipt3'
                                    and table_name = 'kzmn_dwh_fact_transactions' """ )

        # Фиксация транзакции  # commits are off from the function
        conn_in.commit()

        # Отработка файла
        #os.replace(f'data_out_zip/transactions_{date}.txt', f'! к сдаче/archive/transactions_{date}.txt.backup')
        print(f'transactions_{date}.txt is renamed')

        # Смена флага
        flags[2] = 'Y'

    except FileNotFoundError:
        print( f"""there's no transactions file for the date {date} -- kzmn_meta_rep_fraud won't be updated ! """ )

        # Смена флага
        flags[2] = 'N'

    except PermissionError:
        print(f"""transactions_{date}.txt wasn't renamed -- 'PermissionError'""")

        # Смена флага
        flags[2] = 'N'




    # Распределение ветвей по чеку на файлы (при наличии хотя бы одного будет сделан апдейт данных из db.bank и загружена витрина)
    if (flags[0] == 'N' and flags[1] == 'N' and flags[2] == 'N') :

        # В этом условии не делается апдейт

        dates.remove(date)
        print(f"""
////////////////////////////////////////////////
a bunch from {date} wasn't loaded -- no files
///////////////////////////////////////////////
    """)
    elif (flags[0] == 'Y' and flags[1] == 'Y' and flags[2] == 'Y') :

        # Выгрузка данных сущности 'clients'
        clients = cursor_out.execute(""" select
                                            client_id ,
                                            last_name ,
                                            first_name ,
                                            patronymic ,
                                            date_of_birth ,
                                            passport_num ,
                                            passport_valid_to ,
                                            phone
                                        from info.clients """)
        titles = [x[0] for x in cursor_out.description]

        df = pandas.DataFrame(cursor_out.fetchall(), columns=titles)

        cursor_in.executemany(f""" insert into demipt3.kzmn_stg_source_clients
                                    ( client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, dt )
                                    values( %s, %s, %s, %s, %s, %s, %s, %s, '1901-01-01 00:00:00.000' )""", df.values.tolist())
        my.clients_scd2_load(conn=conn_in, cursor=cursor_in)  # commits are on in the function




        # Выгрузка данных сущности 'accounts'
        accounts = cursor_out.execute(""" select
                                            account ,
                                            valid_to ,
                                            client
                                        from info.accounts """)
        titles = [x[0] for x in cursor_out.description]

        df = pandas.DataFrame(cursor_out.fetchall(), columns=titles)

        cursor_in.executemany(f""" insert into demipt3.kzmn_stg_source_accounts
                                    ( account_num, valid_to, client, update_dt )
                                    values( %s, %s, %s, '1901-01-01 00:00:00.000' )""", df.values.tolist())
        my.accounts_scd1_load(conn=conn_in, cursor=cursor_in)  # commits are on in the function




        # Выгрузка данных сущности 'cards'
        cards = cursor_out.execute(""" select
                                            trim(card_num) ,
                                            account
                                        from info.cards """)
        titles = [x[0] for x in cursor_out.description]

        df = pandas.DataFrame(cursor_out.fetchall(), columns=titles)

        cursor_in.executemany(f""" insert into demipt3.kzmn_stg_source_cards
                                    ( card_num, account_num, update_dt )
                                    values( %s, %s, '1901-01-01 00:00:00.000' )""", df.values.tolist())
        my.cards_scd1_load(conn=conn_in, cursor=cursor_in)  # commits are on in the function




        # Выгрузка данных в витрину
        my.mart_rep_fraud1_load(conn=conn_in, cursor=cursor_in)  # commits are off from the function
        print( 'marts: type 1 was done' )
        my.mart_rep_fraud2_load(conn=conn_in, cursor=cursor_in)  # commits are off from the function
        print( 'marts: type 2 was done' )
        my.mart_rep_fraud3_load(conn=conn_in, cursor=cursor_in)  # commits are off from the function
        print( 'marts: type 3 was done' )
        my.mart_rep_fraud4_load(conn=conn_in, cursor=cursor_in)  # commits are off from the function
        print( 'marts: type 4 was done' )

        # Фиксация транзакции
        conn_in.commit()  # commits are off from the functions




        dates.remove(date)
        print(f"""
/////////////////////////////////
a bunch from {date} was loaded
////////////////////////////////
    """)
    elif (flags[0] == 'Y' or flags[1] == 'Y' or flags[2] == 'Y') and (flags[0] == 'N' or flags[1] == 'N' or flags[2] == 'N') :  # ! вторая часть условия может быть излишня!

        # Выгрузка данных сущности 'clients'
        clients = cursor_out.execute(""" select
                                            client_id ,
                                            last_name ,
                                            first_name ,
                                            patronymic ,
                                            date_of_birth ,
                                            passport_num ,
                                            passport_valid_to ,
                                            phone
                                        from info.clients """)
        titles = [x[0] for x in cursor_out.description]

        df = pandas.DataFrame(cursor_out.fetchall(), columns=titles)

        cursor_in.executemany(f""" insert into demipt3.kzmn_stg_source_clients
                                    ( client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, dt )
                                    values( %s, %s, %s, %s, %s, %s, %s, %s, '1901-01-01 00:00:00.000' )""", df.values.tolist())
        my.clients_scd2_load(conn=conn_in, cursor=cursor_in)  # commits are on in the function




        # Выгрузка данных сущности 'accounts'
        accounts = cursor_out.execute(""" select
                                            account ,
                                            valid_to ,
                                            client
                                        from info.accounts """)
        titles = [x[0] for x in cursor_out.description]

        df = pandas.DataFrame(cursor_out.fetchall(), columns=titles)

        cursor_in.executemany(f""" insert into demipt3.kzmn_stg_source_accounts
                                    ( account_num, valid_to, client, update_dt )
                                    values( %s, %s, %s, '1901-01-01 00:00:00.000' )""", df.values.tolist())
        my.accounts_scd1_load(conn=conn_in, cursor=cursor_in)  # commits are on in the function




        # Выгрузка данных сущности 'cards'
        cards = cursor_out.execute(""" select
                                            trim(card_num) ,
                                            account
                                        from info.cards """)
        titles = [x[0] for x in cursor_out.description]

        df = pandas.DataFrame(cursor_out.fetchall(), columns=titles)

        cursor_in.executemany(f""" insert into demipt3.kzmn_stg_source_cards
                                    ( card_num, account_num, update_dt )
                                    values( %s, %s, '1901-01-01 00:00:00.000' )""", df.values.tolist())
        my.cards_scd1_load(conn=conn_in, cursor=cursor_in)  # commits are on in the function


        # Выгрузка витрин при неполных файлах не имеет смысла ( meta_rep_fraud -- last_update_dt )


        dates.remove(date)
        print(f"""
//////////////////////////////////////////////////////////////////////////
a bunch from {date} was partly loaded -- one or more files were missing
/////////////////////////////////////////////////////////////////////////
    """)

print('finished // code 0')


cursor_out.close()
conn_out.close()
cursor_in.close()
conn_in.close()