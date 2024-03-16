
'''
# CARS (dim) : зависит от базы  ##
# DIVERS (dim) : зависит от базы, ЛОГфайла  ##
# CLIENTS (dim) : зависит от базы  ##
# PAYMENTS (fact) : зависит от payment_file, ЛОГфайла  ##
# WAYBILLS (fact) : зависит от waybills_file, drivers_tbl (dim), ЛОГфайла  ##
# RIDES (fact): зависит от waybills_tbl (fact), waybills_file, drivers_tbl (dim), ЛОГфайла, базы  ##
'''

scripts_path = 'D://-- MFTI --//Taxi project (additional)//'

import csv
import os
import psycopg2
import pandas
import numpy  #! numpy лучше избегать

import sys
import datetime
sys.path.insert( 0, f'{scripts_path}py_scripts' )
import TaxiLib as lib


# Настраиваем коннект для ввода
conn_in = psycopg2.connect( database = 	"xxxx" ,
                             host =	 	"xxxx" ,
                             user =	 	"xxxx" ,
                             password = "xxxx" ,
                             port =	 	"xxxx")
conn_in.autocommit = False
cursor_in = conn_in.cursor()

# Настраиваем коннект для вывода
conn_out = psycopg2.connect( database = "xxxx" ,
                             host =	 	"xxxx" ,
                             user =	 	"xxxx" ,
                             password = "xxxx" ,
                             port =	 	"xxxx")
conn_out.autocommit = False
cursor_out = conn_out.cursor()




# Назначение путей выгружаемых файлов
waybill_path = 'D:\\-- MFTI --\\Taxi project (additional)\\data_out\\'
payment_path = 'D:\\-- MFTI --\\Taxi project (additional)\\data_out\\'
archive_path = 'D:\\-- MFTI --\\Taxi project (additional)\\archive\\'


print( f"""------------------------------------------------------------------------------------
started at {str(datetime.datetime.now())[11:19]} :: {str(datetime.datetime.now())[:10]}
processing...
""" )


# Создание папки для отработанных файлов
path = r'archive'  # snippet taken from the Internet
if not os.path.exists(path):
    os.makedirs(path) #
    print( '''! archive folder was created
            ''' )


# Создание скриптов всех всех сущностей и загрузок кроме витрин  # (сначала текст скрипта генерировался функциями, затем индивидуально дополнялся вручную (дополнительные очистки stg/stg_del/stg_source в loads))
#lib.scd2entity_with_processed_dt_script_generator( schema_name='dwh_novokuznetsk', entity_name='cars', target_fields_names_and_types=[ 'plate_num char(9)', 'model_name varchar(20)', 'revision_dt date' ] )
#lib.scd2entity_with_processed_dt_script_generator( schema_name='dwh_novokuznetsk', entity_name='drivers', target_fields_names_and_types=[ 'personnel_num char(5)', 'last_name varchar(20)', 'first_name varchar(20)', 'middle_name varchar(20)', 'birth_dt date', 'card_num char(19)', 'driver_license_num char(12)', 'driver_license_dt date' ] )
#lib.scd2entity_with_processed_dt_script_generator( schema_name='dwh_novokuznetsk', entity_name='clients', target_fields_names_and_types=[ 'phone_num char(18)', 'card_num char(19)' ] )
#lib.factentity_with_processed_dt_script_generator( schema_name='dwh_novokuznetsk', entity_name='payments', target_fields_names_and_types=[ 'transaction_id varchar(9)', 'card_num char(19)', 'transaction_amt numeric(5,2)', 'transaction_dt timestamp(0)' ] )
#lib.factentity_with_processed_dt_script_generator( schema_name='dwh_novokuznetsk', entity_name='waybills', target_fields_names_and_types=[ 'waybill_num char(6)', 'driver_pers_num char(5)', 'car_plate_num char(9)', 'work_start_dt timestamp(0)', 'work_end_dt timestamp(0)', 'issue_dt timestamp(0)' ] )
#lib.factentity_with_processed_dt_script_generator( schema_name='dwh_novokuznetsk', entity_name='rides', target_fields_names_and_types=[ 'ride_id serial', 'point_from_txt text', 'point_to_txt text', 'distance_val numeric(5,2)', 'price_amt numeric(7,2)', 'client_phone_num char(18)', 'driver_pers_num char(5)', 'car_plate_num char(9)', 'ride_arrival_dt timestamp(0)', 'ride_start_dt timestamp(0)', 'ride_end_dt timestamp(0)' ] )
#
#lib.scd2load_with_processed_dt_script_generator( schema_name='dwh_novokuznetsk', entity_name='cars', fields=[ 'plate_num', 'model_name', 'revision_dt' ] )
#lib.scd2load_with_processed_dt_script_generator( schema_name='dwh_novokuznetsk', entity_name='drivers', fields=[ 'personnel_num', 'last_name', 'first_name', 'middle_name', 'birth_dt', 'card_num', 'driver_license_num', 'driver_license_dt' ] )
#lib.scd2load_with_processed_dt_script_generator( schema_name='dwh_novokuznetsk', entity_name='clients', fields=[ 'phone_num', 'card_num' ] )
#lib.factload_with_processed_dt_script_generator( schema_name='dwh_novokuznetsk', entity_name='payments', fields=[ 'transaction_id', 'card_num', 'transaction_amt', 'transaction_dt' ] )
#lib.factload_with_processed_dt_script_generator( schema_name='dwh_novokuznetsk', entity_name='waybills', fields=[ 'waybill_num', 'driver_pers_num', 'car_plate_num', 'work_start_dt', 'work_end_dt', 'issue_dt' ] )
#lib.factload_with_processed_dt_script_generator( schema_name='dwh_novokuznetsk', entity_name='rides', fields=[ 'ride_id', 'point_from_txt', 'point_to_txt', 'distance_val', 'price_amt', 'client_phone_num', 'driver_pers_num', 'car_plate_num', 'ride_arrival_dt', 'ride_start_dt', 'ride_end_dt' ] )



# Создание meta-таблицы для корректной заливки данных в витрину
'''try:
    cursor_in.execute( """ create table dwh_novokuznetsk.kzmn_meta_rep_fraud (
                                schema_name varchar(20) ,
                                table_name varchar(60) ,
                                last_update_dt date ) """ )
    cursor_in.execute( """ insert into demipt3.kzmn_meta_rep_fraud
                                    ( schema_name, table_name, last_update_dt )
                                values
                                    ( 'demipt3','kzmn_dwh_fact_passport_blacklist', to_date('2021-03-01','YYYY-MM-DD') ) """ )
    cursor_in.execute( """ insert into demipt3.kzmn_meta_rep_fraud
                                    ( schema_name, table_name, last_update_dt )
                                values
                                    ( 'demipt3','kzmn_dwh_dim_terminals_hist', to_date('2021-03-01','YYYY-MM-DD') ) """ )
    cursor_in.execute( """ insert into demipt3.kzmn_meta_rep_fraud
                                    ( schema_name, table_name, last_update_dt )
                                values
                                    ( 'demipt3','kzmn_dwh_fact_transactions', to_date('2021-03-01','YYYY-MM-DD') ) """ )
    conn_in.commit()
except psycopg2.errors.DuplicateTable:
    conn_in.rollback()'''




# Создание сущностей БД  # commits are on in the functions
lib.cars_entity( conn=conn_in, cursor=cursor_in )
lib.drivers_entity( conn=conn_in, cursor=cursor_in )
lib.clients_entity( conn=conn_in, cursor=cursor_in )
lib.waybills_entity( conn=conn_in, cursor=cursor_in )
lib.rides_entity( conn=conn_in, cursor=cursor_in )
lib.payments_entity( conn=conn_in, cursor=cursor_in )

'''# Создание витрины БД
my.mart_tables( conn=conn_in, cursor=cursor_in )'''

# Фиксация транзакции (разовое создание хранилища)  # commits are made in the functions
#conn_in.commit()




# Создание файла для сохранения номеров уже обработанных waybill
path = fr'{archive_path}waybill_file_number_log.txt'
if os.path.exists(path):
    if os.path.isfile(path):
        ...
else:
    file = open( f"{archive_path}waybill_file_number_log.txt", "w" )
    file.write("0")
    file.close()
    print( "! waybill_file_number_log was created" )

file = open( f"{archive_path}waybill_file_number_log.txt" )
waybill_number = int(file.read())
file.close()


# Создание файла для сохранения дат уже обработанных waybill
path = fr'{archive_path}waybill_file_date_log.txt'
if os.path.exists(path):
    if os.path.isfile(path):
        ...
else:
    file = open( f"{archive_path}waybill_file_date_log.txt", "w" )
    file.write("1900-01-01 00:00:00")
    file.close()
    print( "! waybill_file_date_log was created" )

file = open( f'{archive_path}waybill_file_date_log.txt' )
waybill_date = str(file.read())
file.close()




flags = [ 'N', 'N' ]
while waybill_number < 7 :  # < N где N = номеру waybill_file
    waybill_number += 1

    # Чтение waybill_file и определение limiter date для дальнейшей загрузки хранилища
    try:
        print( f'file waybill_{str(waybill_number).zfill(6)}.xml in a process' )
        df_waybill = pandas.read_xml( f'{waybill_path}waybill_{str(waybill_number).zfill(6)}.xml', xpath='//waybill|//waybill/driver|//waybill/period' )
        df_waybill = df_waybill[['number', 'issuedt', 'car', 'license', 'start', 'stop']]
        df_waybill['number'] = df_waybill['number'][0]
    
        old_date = waybill_date
        old_date = datetime.datetime.strptime(old_date, '%Y-%m-%d %H:%M:%S')
        new_date = str(df_waybill.loc[2, 'stop'])
        new_date = datetime.datetime.strptime(new_date, '%Y-%m-%d %H:%M:%S')
    
        if new_date > old_date:
            waybill_date = str(new_date)
        elif new_date <= old_date:
            waybill_date = str(old_date)

        # Смена флага
        flags[0] = 'Y'

    except Exception:
        print( f"""! there's no waybill_{str(waybill_number).zfill(6)}.xml file for the date {str(datetime.datetime.now())[:10]} -- ...meta...mart won't be updated""" )

        # Смена флага
        flags[0] = 'N'
    
    # Установление ограничителя выборки
    limiter = waybill_date




    # Распределение ветвей по наличию waybill_file
    # Отсутствие waybill_file -- никаких выгрузок не делается :
    if flags[0] == 'N' :

        # В этом условии не делается UPD
        ...
    
    # Присутствие waybill_file -- делаются выгрузки базы и витрин :
    elif flags[0] == 'Y' :

        # Выгрузка данных сущности 'cars'
        cursor_out.execute( """ select
                                    plate_num ,
                                    model ,
                                    revision_dt ,
                                    register_dt
                                from main.car_pool """ )
        titles = [x[0] for x in cursor_out.description]
    
        df = pandas.DataFrame(cursor_out.fetchall(), columns=titles)
    
        cursor_in.executemany( f""" insert into dwh_novokuznetsk.kzmn_stg_source_cars
                                            ( plate_num, model_name, revision_dt, dt )
                                             values( %s, %s, %s, %s ) """, df.values.tolist() )
        lib.cars_scd2_load(conn=conn_in, cursor=cursor_in)  # commits are OFF in the function  #! load, #5 (множество подзапросов)
    
    
    
    
        # Выгрузка данных сущности 'drivers'
        # Генерация номера для drivers.personnel_num
        # Создание файла для сохранения номеров
        path = fr'{archive_path}personnel_num_log.txt'
        if os.path.exists(path):
            if os.path.isfile(path):
                ...
        else:
            file = open( f"{archive_path}personnel_num_log.txt", "w" )
            file.write("A0000")
            file.close()
            print( "! personnel_num_log was created" )
    
        # Открытие файла и чтение номера
        file = open( f"{archive_path}personnel_num_log.txt" )
        number = file.read()
        file.close()
    
    
        # Выгрузка данных сущности 'drivers'
        cursor_in.execute( """ select cast(last_update_dt as varchar)
                                    from dwh_novokuznetsk.kzmn_meta_drivers """ )
        date = str(cursor_in.fetchone())[2:-3]
        cursor_out.execute( f""" select
                                    update_dt ,
                                    last_name ,
                                    first_name ,
                                    middle_name ,
                                    birth_dt ,
                                    card_num ,
                                    driver_license ,
                                    driver_valid_to 
                                from main.drivers
                                where update_dt > '{date}'
                                and update_dt <= to_timestamp( '{limiter}', 'YYYY-MM-DD HH24:MI:SS' ) + interval '1 hour' """ )  #! при update_dt как ДАТЕ ПОЯВЛЕНИЯ водителя
        titles = [x[0] for x in cursor_out.description]
    
        df = pandas.DataFrame(cursor_out.fetchall(), columns=titles)
    
        # Подсчет n-количества строк, необходимого для генерации номеров
        strings_amount = df.shape[0]
    
        # Генерация номеров в количестве n
        letters = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U',
                   'V', 'W', 'X', 'Y', 'Z']
        numbers_list = []
        letter = str(number[:1])
        numbers = int(number[1:])
        for i in range(int(strings_amount)):
            if numbers <= 9998:
                numbers = numbers + 1
                numbers_list.append(f'{letter}{str(numbers).zfill(4)}')
            elif numbers >= 9999:
                for unit in letters:
                    if unit == letter:
                        new_letter_index = (letters.index(unit) + 1) % len(letters)
                        letter = letters[new_letter_index]
                        numbers = 0
                        break
                numbers = numbers + 1
                numbers_list.append(f'{letter}{str(numbers).zfill(4)}')
    
        # Сохранение последнего номера в файле
        file = open( f"{archive_path}personnel_num_log.txt", "w" )
        file.write(f"{letter}{str(numbers).zfill(4)}")
        file.close()
        df['generated_numbers'] = numbers_list
    
        # Непосредственная выгрузка сущности 'drivers'
        cursor_in.executemany( f""" insert into dwh_novokuznetsk.kzmn_stg_source_drivers
                                            ( dt, last_name, first_name, middle_name, birth_dt, card_num, driver_license_num, driver_license_dt, personnel_num )
                                            values( %s, %s, %s, %s, %s, %s, %s, %s, %s ) """, df.values.tolist() ) 
        lib.drivers_scd2_load(conn=conn_in, cursor=cursor_in)  # commits are OFF in the function  #! load, #5 (множество подзапросов)




        # Выгрузка данных сущности 'clients'
        cursor_in.execute( """ select cast(last_update_dt as varchar)
        					        from dwh_novokuznetsk.kzmn_meta_clients """ )
        date = str(cursor_in.fetchone())[2:-3]
        cursor_out.execute( f""" select
                                distinct
        							    card_num ,
        							    client_phone ,
        							    dt
        						from main.rides
        						where dt > '{date}'
        						and dt <= to_timestamp( '{limiter}', 'YYYY-MM-DD HH24:MI:SS' ) + interval '1 hour' """ )
        titles = [x[0] for x in cursor_out.description]

        df = pandas.DataFrame(cursor_out.fetchall(), columns=titles)

        cursor_in.executemany( f""" insert into dwh_novokuznetsk.kzmn_stg_source_clients
        									( card_num, phone_num, dt )
        									values( %s, %s, %s ) """, df.values.tolist() )
        lib.clients_scd2_load(conn=conn_in, cursor=cursor_in)  # commits are OFF in the function  #! load, дубли, что делать с версиями на разные карты? что делать, если у одной карты разные телефоны?
        
        
        
        
        # Выгрузка сущности 'waybills'
        cursor_in.executemany( """ insert into dwh_novokuznetsk.kzmn_stg_add_waybills
                                            ( waybill_num, issue_dt, car_plate_num, license, work_start_dt, work_end_dt )
                                            values( %s, %s, %s, %s, %s, %s ) ; """, df_waybill.values.tolist() )
        lib.waybills_fact_load(conn=conn_in, cursor=cursor_in)  # commits are OFF from the function
    
        # Загрузка метаданных в таблицу витрины
        ...
    
        # Фиксация транзакции  # commits are OFF from the function
        #conn_in.commit()
    
        # Отработка файла
        #os.replace( f'{waybill_path}waybill_{str(waybill_number).zfill(6)}.xml', f'{archive_path}waybill_{str(waybill_number).zfill(6)}.xml.backup' )




        # Выгрузка данных сущности 'rides' // !# date должна быть самая рання start_dt в waybills, а limiter должна быть самая поздняя end_dt в waybills		
        # Получение даты для оптимизации
        cursor_in.execute( """ select cast(min(work_start_dt) as varchar)
        							from dwh_novokuznetsk.kzmn_dwh_fact_waybills """ )
        date_rides = str(cursor_in.fetchone())[2:-3]
        cursor_in.execute( """ select cast(max(work_end_dt) as varchar)
        							from dwh_novokuznetsk.kzmn_dwh_fact_waybills """ )
        limiter_rides = str(cursor_in.fetchone())[2:-3]

        # Подгрузка stg1
        cursor_out.execute( f""" select
        							ride_id ,
        							point_from ,
        							point_to ,
        							distance ,
        							price ,
        							client_phone
        						from main.rides
        						where dt > '{date_rides}'
        						and dt <= to_timestamp( '{limiter_rides}', 'YYYY-MM-DD HH24:MI:SS' ) + interval '1 hour' """ )
        titles = [x[0] for x in cursor_out.description]

        df = pandas.DataFrame(cursor_out.fetchall(), columns=titles)

        cursor_in.executemany( f""" insert into dwh_novokuznetsk.kzmn_stg1_rides
        									( ride_id, point_from_txt, point_to_txt, distance_val, price_amt, client_phone_num )
        									values( %s, %s, %s, %s, %s, %s ) """, df.values.tolist() )

        # Подгрузка stg2
        cursor_out.execute( f""" select
        							ride_id ,
        							car_plate_num ,
        							ride_arrival_dt ,
        							ride_start_dt ,
        							ride_end_dt
        						from (
        							select
        								max(ride) as ride_id ,
        								max(car_plate_num) as car_plate_num ,
        								max(case 
        									when event = 'READY' then dt
        								end) as ride_arrival_dt ,
        								max(case 
        									when event = 'BEGIN' or event is null then dt
        								end) as ride_start_dt ,
        								max(case 
        									when event = 'END' or event = 'CANCEL' then dt
        								end) as ride_end_dt 	
        							from main.movement
        							group by ride ) sq
        						where ride_end_dt is not null
        							and (ride_arrival_dt > '{date_rides}'
        								and (ride_start_dt is null or ride_start_dt > '{date_rides}')
        								and ride_end_dt > '{date_rides}' )
        							and (ride_arrival_dt <= to_timestamp( '{limiter_rides}', 'YYYY-MM-DD HH24:MI:SS' ) + interval '1 hour'
        								and (ride_start_dt is null or ride_start_dt <= to_timestamp( '{limiter_rides}', 'YYYY-MM-DD HH24:MI:SS' ) + interval '1 hour')
        								and ride_end_dt <= to_timestamp( '{limiter_rides}', 'YYYY-MM-DD HH24:MI:SS' ) + interval '1 hour' ) """ )
        titles = [x[0] for x in cursor_out.description]

        df = pandas.DataFrame(cursor_out.fetchall(), columns=titles)
        df = df.replace({ numpy.NaN: None })

        cursor_in.executemany( f""" insert into dwh_novokuznetsk.kzmn_stg2_rides
        									( ride_id, car_plate_num, ride_arrival_dt, ride_start_dt, ride_end_dt )
        									values( %s, %s, %s, %s, %s ) """, df.values.tolist() )
        lib.rides_fact_load(conn=conn_in, cursor=cursor_in)  # commits are OFF from the function

        # Фиксация транзакции  # commits are OFF from the function
        #conn_in.commit()
        
        
        # Общий коммит пачки N waybill файла N
        conn_in.commit()


        print( f"""
main tables, waybill tables were updated
-- a bunch from {waybill_date}
    """ )




# Дополнительная оптимизиационная очистка таблиц
cursor_in.execute( """ delete from dwh_novokuznetsk.kzmn_stg_source_cars """)
cursor_in.execute( """ delete from dwh_novokuznetsk.kzmn_stg_source_drivers """)
cursor_in.execute( """ delete from dwh_novokuznetsk.kzmn_stg_source_clients """ )

cursor_in.execute( """ insert into dwh_novokuznetsk.kzmn_stg_del_add_cars
                                ( plate_num )
                                select
                                distinct
                                    plate_num
                                from dwh_novokuznetsk.kzmn_stg_del_cars """ )
cursor_in.execute( """ delete from dwh_novokuznetsk.kzmn_stg_del_cars """ )
cursor_in.execute( """ insert into dwh_novokuznetsk.kzmn_stg_del_cars
                                    ( plate_num )
                                    select
                                        plate_num
                                    from dwh_novokuznetsk.kzmn_stg_del_add_cars """ )
cursor_in.execute( """ delete from dwh_novokuznetsk.kzmn_stg_del_add_cars """ )

cursor_in.execute( """ insert into dwh_novokuznetsk.kzmn_stg_del_add_clients
                                ( phone_num )
                                select
                                distinct
                                    phone_num
                                from dwh_novokuznetsk.kzmn_stg_del_clients """ )
cursor_in.execute( """ delete from dwh_novokuznetsk.kzmn_stg_del_clients """ )
cursor_in.execute( """ insert into dwh_novokuznetsk.kzmn_stg_del_clients
                                    ( phone_num )
                                    select
                                        phone_num
                                    from dwh_novokuznetsk.kzmn_stg_del_add_clients """ )
cursor_in.execute( """ delete from dwh_novokuznetsk.kzmn_stg_del_add_clients """ )

cursor_in.execute( """ insert into dwh_novokuznetsk.kzmn_stg_del_add_drivers
                                ( personnel_num )
                                select
                                distinct
                                    personnel_num
                                from dwh_novokuznetsk.kzmn_stg_del_drivers """ )
cursor_in.execute( """ delete from dwh_novokuznetsk.kzmn_stg_del_drivers """ )
cursor_in.execute( """ insert into dwh_novokuznetsk.kzmn_stg_del_drivers
                                    ( personnel_num )
                                    select
                                        personnel_num
                                    from dwh_novokuznetsk.kzmn_stg_del_add_drivers """ )
cursor_in.execute( """ delete from dwh_novokuznetsk.kzmn_stg_del_add_drivers """ )

conn_in.commit()


# Сохранение логов
file = open( f"{archive_path}waybill_file_number_log.txt","w" )
file.write( f"{int(waybill_number)}" )
file.close()

file = open( f"{archive_path}waybill_file_date_log.txt","w" )
file.write( waybill_date )
file.close()




# Выгрузка сущности 'payments' в отдельном цикле

# Создание файла для сохранения номеров уже обработанных payment
path = fr'{archive_path}transaction_id_log.txt'
if os.path.exists(path):
    if os.path.isfile(path):
        ...
else:
    file = open( f"{archive_path}transaction_id_log.txt", "w" )
    file.write("A-0000000")
    file.close()
    print( "! transaction_id_log was created" )


# Основной цикл:
#dates = [ '2022-11-19_00-30', '2022-11-19_01-00', '2022-11-19_01-30', '2022-11-19_02-00', '2022-11-19_02-30', '2022-11-19_03-00', '2022-11-19_03-30', '2022-11-19_04-00', '2022-11-19_04-30', '2022-11-19_05-00', '2022-11-19_05-30', '2022-11-19_06-00', '2022-11-19_06-30', '2022-11-19_07-00' ]
dates = [ '2022-11-19_00-30', '2022-11-19_01-00', '2022-11-19_01-30' ]
while len(dates) > 0 :
    date = dates[0]


    # Открытие файла и чтение номера
    file = open( f'{archive_path}transaction_id_log.txt' )
    number = file.read()
    file.close()

    try:
        # Непосредственная выгрузка сущности 'payments'
        df = pandas.read_csv( fr'{payment_path}payment_{date}.csv', encoding='utf8', header=None, delimiter='\t' )
        df.columns = ['date', 'code', 'amount']

        # Подсчет n-количества строк, необходимого для генерации номеров для payments.transaction_id
        strings_amount = df.shape[0]

        # Генерация номеров в количестве n
        letters = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U',
                   'V', 'W', 'X', 'Y', 'Z']

        numbers_list = []
        letter = str(number[0:1])
        numbers = int(number[3:])
        for i in range(int(strings_amount)):
            if numbers <= 9999998:
                numbers = numbers + 1
                numbers_list.append(f'{letter}-{str(numbers).zfill(7)}')
            elif numbers >= 9999999:
                for unit in letters:
                    if unit == letter:
                        new_letter_index = (letters.index(unit) + 1) % len(letters)
                        letter = letters[new_letter_index]
                        numbers = 0
                        break
                numbers = numbers + 1
                numbers_list.append(f'{letter}-{str(numbers).zfill(7)}')

        # Выгрузка датафрейма 
        df['transaction_id'] = numbers_list

        cursor_in.executemany( """ insert into dwh_novokuznetsk.kzmn_stg_payments
                                                ( transaction_dt, card_num, transaction_amt, transaction_id )
                                                values( to_timestamp ( %s ,'DD-MM-YYYY HH24:MI:SS'), %s, %s, %s ) ; """, df.values.tolist() )
        lib.payments_fact_load( conn=conn_in, cursor=cursor_in )  # commits are off from the function

        # Фиксация транзакции  # commits are off from the function
        conn_in.commit()

        # Отработка файла
        #os.replace( f'{payment_path}payment_{date}.csv', f'{archive_path}payment_2022-11-19_00-30.csv.backup' )

        # Сохранение логов
        file = open( f"{archive_path}transaction_id_log.txt", "w" )
        file.write(f'{letter}-{str(numbers).zfill(7)}')
        file.close()
        df['transaction_id'] = numbers_list

        # Смена флага
        #flags[1] = 'Y'

        print(f"""payment tables were updated
-- a bunch from {date}""")

    except FileNotFoundError :
        print( f"""! there's no payment_{date}.csv file for the date {str(datetime.datetime.now())[:10]} -- ...meta...mart won't be updated""" )

        # Смена флага
        #flags[1] = 'N'
    
        print( f"""! payment tables weren't updated
-- a bunch from {date}""" )
    dates.remove(date)


print( f"""
finished at {str(datetime.datetime.now())[11:19]} :: {str(datetime.datetime.now())[:10]}
code 0
------------------------------------------------------------------------------------""" )


cursor_out.close()
conn_out.close()
cursor_in.close()
conn_in.close()
