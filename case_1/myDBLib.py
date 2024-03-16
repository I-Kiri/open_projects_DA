'''
Вывод сообщения о создании каждой таблицы в ddl процессе отдельно необходим -- в случае сообщения при автозапуске и завершенном ddl оно будет свидетельствовать о поломке.
'''
import psycopg2.errors
import copy


def scd1entity_script_generator( schema_name, entity_name, target_fields_names_and_types ):  # был вариант с раздельными списками target_fields и target_types, однако их элементы все равно требовалось сопостовлять, поэтому для экономии ресурсов я сразу ввела один список 
    key_field = target_fields_names_and_types[0]

    def field_in_list():
        s = ''
        fields_func = []
        for field in target_fields_names_and_types:
            update_string = f'{field}, '
            fields_func.append( update_string )
        fields_func = s.join( fields_func )
        return fields_func


    print( f'''
    # Создание в хранилище сущности "{entity_name}" :

    # Создание искусственного источника (сущности "{entity_name}")
    try:
        cursor.execute( """ create table {schema_name}.kzmn_stg_source_{entity_name}( 
							{field_in_list()}
                            update_dt timestamp(0) ) ; """ )
        print('created table {schema_name}.kzmn_stg_source_{entity_name}')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание target-таблицы (сущности "{entity_name}")
    try:
        cursor.execute( """ create table {schema_name}.kzmn_dwh_dim_{entity_name} (
							{field_in_list()}
							create_dt timestamp(0) ,
							update_dt timestamp(0) ) ; """ )
        print('created table {schema_name}.kzmn_dwh_dim_{entity_name}')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание stg-таблицы
    try:
        cursor.execute( """ create table {schema_name}.kzmn_stg_{entity_name}( 
							{field_in_list()}
							update_dt timestamp(0) ) ; """ )
        print('created table {schema_name}.kzmn_stg_{entity_name}')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание stg_del-таблицы
    try:
        cursor.execute( """ create table {schema_name}.kzmn_stg_del_{entity_name} ( 
							{key_field} ) ; """ )
        print('created table {schema_name}.kzmn_stg_del_{entity_name}')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание meta-таблицы
    try:
        cursor.execute( """ create table {schema_name}.kzmn_meta_{entity_name} (
                            schema_name varchar(20) ,
                            table_name varchar(60) ,
                            max_update_dt timestamp(0) ) ; """ )
        cursor.execute( """ insert into {schema_name}.kzmn_meta_{entity_name}
                                ( schema_name, table_name, max_update_dt )
                            values
                                ( '{schema_name}','kzmn_stg_source_{entity_name}', to_timestamp('1900-01-01','YYYY-MM-DD') ) ; """ )
        print('created table {schema_name}.kzmn_meta_{entity_name}')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()
    conn.commit()  # необходимо зафиксировать транзакцию в любом состоянии

    return 1
    ''' )

    return 1


def scd2entity_script_generator( schema_name, entity_name, target_fields_names_and_types ):  # был вариант с раздельными списками target_fields и target_types, однако их элементы все равно требовалось сопостовлять, поэтому для экономии ресурсов я сразу ввела один список 
    key_field = target_fields_names_and_types[0]

    def field_in_list():
        s = ''
        fields_func = []
        for field in target_fields_names_and_types:
            update_string = f'{field}, '
            fields_func.append( update_string )
        fields_func = s.join( fields_func )
        return fields_func
    

    print( f'''
    # Создание в хранилище сущности "{entity_name}" :

    # Создание искусственного источника (сущности "{entity_name}")
    try:
        cursor.execute( """ create table {schema_name}.kzmn_stg_source_{entity_name} (
                            {field_in_list()}
                            dt timestamp(0) ) ; """ )
        print('created table {schema_name}.kzmn_stg_source_{entity_name}')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание target-таблицы (сущности "{entity_name}")
    try:
        cursor.execute( """ create table {schema_name}.kzmn_dwh_dim_{entity_name}_hist ( 
                            {field_in_list()}
                            effective_from timestamp(0) ,
                            effective_to timestamp(0) ,
                            deleted_flg char(1) ) ; """ )
        print('created table {schema_name}.kzmn_dwh_dim_{entity_name}_hist')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание stg-таблицы
    try:
        cursor.execute( """ create table {schema_name}.kzmn_stg_{entity_name} (
                            {field_in_list()}
                            update_dt timestamp(0) ) ; """ )
        print('created table {schema_name}.kzmn_stg_{entity_name}')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание meta_stg_del-таблицы
    try:
        cursor.execute( """ create table {schema_name}.kzmn_stg_del_{entity_name} (
                            {key_field} ) ; """ )
        print('created table {schema_name}.kzmn_stg_del_{entity_name}')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание meta-таблицы
    try:
        cursor.execute( """ create table {schema_name}.kzmn_meta_{entity_name} (
                            schema_name varchar(20) ,
                            table_name varchar(60) ,
                            last_update_dt timestamp(0) ) ; """ )
        cursor.execute( """ insert into {schema_name}.kzmn_meta_{entity_name}
                                    ( schema_name, table_name, last_update_dt )
                                values
                                    ( '{schema_name}','kzmn_stg_source_{entity_name}', to_timestamp('1900-01-01','YYYY-MM-DD') ) ; """ )
        print('created table {schema_name}.kzmn_meta_{entity_name}')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()
    conn.commit()  # необходимо зафиксировать транзакцию в любом состоянии

    return 1
    ''' )

    return 1


def factentity_script_generator( schema_name, entity_name, target_fields_names_and_types ):

    def field_in_list():
        s = ''
        fields_func = []
        for field in target_fields_names_and_types:
            update_string = f'{field}, '
            fields_func.append( update_string )
        fields_func = s.join( fields_func )
        return fields_func[:-2]


    print( f'''
    # Создание в хранилище сущности "{entity_name}" :

    # Создание stg-источника (сущности "{entity_name}")
    try:
        cursor.execute( """ create table {schema_name}.kzmn_stg_{entity_name} ( 
                                    {field_in_list()} ) ; """ )
        print('created table {schema_name}.kzmn_stg_{entity_name}')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание фактовой target-таблицы (сущности "{entity_name}")
    try:
        cursor.execute( """ create table {schema_name}.kzmn_dwh_fact_{entity_name} (
                                {field_in_list()} ) ; """ )
        print('created table {schema_name}.kzmn_dwh_fact_{entity_name}')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()  
    conn.commit()  # необходимо зафиксировать транзакцию в любом состоянии

    return 1
    ''' )

    return 1


def scd1load_script_generator( schema_name, entity_name, fields ):
    s = ', '
    key_field = fields[0]
    fields_str = s.join(fields)

    def selected_field_in_list( alias ):
        s = ''
        fields_func = []
        for field in fields:
            update_string = f'{alias}.{field}, '
            fields_func.append(update_string)
        fields_func = s.join(fields_func)
        return fields_func

    def field_in_list():
        fields_func_1 = copy.deepcopy( fields )
        fields_func_1.remove( fields_func_1[ 0 ] )
        s = ''
        fields_func = []
        for field in fields_func_1:
            update_string = f"""{field} = tmp.{field} , """
            fields_func.append(update_string)
        fields_func = s.join(fields_func)
        return fields_func

    def field_in_list_2():
        fields_func_2 = copy.deepcopy( fields )
        fields_func_2.remove(fields_func_2[0])
        s = ''
        fields_func = []
        for field in fields_func_2:
            update_string = f"""----
                                or ( s.{field} <> t.{field} or (s.{field} is null and t.{field} is not null) or (s.{field} is not null and t.{field} is null) )"""
            fields_func.append(update_string)
        fields_func = s.join(fields_func)
        return fields_func

    print( f'''
    # Последовательная инкрементальная выгрузка данных сущности "{entity_name}" :

    try:
        # 1. Очистка стейджинговых таблиц
        cursor.execute( """ delete from {schema_name}.kzmn_stg_{entity_name} ; """ )
        cursor.execute( """ delete from {schema_name}.kzmn_stg_del_{entity_name} ; """ )

        # 2. Захват данных из источника (измененных с момента последней загрузки) в стейджинг
        cursor.execute( """ insert into {schema_name}.kzmn_stg_{entity_name}
                                ( {fields_str}, update_dt )
                                    select
                                        {fields_str},
                                        update_dt
                                    from {schema_name}.kzmn_stg_source_{entity_name}
                                    where update_dt > ( select max_update_dt
                                                        from {schema_name}.kzmn_meta_{entity_name}
                                                        where schema_name='{schema_name}'
                                                            and table_name='kzmn_stg_source_{entity_name}' ) ; """ )

        # 3. Захват полного среза ключей для вычисления удалений
        cursor.execute( """ insert into {schema_name}.kzmn_stg_del_{entity_name}
                                ( {key_field} )
                                    select
                                        {key_field}
                                    from {schema_name}.kzmn_stg_source_{entity_name} ; """ )

        # 4. Загрузка в приемник "вставок" на источнике (формат SCD1)
        cursor.execute( """ insert into {schema_name}.kzmn_dwh_dim_{entity_name}
                                ( {fields_str}, create_dt, update_dt )
                                    select
                                        {selected_field_in_list( alias='s' )}
                                        s.update_dt,
                                        null
                                    from {schema_name}.kzmn_stg_{entity_name} s
                                    left join {schema_name}.kzmn_dwh_dim_{entity_name} t 
                                    on s.{key_field} = t.{key_field}
                                    where t.{key_field} is null ; """ )

        # 5. Обновление в приемнике "обновлений" на источнике (формат SCD1)
        cursor.execute( """ update {schema_name}.kzmn_dwh_dim_{entity_name}
                            set
                                {field_in_list()}
                                update_dt = tmp.update_dt
                            from (
                                select
                                    {selected_field_in_list( alias='s' )}
                                    s.update_dt,
                                    null
                                from {schema_name}.kzmn_stg_{entity_name} s
                                inner join {schema_name}.kzmn_dwh_dim_{entity_name} t 
                                on s.{key_field} = t.{key_field}
                                where 1=0
                                {field_in_list_2()}
                            ) tmp
                            where kzmn_dwh_dim_{entity_name}.{key_field} = tmp.{key_field} ; """ )

        # 6. Удаление удаленных записей (формат SCD1)
        cursor.execute( """ delete from {schema_name}.kzmn_dwh_dim_{entity_name} 
                            where {key_field} in (
                                select t.{key_field}
                                from {schema_name}.kzmn_dwh_dim_{entity_name} t
                                left join {schema_name}.kzmn_stg_del_{entity_name} s
                                on t.{key_field} = s.{key_field}
                                where s.{key_field} is null ) ;  """ )

        # 7. Обновление метаданных.
        cursor.execute( """ update {schema_name}.kzmn_meta_{entity_name}
                                set max_update_dt = coalesce( ( select max(update_dt)
                                                                from {schema_name}.kzmn_stg_{entity_name} ),
                                                                    ( select max_update_dt
                                                                    from {schema_name}.kzmn_meta_{entity_name}
                                                                    where schema_name='{schema_name}'
                                                                        and table_name='kzmn_stg_source_{entity_name}' ) )
                                where schema_name='{schema_name}'
                                    and table_name='kzmn_stg_source_{entity_name}'; """ )

        # 8. Фиксация транзакции
        conn.commit()

    except Exception :
        conn.rollback()
        print( '{entity_name}_scd1_load --> something went wrong' )

    return 1
    ''' )

    return 1


def scd2load_script_generator( schema_name, entity_name, fields ):
    s = ', '
    key_field = fields[0]
    fields_str = s.join(fields)

    def selected_field_in_list( alias ):
        s = ''
        fields_func = []
        for field in fields:
            update_string = f'{alias}.{field}, '
            fields_func.append(update_string)
        fields_func = s.join(fields_func)
        return fields_func

    def field_in_list():
        fields_func_1 = copy.deepcopy( fields )
        fields_func_1.remove(fields_func_1[0])
        s = ''
        fields_func = []
        for field in fields_func_1:
            update_string = f"""----
                                    or ( (s.{field} <> t.{field} or (s.{field} is null and t.{field} is not null) or (s.{field} is not null and t.{field} is null))
                                        and s.{field} not in (select t.{field} from {schema_name}.kzmn_stg_{entity_name} s inner join {schema_name}.kzmn_dwh_dim_{entity_name}_hist t on s.{key_field} = t.{key_field}) )"""
            fields_func.append(update_string)
        fields_func = s.join(fields_func)
        return fields_func

    def field_in_list_2():
        fields_func_2 = copy.deepcopy( fields )
        fields_func_2.remove(fields_func_2[0])
        s = ''
        fields_func = []
        for field in fields_func_2:
            update_string = f""" and kzmn_dwh_dim_{entity_name}_hist.{field} = ttemp.{field}"""
            fields_func.append(update_string)
        fields_func = s.join(fields_func)
        return fields_func

    def field_in_list_3():
        fields_add = fields
        # del fields_add[0]
        fields_add.remove(fields_add[0])

        s = ''
        fields_func = []
        fields_func_2 = []
        fields_func_2 = s.join( f'kzmn_dwh_dim_{entity_name}_hist.{fields_add[0]} = ttemp.{fields_add[0]} ' )
        # del fields_add[0]
        fields_add.remove(fields_add[0])

        for field in fields:
            update_string = f"""and kzmn_dwh_dim_{entity_name}_hist.{field} = ttemp.{field}"""
            fields_func.append(update_string)
        fields_func = fields_func_2 + s.join(fields_func)
        return fields_func

    print( f'''
    # Последовательная инкрементальная выгрузка данных сущности "{entity_name}" :

    try:
        # 1. Очистка стейджинговых таблиц
        cursor.execute( """delete from {schema_name}.kzmn_stg_{entity_name} ;""" )
        cursor.execute( """delete from {schema_name}.kzmn_stg_del_{entity_name} ;""" )

        # 2. Захват данных из источника (измененных с момента последней загрузки) в стейджинг
        cursor.execute( """insert into {schema_name}.kzmn_stg_{entity_name}
                                ( {fields_str} )
                                    select
                                        {fields_str} ,
                                        dt
                                    from {schema_name}.kzmn_stg_source_{entity_name}
                                    where dt > ( select last_update_dt
                                                from {schema_name}.kzmn_meta_{entity_name}
                                                where schema_name='{schema_name}'
                                                    and table_name='kzmn_stg_source_{entity_name}' ) ;""")

        # 3. Захват полного среза ключей для вычисления удалений
        cursor.execute( """insert into {schema_name}.kzmn_stg_del_{entity_name}
                                ( {key_field} )
                                    select
                                        {key_field}
                                    from {schema_name}.kzmn_stg_source_{entity_name} ;""")

        # 4. Загрузка в приемник "вставок" на источнике (формат SCD2)
        cursor.execute( """insert into {schema_name}.kzmn_dwh_dim_{entity_name}_hist
                                ( {fields_str}, effective_from, effective_to, deleted_flg )
                                    select
                                        {selected_field_in_list( alias='s' )}
                                        s.update_dt ,
                                        coalesce(cast (lead ( s.update_dt) over ( partition by s.{key_field} order by s.update_dt ) - interval '1 day' as date), '9999-12-31' ) , 
                                        'N'
                                    from {schema_name}.kzmn_stg_{entity_name} s
                                    left join {schema_name}.kzmn_dwh_dim_{entity_name}_hist t 
                                    on s.{key_field} = t.{key_field}
                                    where t.{key_field} is null ;""")

        # 5. Обновление в приемнике "обновлений" на источнике (формат SCD2, с учетом фиктивных обновлений)
        cursor.execute( """insert into {schema_name}.kzmn_dwh_dim_{entity_name}_hist
                                ( {fields_str}, effective_from, effective_to, deleted_flg ) 
                                    select distinct 
                                        {selected_field_in_list( alias='s' )}
                                        s.update_dt ,
                                        cast(null as timestamp),
                                        'N'
                                    from {schema_name}.kzmn_stg_{entity_name} s
                                    left join {schema_name}.kzmn_dwh_dim_{entity_name}_hist t
                                        on s.{key_field} = t.{key_field}
                                    where 1=0
                                    {field_in_list()} ;""")

        cursor.execute( """update {schema_name}.kzmn_dwh_dim_{entity_name}_hist
                            set 
                                effective_to = ttemp.effective_to
                            from (
                                select
                                    {selected_field_in_list( alias='a' )}
                                    coalesce((lead ( a.effective_from ) over ( partition by a.{key_field} order by a.effective_from ) - interval '1 day' ), '9999-12-31' ) as effective_to 
                                from {schema_name}.kzmn_dwh_dim_{entity_name}_hist a
                                inner join {schema_name}.kzmn_dwh_dim_{entity_name}_hist b
                                    on a.{key_field} = b.{key_field}
                                    and b.effective_to is null
                            ) ttemp
                        where ( kzmn_dwh_dim_{entity_name}_hist.{key_field} = ttemp.{key_field}
                                {field_in_list_2()} ) ;""")

        # 6. Удаление удаленных записей (формат SCD2)
        cursor.execute( """insert into {schema_name}.kzmn_dwh_dim_{entity_name}_hist
                                ( {fields_str}, effective_from, effective_to, deleted_flg )
                                    select 
                                        {fields_str} ,
                                        cast( now() as timestamp ) ,
                                        cast( null as timestamp ) , 
                                        'Y' 
                                    from {schema_name}.kzmn_dwh_dim_{entity_name}_hist
                                    where {key_field} in (
                                        select
                                            t.{key_field}
                                        from {schema_name}.kzmn_dwh_dim_{entity_name}_hist t 
                                        left join {schema_name}.kzmn_stg_del_{entity_name} s
                                            on t.{key_field} = s.{key_field}
                                            where s.{key_field} is null
                                            )
                                        and effective_to = '9999-12-31'
                                        and deleted_flg = 'N' ;""")

        cursor.execute( """update {schema_name}.kzmn_dwh_dim_{entity_name}_hist
                            set 
                                effective_to = ttemp.effective_to
                            from (
                                select
                                {fields_str} ,
                                effective_to
                                from (	select
                                        {selected_field_in_list( alias='a' )}
                                        a.effective_to as dt ,
                                        coalesce((lead ( a.effective_from ) over ( partition by a.{key_field} order by a.effective_from ) - interval '1 day' ), '9999-12-31' ) as effective_to
                                        from {schema_name}.kzmn_dwh_dim_{entity_name}_hist a
                                        inner join {schema_name}.kzmn_dwh_dim_{entity_name}_hist b
                                            on a.{key_field} = b.{key_field}
                                            and b.effective_to is null ) sq
                                where dt = '9999-12-31'
                            ) ttemp
                        where kzmn_dwh_dim_{entity_name}_hist.{key_field} = ttemp.{key_field}
                            and ( {field_in_list_3()} )
                                and kzmn_dwh_dim_{entity_name}_hist.effective_to  = '9999-12-31' ;""")
        cursor.execute( """update {schema_name}.kzmn_dwh_dim_{entity_name}_hist
                            set 
                                effective_to = ttemp.effective_to
                            from (
                                select
                                    {fields_str} ,
                                    effective_from ,
                                    coalesce(effective_to, '9999-12-31') as effective_to ,
                                    deleted_flg
                                from {schema_name}.kzmn_dwh_dim_{entity_name}_hist
                                where deleted_flg = 'Y'
                            ) ttemp
                        where kzmn_dwh_dim_{entity_name}_hist.{key_field} = ttemp.{key_field}
                            and ( {field_in_list_3()} )
                                and kzmn_dwh_dim_{entity_name}_hist.effective_to is null ;""")

        # 7. Обновление метаданных
        cursor.execute( """update {schema_name}.kzmn_meta_{entity_name}
                                set last_update_dt = coalesce( ( select max(update_dt)
                                                                from {schema_name}.kzmn_stg_{entity_name} ),
                                                                    ( select last_update_dt
                                                                    from {schema_name}.kzmn_meta_{entity_name}
                                                                    where schema_name='{schema_name}'
                                                                        and table_name='kzmn_stg_source_{entity_name}' ) )
                                where schema_name='{schema_name}'
                                    and table_name='kzmn_stg_source_{entity_name}' ;""")

        # 8. Фиксация транзакции
        conn.commit()

    except Exception :
        conn.rollback()
        print( '{entity_name}_scd2_load --> something went wrong' )

    return 1
    ''' )

    return 1


def factload_script_generator( schema_name, entity_name, fields ):
    key_field = fields[0]

    def selected_field_in_list():
        s = ''
        fields_func = []
        for field in fields:
            update_string = f's.{field}, '
            fields_func.append(update_string)
        fields_func = s.join(fields_func)
        return fields_func[:-2]

    def field_in_list():
        s = ''
        fields_func = []
        for field in fields:
            update_string = f'{field}, '
            fields_func.append(update_string)
        fields_func = s.join(fields_func)
        return fields_func[:-2]

    print( f'''
    # Фактовая выгрузка сущности "{entity_name}" :

    try:
        # 1. Загрузка новых значений из стейджинга
        cursor.execute( """ insert into {schema_name}.kzmn_dwh_fact_{entity_name}
                                ( {field_in_list()} )
                                    select
                                        {selected_field_in_list()}
                                    from {schema_name}.kzmn_stg_{entity_name} s
                                    left join {schema_name}.kzmn_dwh_fact_{entity_name} t 
                                    on s.{key_field} = t.{key_field}
                                    where t.{key_field} is null ; """ )

        # 2. Дополнительная очистка стейджинга-источника
        cursor.execute( """ delete from {schema_name}.kzmn_stg_{entity_name} ; """ )

        # 3. Фиксация транзакции
        #conn.commit()

    except Exception :
        conn.rollback()
        print( '{entity_name}_fact_load --> something went wrong' )

    return 1
    ''' )

    return 1


def passport_blacklist_entity( conn, cursor ):
    # Создание в хранилище сущности "passport_blacklist" :
    
    # Создание stg-источника (сущности "passport_blacklist")
    try:
        cursor.execute( """ create table demipt3.kzmn_stg_passport_blacklist( 
                                    passport_num char(11) ,
                                    entry_dt date ) ; """ )
        print('created table demipt3.kzmn_stg_passport_blacklist')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()
    
    # Создание фактовой target-таблицы (сущности "passport_blacklist")
    try:
        cursor.execute( """ create table demipt3.kzmn_dwh_fact_passport_blacklist(
                                passport_num char(11) ,
                                entry_dt date ) ; """ )
        print('created table demipt3.kzmn_dwh_fact_passport_blacklist')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()  
    conn.commit()  # необходимо зафиксировать транзакцию в любом состоянии
    
    return 1


def terminals_entity( conn, cursor ):
    # Создание в хранилище сущности "terminals" :

    # Создание искусственного источника (сущности "terminals")
    try:
        cursor.execute( """ create table demipt3.kzmn_stg_source_terminals (
                            terminal_id char(5) ,
                            terminal_type char(3) ,
                            terminal_city varchar(20) ,
                            terminal_address varchar(60) ,
                            dt timestamp(0) ) ; """ )
        print('created table demipt3.kzmn_stg_source_terminals')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание target-таблицы (сущности "terminals")
    try:
        cursor.execute( """ create table demipt3.kzmn_dwh_dim_terminals_hist ( 
                            terminal_id char(5) ,
                            terminal_type char(3) ,
                            terminal_city varchar(20) ,
                            terminal_address varchar(60) ,
                            effective_from timestamp(0) ,
                            effective_to timestamp(0) ,
                            deleted_flg char(1) ) ; """ )
        print('created table demipt3.kzmn_dwh_dim_terminals_hist')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание stg-таблицы
    try:
        cursor.execute( """ create table demipt3.kzmn_stg_terminals (
                            terminal_id char(5) ,
                            terminal_type char(3) ,
                            terminal_city varchar(20) ,
                            terminal_address varchar(60) ,
                            update_dt timestamp(0) ) ; """ )
        print('created table demipt3.kzmn_stg_terminals')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание meta_stg_del-таблицы
    try:
        cursor.execute( """ create table demipt3.kzmn_stg_del_terminals (
                            terminal_id char(5) ) ; """ )
        print('created table demipt3.kzmn_stg_del_terminals')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание meta-таблицы
    try:
        cursor.execute( """ create table demipt3.kzmn_meta_terminals (
                            schema_name varchar(20) ,
                            table_name varchar(60) ,
                            last_update_dt timestamp(0) ) ; """ )
        cursor.execute( """ insert into demipt3.kzmn_meta_terminals
                                    ( schema_name, table_name, last_update_dt )
                                values
                                    ( 'demipt3','kzmn_stg_source_terminals', to_timestamp('1900-01-01','YYYY-MM-DD') ) ; """ )
        print('created table demipt3.kzmn_meta_terminals')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()
    conn.commit()  # необходимо зафиксировать транзакцию в любом состоянии

    return 1


def clients_entity( conn, cursor ):
    # Создание в хранилище сущности "clients" :

    # Создание искусственного источника (сущности "clients")
    try:
        cursor.execute( """ create table demipt3.kzmn_stg_source_clients( 
							client_id varchar(8) ,
                            last_name varchar(20) ,
                            first_name varchar(20) ,
                            patronymic varchar(20) ,
                            date_of_birth date ,
                            passport_num char(11) ,
                            passport_valid_to date ,
                            phone char(16) ,
                            dt timestamp(0) ) ; """ )
        print('created table demipt3.kzmn_stg_source_clients')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание target-таблицы (сущности "clients")
    try:
        cursor.execute( """ create table demipt3.kzmn_dwh_dim_clients_hist(
                            client_id varchar(8) ,
                            last_name varchar(20) ,
                            first_name varchar(20) ,
                            patronymic varchar(20) ,
                            date_of_birth date ,
                            passport_num char(11) ,
                            passport_valid_to date ,
                            phone char(16) ,
                            effective_from timestamp(0) ,
                            effective_to timestamp(0) ,
                            deleted_flg char(1) ) ; """ )
        print('created table demipt3.kzmn_dwh_dim_clients_hist')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание stg-таблицы
    try:
        cursor.execute( """ create table demipt3.kzmn_stg_clients (
                            client_id varchar(8) ,
                            last_name varchar(20) ,
                            first_name varchar(20) ,
                            patronymic varchar(20) ,
                            date_of_birth date ,
                            passport_num char(11) ,
                            passport_valid_to date ,
                            phone char(16) ,
                            update_dt timestamp(0) ) ; """ )
        print('created table demipt3.kzmn_stg_clients')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание meta_stg_del-таблицы
    try:
        cursor.execute( """ create table demipt3.kzmn_stg_del_clients (
                            client_id char(8) ) ; """ )
        print('created table demipt3.kzmn_stg_del_clients')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание meta-таблицы
    try:
        cursor.execute( """ create table demipt3.kzmn_meta_clients (
                            schema_name varchar(20) ,
                            table_name varchar(60) ,
                            last_update_dt timestamp(0) ) ; """ )
        cursor.execute( """ insert into demipt3.kzmn_meta_clients
                                ( schema_name, table_name, last_update_dt )
                            values
                                ( 'demipt3','kzmn_stg_source_clients', to_timestamp('1900-01-01','YYYY-MM-DD') ) ; """ )
        print('created table demipt3.kzmn_meta_clients')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()
    conn.commit()  # необходимо зафиксировать транзакцию в любом состоянии

    return 1


def accounts_entity( conn, cursor ):
    # Создание в хранилище сущности "accounts" :

    # Создание искусственного источника (сущности "accounts")
    try:
        cursor.execute( """ create table demipt3.kzmn_stg_source_accounts( 
							account_num varchar(20) ,
                            valid_to date ,
                            client varchar(8) ,
                            update_dt timestamp(0) ) ; """ )
        print('created table demipt3.kzmn_stg_source_accounts')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание target-таблицы (сущности "accounts")
    try:
        cursor.execute( """ create table demipt3.kzmn_dwh_dim_accounts (
							account_num varchar(20) ,
                            valid_to date ,
                            client varchar(8) ,
							create_dt timestamp(0) ,
							update_dt timestamp(0) ) ; """ )
        print('created table demipt3.kzmn_dwh_dim_accounts')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание stg-таблицы
    try:
        cursor.execute( """ create table demipt3.kzmn_stg_accounts( 
							account_num varchar(20) ,
                            valid_to date ,
                            client varchar(8) ,
							update_dt timestamp(0) ) ; """ )
        print('created table demipt3.kzmn_stg_accounts')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание stg_del-таблицы
    try:
        cursor.execute( """ create table demipt3.kzmn_stg_del_accounts ( 
							account_num varchar(20) ) ; """ )
        print('created table demipt3.kzmn_stg_del_accounts')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание meta-таблицы
    try:
        cursor.execute( """ create table demipt3.kzmn_meta_accounts (
                            schema_name varchar(20) ,
                            table_name varchar(60) ,
                            max_update_dt timestamp(0) ) ; """ )
        cursor.execute( """ insert into demipt3.kzmn_meta_accounts
                                ( schema_name, table_name, max_update_dt )
                            values
                                ( 'demipt3','kzmn_stg_source_accounts', to_timestamp('1900-01-01','YYYY-MM-DD') ) ; """ )
        print('created table demipt3.kzmn_meta_accounts')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()
    conn.commit()  # необходимо зафиксировать транзакцию в любом состоянии

    return 1


def cards_entity( conn, cursor ):
    # Создание в хранилище сущности "cards" :

    # Создание искусственного источника (сущности "cards")
    try:
        cursor.execute( """ create table demipt3.kzmn_stg_source_cards( 
							card_num char(19) ,
							account_num varchar(20) ,
                            update_dt timestamp(0) ) ; """ )
        print('created table demipt3.kzmn_stg_source_cards')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание target-таблицы (сущности "cards")
    try:
        cursor.execute( """ create table demipt3.kzmn_dwh_dim_cards (
							card_num char(19) ,
							account_num varchar(20) ,
							create_dt timestamp(0) ,
							update_dt timestamp(0) ) ; """ )
        print('created table demipt3.kzmn_dwh_dim_cards')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание stg-таблицы
    try:
        cursor.execute( """ create table demipt3.kzmn_stg_cards( 
							card_num char(19) ,
							account_num varchar(20) ,
							update_dt timestamp(0) ) ; """ )
        print('created table demipt3.kzmn_stg_cards')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание stg_del-таблицы
    try:
        cursor.execute( """ create table demipt3.kzmn_stg_del_cards ( 
							card_num char(19) ) ; """ )
        print('created table demipt3.kzmn_stg_del_cards')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание meta-таблицы
    try:
        cursor.execute( """ create table demipt3.kzmn_meta_cards (
                            schema_name varchar(20) ,
                            table_name varchar(60) ,
                            max_update_dt timestamp(0) ) ; """ )
        cursor.execute( """ insert into demipt3.kzmn_meta_cards
                                ( schema_name, table_name, max_update_dt )
                            values
                                ( 'demipt3','kzmn_stg_source_cards', to_timestamp('1900-01-01','YYYY-MM-DD') ) ; """ )
        print('created table demipt3.kzmn_meta_cards')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()
    conn.commit()  # необходимо зафиксировать транзакцию в любом состоянии

    return 1


def transactions_entity( conn, cursor ):
    # Создание в хранилище сущности "transactions" :

    # Создание stg-источника (сущности "transactions")
    try:
        cursor.execute( """ create table demipt3.kzmn_stg_transactions( 
                            trans_id char(11) , 
                            trans_date timestamp(0) ,
                            card_num varchar(19) ,
                            oper_type varchar(10) ,
                            amt decimal(10,2) ,
                            oper_result varchar(10) ,
                            terminal char(5) ) ; """ )
        print('created table demipt3.kzmn_stg_transactions')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание фактовой target-таблицы (сущности "transactions")
    try:
        cursor.execute( """ create table demipt3.kzmn_dwh_fact_transactions(
                            trans_id char(11) , 
                            trans_date timestamp(0) ,
                            card_num varchar(19) ,
                            oper_type varchar(10) ,
                            amt decimal(10,2) ,
                            oper_result varchar(10) ,
                            terminal char(5) ) ; """ )
        print('created table demipt3.kzmn_dwh_fact_transactions')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()
    conn.commit()  # необходимо зафиксировать транзакцию в любом состоянии

    return 1


def mart_tables(conn, cursor):
    # Создание в хранилище витрины "frauds" :

    # Создание stg-таблицы (сущности "frauds")
    try:
        cursor.execute( """ create table demipt3.kzmn_stg_rep_fraud (
							event_dt timestamp(0) ,
                            passport char(11) ,
                            fio varchar(100) ,
							phone char(16) ,
							event_type char(1) ,
							report_dt date ) ; """ )
        print('created table demipt3.kzmn_stg_rep_fraud')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()
    
    # Создание промежуточной таблицы (сущности "frauds", тип 4)
    try:
        cursor.execute( """ create table demipt3.kzmn_stg_add_rep_fraud(
                            trans_date timestamp(0) ,
                            card_num varchar(19) ) ; """ )
    except psycopg2.errors.DuplicateTable:
        conn.rollback()
    
    # Создание target-таблицы (сущности "frauds")
    try:
        cursor.execute( """ create table demipt3.kzmn_rep_fraud (
							event_dt timestamp(0) ,
                            passport char(11) ,
                            fio varchar(100) ,
							phone char(16) ,
							event_type char(1) ,
							report_dt date ) ; """ )
        print('created table demipt3.kzmn_rep_fraud')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()
    conn.commit()  # необходимо зафиксировать транзакцию в любом состоянии
    
    return 1


def passport_blacklist_fact_load( conn, cursor ):
    # Фактовая выгрузка сущности "passport_blacklist" :

    try:
        # 1. Загрузка новых значений из стейджинга
        cursor.execute( """ insert into demipt3.kzmn_dwh_fact_passport_blacklist
                                ( passport_num, entry_dt )
                                    select
                                        s.passport_num ,
                                        s.entry_dt 
                                    from demipt3.kzmn_stg_passport_blacklist s
                                    left join demipt3.kzmn_dwh_fact_passport_blacklist t 
                                    on s.passport_num = t.passport_num
                                    where t.passport_num is null ; """ )

        # 2. Дополнительная очистка стейджинга-источника
        cursor.execute( """ delete from demipt3.kzmn_stg_passport_blacklist ; """ )

        # 3. Фиксация транзакции
        #conn.commit()

    except Exception :
        conn.rollback()
        print( 'passport_blacklist_fact_load --> something went wrong' )

    return 1


def terminals_scd2_load( conn, cursor, date ):
    # Последовательная инкрементальная выгрузка данных сущности "terminals" :

    try:
        # 1. Очистка стейджинговых таблиц
        cursor.execute( """ delete from demipt3.kzmn_stg_terminals ; """ )
        cursor.execute( """ delete from demipt3.kzmn_stg_del_terminals ; """ )

        # 2. Захват данных из источника (измененных с момента последней загрузки) в стейджинг
        cursor.execute( """ insert into demipt3.kzmn_stg_terminals
                                ( terminal_id, terminal_type, terminal_city, terminal_address, update_dt  )
                                    select
                                        terminal_id,
                                        terminal_type,
                                        terminal_city,
                                        terminal_address,
                                        dt
                                    from demipt3.kzmn_stg_source_terminals
                                    where dt > ( select last_update_dt
                                                from demipt3.kzmn_meta_terminals
                                                where schema_name='demipt3'
                                                    and table_name='kzmn_stg_source_terminals' ) ; """ )

        # 3. Захват полного среза ключей для вычисления удалений
        cursor.execute( """ insert into demipt3.kzmn_stg_del_terminals
                                ( terminal_id )
                                    select
                                        terminal_id
                                    from demipt3.kzmn_stg_source_terminals ; """ )

        # 4. Загрузка в приемник "вставок" на источнике (формат SCD2)
        cursor.execute( """ insert into demipt3.kzmn_dwh_dim_terminals_hist
                                ( terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg )
                                    select
                                        s.terminal_id ,
                                        s.terminal_type ,
                                        s.terminal_city ,
                                        s.terminal_address ,
                                        s.update_dt ,
                                        coalesce(cast (lead ( s.update_dt) over ( partition by s.terminal_id order by s.update_dt ) - interval '1 day' as date), '9999-12-31' ) , 
                                        'N'
                                    from demipt3.kzmn_stg_terminals s
                                    left join demipt3.kzmn_dwh_dim_terminals_hist t 
                                    on s.terminal_id = t.terminal_id
                                    where t.terminal_id is null ; """ )

        # 5. Обновление в приемнике "обновлений" на источнике (формат SCD2, с учетом фиктивных обновлений)
        cursor.execute( """ insert into demipt3.kzmn_dwh_dim_terminals_hist
                                ( terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg ) 
                                    select distinct 
                                        s.terminal_id ,
                                        s.terminal_type ,
                                        s.terminal_city ,
                                        s.terminal_address ,
                                        s.update_dt ,
                                        cast(null as timestamp),
                                        'N'
                                    from demipt3.kzmn_stg_terminals s
                                    left join demipt3.kzmn_dwh_dim_terminals_hist t
                                        on s.terminal_id = t.terminal_id
                                    where 1=0
                                    -- terminal_type 
                                    or ( ( s.terminal_type <> t.terminal_type or (s.terminal_type is null and t.terminal_type is not null) or (s.terminal_type is not null and t.terminal_type is null) )
                                        and s.terminal_type not in ( select t.terminal_type from demipt3.kzmn_stg_terminals s inner join demipt3.kzmn_dwh_dim_terminals_hist t on s.terminal_id = t.terminal_id ) )
                                    -- terminal_city 
                                    or ( ( s.terminal_city <> t.terminal_city or (s.terminal_city is null and t.terminal_city is not null) or (s.terminal_city is not null and t.terminal_city is null) )
                                        and s.terminal_city not in ( select t.terminal_city from demipt3.kzmn_stg_terminals s inner join demipt3.kzmn_dwh_dim_terminals_hist t on s.terminal_id = t.terminal_id ) )
                                    -- terminal_address 
                                    or ( ( s.terminal_address <> t.terminal_address or (s.terminal_address is null and t.terminal_address is not null) or (s.terminal_address is not null and t.terminal_address is null) )
                                        and s.terminal_address not in ( select t.terminal_address from demipt3.kzmn_stg_terminals s inner join demipt3.kzmn_dwh_dim_terminals_hist t on s.terminal_id = t.terminal_id ) ) ; """ )
        cursor.execute( """ update demipt3.kzmn_dwh_dim_terminals_hist
                            set 
                                effective_to = ttemp.effective_to
                            from (
                                select
                                    a.terminal_id ,
                                    a.terminal_type ,
                                    a.terminal_city ,
                                    a.terminal_address ,
                                    coalesce((lead ( a.effective_from ) over ( partition by a.terminal_id order by a.effective_from ) - interval '1 day' ), '9999-12-31' ) as effective_to 
                                from demipt3.kzmn_dwh_dim_terminals_hist a
                                inner join demipt3.kzmn_dwh_dim_terminals_hist b
                                    on a.terminal_id = b.terminal_id
                                    and b.effective_to is null
                            ) ttemp
                        where ( kzmn_dwh_dim_terminals_hist.terminal_id = ttemp.terminal_id
                                and kzmn_dwh_dim_terminals_hist.terminal_type = ttemp.terminal_type
                                and kzmn_dwh_dim_terminals_hist.terminal_city = ttemp.terminal_city
                                and kzmn_dwh_dim_terminals_hist.terminal_address = ttemp.terminal_address ) ; """ )

        # 6. Удаление удаленных записей (формат SCD2)
        cursor.execute( """ insert into demipt3.kzmn_dwh_dim_terminals_hist
                                ( terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg )
                                    select 
                                        terminal_id ,
                                        terminal_type ,
                                        terminal_city ,
                                        terminal_address ,
                                        cast( now() as timestamp ) ,
                                        cast( null as timestamp ) , 
                                        'Y' 
                                    from demipt3.kzmn_dwh_dim_terminals_hist
                                    where terminal_id in (
                                        select
                                            t.terminal_id
                                        from demipt3.kzmn_dwh_dim_terminals_hist t 
                                        left join demipt3.kzmn_stg_del_terminals s
                                            on t.terminal_id = s.terminal_id
                                            where s.terminal_id is null
                                            )
                                        and effective_to = '9999-12-31'
                                        and deleted_flg = 'N' ;  """ )

        cursor.execute( """ update demipt3.kzmn_dwh_dim_terminals_hist
                            set 
                                effective_to = ttemp.effective_to
                            from (
                                select
                                terminal_id ,
                                terminal_type ,
                                terminal_city ,
                                terminal_address ,
                                effective_to
                                from (	select
                                        a.terminal_id ,
                                        a.terminal_type ,
                                        a.terminal_city ,
                                        a.terminal_address ,
                                        a.effective_to as dt ,
                                        coalesce((lead ( a.effective_from ) over ( partition by a.terminal_id order by a.effective_from ) - interval '1 day' ), '9999-12-31' ) as effective_to
                                        from demipt3.kzmn_dwh_dim_terminals_hist a
                                        inner join demipt3.kzmn_dwh_dim_terminals_hist b
                                            on a.terminal_id = b.terminal_id
                                            and b.effective_to is null ) sq
                                where dt = '9999-12-31'
                            ) ttemp
                        where kzmn_dwh_dim_terminals_hist.terminal_id = ttemp.terminal_id
                            and ( kzmn_dwh_dim_terminals_hist.terminal_type = ttemp.terminal_type and kzmn_dwh_dim_terminals_hist.terminal_city = ttemp.terminal_city and kzmn_dwh_dim_terminals_hist.terminal_address = ttemp.terminal_address )
                                and kzmn_dwh_dim_terminals_hist.effective_to  = '9999-12-31' ; """ )
        cursor.execute( """ update demipt3.kzmn_dwh_dim_terminals_hist
                            set 
                                effective_to = ttemp.effective_to
                            from (
                                select
                                    terminal_id ,
                                    terminal_type ,
                                    terminal_city ,
                                    terminal_address ,
                                    effective_from ,
                                    coalesce(effective_to, '9999-12-31') as effective_to ,
                                    deleted_flg
                                from demipt3.kzmn_dwh_dim_terminals_hist
                                where deleted_flg = 'Y'
                            ) ttemp
                        where kzmn_dwh_dim_terminals_hist.terminal_id = ttemp.terminal_id
                            and ( kzmn_dwh_dim_terminals_hist.terminal_type = ttemp.terminal_type and kzmn_dwh_dim_terminals_hist.terminal_city = ttemp.terminal_city and kzmn_dwh_dim_terminals_hist.terminal_address = ttemp.terminal_address )
                                and kzmn_dwh_dim_terminals_hist.effective_to is null ; """ )

        # 7. Обновление метаданных
        cursor.execute( """ update demipt3.kzmn_meta_terminals
                                set last_update_dt = coalesce( ( select max(update_dt)
                                                                from demipt3.kzmn_stg_terminals ),
                                                                    ( select last_update_dt
                                                                    from demipt3.kzmn_meta_terminals
                                                                    where schema_name='demipt3'
                                                                        and table_name='kzmn_stg_source_terminals' ) )
                                where schema_name='demipt3'
                                    and table_name='kzmn_stg_source_terminals' ; """ )

        # 8. Фиксация транзакции
        #conn.commit()

        # 9. Дополнительная (необязательная) очистка источника
        cursor.execute( """ delete from demipt3.kzmn_stg_source_terminals ; """ )
        #conn.commit()

    except Exception :
        conn.rollback()
        print( 'terminals_scd2_load --> something went wrong' )

    return 1


def clients_scd2_load( conn, cursor ):
    # Последовательная инкрементальная выгрузка данных сущности "clients" :

    try:
        # 1. Очистка стейджинговых таблиц
        cursor.execute( """ delete from demipt3.kzmn_stg_clients ; """ )
        cursor.execute( """ delete from demipt3.kzmn_stg_del_clients ; """ )

        # 2. Захват данных из источника (измененных с момента последней загрузки) в стейджинг
        cursor.execute( """ insert into demipt3.kzmn_stg_clients
                                ( client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, update_dt  )
                                    select
                                        client_id,
                                        last_name,
                                        first_name,
                                        patronymic,
                                        date_of_birth,
                                        passport_num,
                                        passport_valid_to,
                                        phone,
                                        dt
                                    from demipt3.kzmn_stg_source_clients
                                    where dt > ( select last_update_dt
                                                from demipt3.kzmn_meta_clients
                                                where schema_name='demipt3'
                                                    and table_name='kzmn_stg_source_clients' ) ; """ )

        # 3. Захват полного среза ключей для вычисления удалений
        cursor.execute( """ insert into demipt3.kzmn_stg_del_clients
                                ( client_id )
                                    select
                                        client_id
                                    from demipt3.kzmn_stg_source_clients ; """ )

        # 4. Загрузка в приемник "вставок" на источнике (формат SCD2)
        cursor.execute( """ insert into demipt3.kzmn_dwh_dim_clients_hist
                                ( client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, effective_from, effective_to, deleted_flg )
                                    select
                                        s.client_id ,
                                        s.last_name ,
                                        s.first_name ,
                                        s.patronymic ,
                                        s.date_of_birth ,
                                        s.passport_num ,
                                        s.passport_valid_to ,
                                        s.phone ,
                                        s.update_dt ,
                                        coalesce(cast (lead ( s.update_dt) over ( partition by s.client_id order by s.update_dt ) - interval '1 day' as date), '9999-12-31' ) , 
                                        'N'
                                    from demipt3.kzmn_stg_clients s
                                    left join demipt3.kzmn_dwh_dim_clients_hist t 
                                    on s.client_id = t.client_id
                                    where t.client_id is null ; """ )

        # 5. Обновление в приемнике "обновлений" на источнике (формат SCD2, без учета фиктивных обновлений)
        cursor.execute( """ insert into demipt3.kzmn_dwh_dim_clients_hist
                                ( client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, effective_from, effective_to, deleted_flg )
                                    select distinct
                                        s.client_id ,
                                        s.last_name ,
                                        s.first_name ,
                                        s.patronymic ,
                                        s.date_of_birth ,
                                        s.passport_num ,
                                        s.passport_valid_to ,
                                        s.phone ,
                                        s.update_dt ,
                                        cast(null as timestamp), 
                                        'N' 
                                    from demipt3.kzmn_stg_clients s
                                    left join demipt3.kzmn_dwh_dim_clients_hist t 
                                    on s.client_id = t.client_id
                                    where 1=0
                                    or ( s.last_name <> t.last_name or (s.last_name is null and t.last_name is not null) or (s.last_name is not null and t.last_name is null) )
                                    or ( s.first_name <> t.first_name or (s.first_name is null and t.first_name is not null) or (s.first_name is not null and t.first_name is null) )
                                    or ( s.patronymic <> t.patronymic or (s.patronymic is null and t.patronymic is not null) or (s.patronymic is not null and t.patronymic is null) ) 
                                    or ( s.date_of_birth <> t.date_of_birth or (s.date_of_birth is null and t.date_of_birth is not null) or (s.date_of_birth is not null and t.date_of_birth is null) ) 
                                    or ( s.passport_num <> t.passport_num or (s.passport_num is null and t.passport_num is not null) or (s.passport_num is not null and t.passport_num is null) ) 
                                    or ( s.passport_valid_to <> t.passport_valid_to or (s.passport_valid_to is null and t.passport_valid_to is not null) or (s.passport_valid_to is not null and t.passport_valid_to is null) )
                                    or ( s.phone <> t.phone or (s.phone is null and t.phone is not null) or (s.phone is not null and t.phone is null) ) ; """ )
        cursor.execute( """ update demipt3.kzmn_dwh_dim_clients_hist
                            set 
                                effective_to = ttemp.effective_to
                            from (
                                select
                                    a.client_id ,
                                    a.last_name ,
                                    a.first_name ,
                                    a.patronymic ,
                                    a.date_of_birth ,
                                    a.passport_num ,
                                    a.passport_valid_to ,
                                    a.phone ,
                                    coalesce((lead ( a.effective_from ) over ( partition by a.client_id order by a.effective_from ) - interval '1 day' ), '9999-12-31' ) as effective_to 
                                from demipt3.kzmn_dwh_dim_clients_hist a
                                inner join demipt3.kzmn_dwh_dim_clients_hist b
                                    on a.client_id = b.client_id
                                    and b.effective_to is null
                            ) ttemp
                        where ( kzmn_dwh_dim_clients_hist.client_id = ttemp.client_id
                                and kzmn_dwh_dim_clients_hist.last_name = ttemp.last_name
                                and kzmn_dwh_dim_clients_hist.first_name = ttemp.first_name
                                and kzmn_dwh_dim_clients_hist.patronymic = ttemp.patronymic  -- "patronymic не может быть налом"
                                and kzmn_dwh_dim_clients_hist.date_of_birth = ttemp.date_of_birth
                                and kzmn_dwh_dim_clients_hist.passport_num = ttemp.passport_num
                                and coalesce( kzmn_dwh_dim_clients_hist.passport_valid_to, '9999-12-31' ) = coalesce( ttemp.passport_valid_to, '9999-12-31' ) 
                                and kzmn_dwh_dim_clients_hist.phone = ttemp.phone ) ; """ )

        # 6. Удаление удаленных записей (формат SCD2)
        cursor.execute( """ insert into demipt3.kzmn_dwh_dim_clients_hist
                                ( client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, effective_from, effective_to, deleted_flg )
                                    select 
                                        client_id ,
                                        last_name ,
                                        first_name ,
                                        patronymic ,
                                        date_of_birth ,
                                        passport_num ,
                                        passport_valid_to ,
                                        phone ,
                                        cast( now() as timestamp ) ,
                                        cast( null as timestamp ) , 
                                        'Y' 
                                    from demipt3.kzmn_dwh_dim_clients_hist
                                    where client_id in (
                                        select
                                            t.client_id
                                        from demipt3.kzmn_dwh_dim_clients_hist t 
                                        left join demipt3.kzmn_stg_del_clients s
                                            on t.client_id = s.client_id
                                            where s.client_id is null
                                            )
                                        and effective_to = '9999-12-31'
                                        and deleted_flg = 'N' ;  """ )

        cursor.execute( """ update demipt3.kzmn_dwh_dim_clients_hist
                            set 
                                effective_to = ttemp.effective_to
                            from (
                                select
                                    client_id ,
                                    last_name ,
                                    first_name ,
                                    patronymic ,
                                    date_of_birth ,
                                    passport_num ,
                                    passport_valid_to ,
                                    phone ,
                                    effective_to
                                from (	select
                                        a.client_id ,
                                        a.last_name ,
                                        a.first_name ,
                                        a.patronymic ,
                                        a.date_of_birth ,
                                        a.passport_num ,
                                        a.passport_valid_to ,
                                        a.phone ,
                                        a.effective_from ,
                                        a.effective_to as dt,
                                        coalesce((lead ( a.effective_from ) over ( partition by a.client_id order by a.effective_from ) - interval '1 day' ), '9999-12-31' ) as effective_to
                                        from demipt3.kzmn_dwh_dim_clients_hist a
                                        inner join demipt3.kzmn_dwh_dim_clients_hist b
                                            on a.client_id = b.client_id
                                            and b.effective_to is null ) sq
                                where dt = '9999-12-31'
                            ) ttemp
                        where kzmn_dwh_dim_clients_hist.client_id = ttemp.client_id
                            and ( kzmn_dwh_dim_clients_hist.last_name = ttemp.last_name
                                and kzmn_dwh_dim_clients_hist.first_name = ttemp.first_name
                                and kzmn_dwh_dim_clients_hist.patronymic = ttemp.patronymic  -- "patronymic не может быть налом"
                                and kzmn_dwh_dim_clients_hist.date_of_birth = ttemp.date_of_birth
                                and kzmn_dwh_dim_clients_hist.passport_num = ttemp.passport_num
                                and coalesce( kzmn_dwh_dim_clients_hist.passport_valid_to, '9999-12-31' ) = coalesce( ttemp.passport_valid_to, '9999-12-31' ) 
                                and kzmn_dwh_dim_clients_hist.phone = ttemp.phone )
                                    and kzmn_dwh_dim_clients_hist.effective_to = '9999-12-31' ; """ )
        cursor.execute( """ update demipt3.kzmn_dwh_dim_clients_hist
                            set 
                                effective_to = ttemp.effective_to
                            from (
                                select
                                    client_id ,
                                    last_name ,
                                    first_name ,
                                    patronymic ,
                                    date_of_birth ,
                                    passport_num ,
                                    passport_valid_to ,
                                    phone ,
                                    effective_from ,
                                    coalesce(effective_to, '9999-12-31') as effective_to,
                                    deleted_flg
                                from demipt3.kzmn_dwh_dim_clients_hist
                                where deleted_flg = 'Y'
                            ) ttemp
                        where kzmn_dwh_dim_clients_hist.client_id = ttemp.client_id
                            and ( kzmn_dwh_dim_clients_hist.last_name = ttemp.last_name
                                and kzmn_dwh_dim_clients_hist.first_name = ttemp.first_name
                                and kzmn_dwh_dim_clients_hist.patronymic = ttemp.patronymic  -- "patronymic не может быть налом"
                                and kzmn_dwh_dim_clients_hist.date_of_birth = ttemp.date_of_birth
                                and kzmn_dwh_dim_clients_hist.passport_num = ttemp.passport_num
                                and coalesce( kzmn_dwh_dim_clients_hist.passport_valid_to, '9999-12-31' ) = coalesce( ttemp.passport_valid_to, '9999-12-31' ) 
                                and kzmn_dwh_dim_clients_hist.phone = ttemp.phone )
                                    and kzmn_dwh_dim_clients_hist.effective_to is null ; """ )

        # 7. Обновление метаданных
        cursor.execute( """ update demipt3.kzmn_meta_clients
                                set last_update_dt = coalesce( ( select max(update_dt)
                                                                from demipt3.kzmn_stg_clients ),
                                                                    ( select last_update_dt
                                                                    from demipt3.kzmn_meta_clients
                                                                    where schema_name='demipt3'
                                                                        and table_name='kzmn_stg_source_clients' ) )
                                where schema_name='demipt3'
                                    and table_name='kzmn_stg_source_clients'; """ )

        # 8. Дополнительная очистка stg и meta_stg_del ( поскольку изменения в сущности редки, а объем и ценность данных значительны )
        cursor.execute( """ delete from demipt3.kzmn_stg_clients ; """ )
        cursor.execute( """ delete from demipt3.kzmn_stg_del_clients ; """ )

        # 9. Дополнительная очистка источника (для экономии места)
        cursor.execute( """ delete from demipt3.kzmn_stg_source_clients ; """ )

        # 10. Фиксация транзакции
        conn.commit()

    except Exception :
        conn.rollback()
        print( 'clients_scd2_load --> something went wrong' )

    return 1


def accounts_scd1_load( conn, cursor ):
    # Последовательная инкрементальная выгрузка данных сущности "accounts" :

    try:
        # 1. Очистка стейджинговых таблиц
        cursor.execute( """ delete from demipt3.kzmn_stg_accounts ; """ )
        cursor.execute( """ delete from demipt3.kzmn_stg_del_accounts ; """ )

        # 2. Захват данных из источника (измененных с момента последней загрузки) в стейджинг
        cursor.execute( """ insert into demipt3.kzmn_stg_accounts
                                ( account_num, valid_to, client, update_dt )
                                    select
                                        account_num,
                                        valid_to,
                                        client,
                                        update_dt
                                    from demipt3.kzmn_stg_source_accounts
                                    where update_dt > ( select max_update_dt
                                                        from demipt3.kzmn_meta_accounts
                                                        where schema_name='demipt3'
                                                            and table_name='kzmn_stg_source_accounts' ) ; """ )

        # 3. Захват полного среза ключей для вычисления удалений
        cursor.execute( """ insert into demipt3.kzmn_stg_del_accounts
                                ( account_num )
                                    select
                                        account_num
                                    from demipt3.kzmn_stg_source_accounts ; """ )

        # 4. Загрузка в приемник "вставок" на источнике (формат SCD1)
        cursor.execute( """ insert into demipt3.kzmn_dwh_dim_accounts
                                ( account_num, valid_to, client, create_dt, update_dt )
                                    select
                                        s.account_num ,
                                        s.valid_to ,
                                        s.client ,
                                        s.update_dt,
                                        null
                                    from demipt3.kzmn_stg_accounts s
                                    left join demipt3.kzmn_dwh_dim_accounts t 
                                    on s.account_num = t.account_num
                                    where t.account_num is null ; """ )

        # 5. Обновление в приемнике "обновлений" на источнике (формат SCD1)
        cursor.execute( """ update demipt3.kzmn_dwh_dim_accounts
                            set
                                valid_to = tmp.valid_to ,
                                client = tmp.client ,
                                update_dt = tmp.update_dt
                            from (
                                select
                                    s.account_num ,
                                    s.valid_to ,
                                    s.client ,
                                    s.update_dt,
                                    null
                                from demipt3.kzmn_stg_accounts s
                                inner join demipt3.kzmn_dwh_dim_accounts t 
                                on s.account_num = t.account_num
                                where 1=0
                                or ( s.valid_to <> t.valid_to or (s.valid_to is null and t.valid_to is not null) or (s.valid_to is not null and t.valid_to is null) )
                                or ( s.client <> t.client or (s.client is null and t.client is not null) or (s.client is not null and t.client is null) )
                            ) tmp
                            where kzmn_dwh_dim_accounts.account_num = tmp.account_num ; """ )

        # 6. Удаление удаленных записей (формат SCD1)
        cursor.execute( """ delete from demipt3.kzmn_dwh_dim_accounts 
                            where account_num in (
                                select t.account_num
                                from demipt3.kzmn_dwh_dim_accounts t
                                left join demipt3.kzmn_stg_del_accounts s
                                on t.account_num = s.account_num
                                where s.account_num is null ) ;  """ )

        # 7. Обновление метаданных.
        cursor.execute( """ update demipt3.kzmn_meta_accounts
                                set max_update_dt = coalesce( ( select max(update_dt)
                                                                from demipt3.kzmn_stg_accounts ),
                                                                    ( select max_update_dt
                                                                    from demipt3.kzmn_meta_accounts
                                                                    where schema_name='demipt3'
                                                                        and table_name='kzmn_stg_source_accounts' ) )
                                where schema_name='demipt3'
                                    and table_name='kzmn_stg_source_accounts'; """ )

        # 8. Дополнительная очистка stg и meta_stg_del ( поскольку изменения в сущности редки, а объем и ценность данных значительны )
        cursor.execute( """ delete from demipt3.kzmn_stg_accounts ; """ )
        cursor.execute( """ delete from demipt3.kzmn_stg_del_accounts ; """ )

        # 9. Дополнительная очистка источника (для экономии места)
        cursor.execute( """ delete from demipt3.kzmn_stg_source_accounts ; """ )

        # 10. Фиксация транзакции
        conn.commit()

    except Exception :
        conn.rollback()
        print( 'account_scd1_load --> something went wrong' )

    return 1


def cards_scd1_load( conn, cursor ):
    # Последовательная инкрементальная выгрузка данных сущности "accounts" :

    try:
        # 1. Очистка стейджинговых таблиц
        cursor.execute( """ delete from demipt3.kzmn_stg_cards ; """ )
        cursor.execute( """ delete from demipt3.kzmn_stg_del_cards ; """ )

        # 2. Захват данных из источника (измененных с момента последней загрузки) в стейджинг
        cursor.execute( """ insert into demipt3.kzmn_stg_cards
                                ( card_num, account_num, update_dt )
                                    select
                                        card_num,
                                        account_num,
                                        update_dt
                                    from demipt3.kzmn_stg_source_cards
                                    where update_dt > ( select max_update_dt
                                                        from demipt3.kzmn_meta_cards
                                                        where schema_name='demipt3'
                                                            and table_name='kzmn_stg_source_cards' ) ; """ )

        # 3. Захват полного среза ключей для вычисления удалений
        cursor.execute( """ insert into demipt3.kzmn_stg_del_cards
                                ( card_num )
                                    select
                                        card_num
                                    from demipt3.kzmn_stg_source_cards ; """ )

        # 4. Загрузка в приемник "вставок" на источнике (формат SCD1)
        cursor.execute( """ insert into demipt3.kzmn_dwh_dim_cards
                                ( card_num, account_num, create_dt, update_dt )
                                    select
                                        s.card_num ,
                                        s.account_num ,
                                        s.update_dt,
                                        null
                                    from demipt3.kzmn_stg_cards s
                                    left join demipt3.kzmn_dwh_dim_cards t 
                                    on s.card_num = t.card_num
                                    where t.card_num is null ; """ )

        # 5. Обновление в приемнике "обновлений" на источнике (формат SCD1)
        cursor.execute( """ update demipt3.kzmn_dwh_dim_cards
                            set
                                account_num = tmp.account_num ,
                                update_dt = tmp.update_dt
                            from (
                                select
                                    s.card_num ,
                                    s.account_num ,
                                    s.update_dt,
                                    null
                                from demipt3.kzmn_stg_cards s
                                inner join demipt3.kzmn_dwh_dim_cards t 
                                on s.card_num = t.card_num
                                where 1=0
                                or ( s.account_num <> t.account_num or (s.account_num is null and t.account_num is not null) or (s.account_num is not null and t.account_num is null) )
                            ) tmp
                            where kzmn_dwh_dim_cards.card_num = tmp.card_num ; """ )

        # 6. Удаление удаленных записей (формат SCD1)
        cursor.execute( """ delete from demipt3.kzmn_dwh_dim_cards
                            where card_num in (
                                select t.card_num
                                from demipt3.kzmn_dwh_dim_cards t
                                left join demipt3.kzmn_stg_del_cards s
                                on t.card_num = s.card_num
                                where s.card_num is null ) ;  """ )

        # 7. Обновление метаданных
        cursor.execute( """ update demipt3.kzmn_meta_cards
                                set max_update_dt = coalesce( ( select max(update_dt)
                                                                from demipt3.kzmn_stg_cards ),
                                                                    ( select max_update_dt
                                                                    from demipt3.kzmn_meta_cards
                                                                    where schema_name='demipt3'
                                                                        and table_name='kzmn_stg_source_cards' ) )
                                where schema_name='demipt3'
                                    and table_name='kzmn_stg_source_cards'; """ )

        # 8. Дополнительная очистка stg и meta_stg_del ( поскольку изменения в сущности редки, а объем и ценность данных значительны )
        cursor.execute( """ delete from demipt3.kzmn_stg_cards ; """ )
        cursor.execute( """ delete from demipt3.kzmn_stg_del_cards ; """ )

        # 9. Дополнительная очистка источника (для экономии места)
        cursor.execute( """ delete from demipt3.kzmn_stg_source_cards ; """ )

        # 10. Фиксация транзакции
        conn.commit()

    except Exception :
        conn.rollback()
        print( 'cards_scd1_load --> something went wrong' )

    return 1


def transactions_fact_load( conn, cursor ):
    # Фактовая выгрузка сущности "transactions" :

    try:
        # 1. Загрузка новых значений из стейджинга
        cursor.execute( """ insert into demipt3.kzmn_dwh_fact_transactions
                                ( trans_id, trans_date, card_num, oper_type, amt, oper_result, terminal )
                                    select
                                        s.trans_id ,
                                        s.trans_date ,
                                        s.card_num ,
                                        s.oper_type ,
                                        s.amt ,
                                        s.oper_result ,
                                        s.terminal
                                    from demipt3.kzmn_stg_transactions s
                                    left join demipt3.kzmn_dwh_fact_transactions t 
                                    on s.trans_id = t.trans_id
                                    where t.trans_id is null ; """ )

        # 2. Дополнительная очистка стейджинга-источника
        cursor.execute( """ delete from demipt3.kzmn_stg_transactions ; """ )

        # 3. Фиксация транзакции
        #conn.commit()

    except Exception :
        conn.rollback()
        print( 'transactions_fact_load --> something went wrong' )

    return 1


def mart_rep_fraud1_load( conn, cursor ):
    # Инкрементальная загрузка витрины rep_fraud (тип мошенничеств -- 1):

    try:
        # 1. Формирование стейджинга
        cursor.execute( """ insert into demipt3.kzmn_stg_rep_fraud
                                    ( event_dt, passport, fio, phone, event_type, report_dt )
                                        select
                                            t.trans_date as event_dt , 
                                            cl.passport_num as passport ,
                                            cl.last_name || ' ' || cl.first_name || ' ' || cl.patronymic as fio , 
                                            cl.phone ,
                                            '1' as event_type ,
                                            cast(t.trans_date as date) as report_dt
                                        from demipt3.kzmn_dwh_fact_transactions t
                                        inner join demipt3.kzmn_dwh_dim_cards c
                                        on t.card_num = c.card_num
                                        inner join demipt3.kzmn_dwh_dim_accounts a 
                                        on c.account_num = a.account_num
                                        inner join demipt3.kzmn_dwh_dim_clients_hist cl
                                        on a.client = cl.client_id 
                                        left join demipt3.kzmn_dwh_fact_passport_blacklist bl
                                        on cl.passport_num = bl.passport_num
                                        where
                                        -- валидность меньше даты транзакции и last_update_dt на момент формирования витрины
                                        ( coalesce(cl.passport_valid_to, '9999-12-31') < cast(t.trans_date as date) and coalesce(cl.passport_valid_to, '9999-12-31') < ( select min(last_update_dt) from demipt3.kzmn_meta_rep_fraud ) )
                                        or
                                        -- паспорт черный и дата внесения в чс больше или равна дате операции
                                        ( bl.passport_num is not null and cast(t.trans_date as date) >= bl.entry_dt ) ; """ )

        # 2. Загрузка новых значений из стейджинга
        cursor.execute( """ insert into demipt3.kzmn_rep_fraud
                                    ( event_dt, passport, fio, phone, event_type, report_dt )
                                        select
                                            s.event_dt ,
                                            s.passport ,
                                            s.fio ,
                                            s.phone ,
                                            s.event_type ,
                                            s.report_dt
                                        from demipt3.kzmn_stg_rep_fraud s
                                        left join demipt3.kzmn_rep_fraud t 
                                        on ( s.event_dt = t.event_dt and s.passport = t.passport )
                                        where ( t.event_dt is null and t.passport is null ) ; """ )

        # 3. Очистка стейджинга-источника
        cursor.execute( """ delete from demipt3.kzmn_stg_rep_fraud ; """ )

        # 4. Фиксация транзакции
        #conn.commit()

    except Exception :
        conn.rollback()
        print( 'mart_rep_fraud1_load --> something went wrong' )

    return 1


def mart_rep_fraud2_load( conn, cursor ):
    # Инкрементальная загрузка витрины rep_fraud (тип мошенничеств -- 2):

    try:
        # 1. Формирование стейджинга
        cursor.execute( """ insert into demipt3.kzmn_stg_rep_fraud
                                    ( event_dt, passport, fio, phone, event_type, report_dt )
                                        select
                                            t.trans_date as event_dt , 
                                            cl.passport_num as passport ,
                                            cl.last_name || ' ' || cl.first_name || ' ' || cl.patronymic as fio , 
                                            cl.phone ,
                                            '2' as event_type ,
                                            cast(t.trans_date as date) as report_dt
                                        from demipt3.kzmn_dwh_fact_transactions t
                                        inner join demipt3.kzmn_dwh_dim_cards c
                                        on t.card_num = c.card_num
                                        inner join demipt3.kzmn_dwh_dim_accounts a 
                                        on c.account_num = a.account_num
                                        inner join demipt3.kzmn_dwh_dim_clients_hist cl
                                        on a.client = cl.client_id 
                                        where a.valid_to <= ( cast(t.trans_date as date) )	-- "пусть будет включая" (про дату в accounts.valid_to как дату просроченного договора)
                                            and cast(t.trans_date as date) = ( select min(last_update_dt) from demipt3.kzmn_meta_rep_fraud ) ; """ )

        # 2. Загрузка новых значений из стейджинга
        cursor.execute( """ insert into demipt3.kzmn_rep_fraud
                                    ( event_dt, passport, fio, phone, event_type, report_dt )
                                        select
                                            s.event_dt ,
                                            s.passport ,
                                            s.fio ,
                                            s.phone ,
                                            s.event_type ,
                                            s.report_dt
                                        from demipt3.kzmn_stg_rep_fraud s
                                        left join demipt3.kzmn_rep_fraud t 
                                        on ( s.event_dt = t.event_dt and s.passport = t.passport )
                                        where ( t.event_dt is null and t.passport is null ) ; """ )

        # 3. Очистка стейджинга-источника
        cursor.execute( """ delete from demipt3.kzmn_stg_rep_fraud ; """ )

        # 4. Фиксация транзакции
        #conn.commit()

    except Exception :
        conn.rollback()
        print( 'mart_rep_fraud2_load --> something went wrong' )

    return 1


def mart_rep_fraud3_load( conn, cursor ):
    # Инкрементальная загрузка витрины rep_fraud (тип мошенничеств -- 1):

    try:
        # 1. Формирование стейджинга
        cursor.execute( """ insert into demipt3.kzmn_stg_rep_fraud
                                    ( event_dt, passport, fio, phone, event_type, report_dt )
                                        with join_tbl as (
                                            select 
                                                t.trans_date  ,
                                                t.card_num ,
                                                h.terminal_id ,
                                                h.terminal_city  
                                            from demipt3.kzmn_dwh_fact_transactions t
                                            inner join demipt3.kzmn_dwh_dim_terminals_hist h
                                            on t.terminal = h.terminal_id
                                            )
                                        ----
                                        select
                                            a.trans_date as event_dt , 
                                            cl.passport_num as passport ,
                                            cl.last_name || ' ' || cl.first_name || ' ' || cl.patronymic as fio , 
                                            cl.phone ,
                                            '3' as event_type ,
                                            cast(a.trans_date as date) as report_dt
                                        from join_tbl a
                                        inner join join_tbl b 
                                        on a.card_num = b.card_num
                                        and
                                            ( extract( year from a.trans_date ) = extract( year from b.trans_date )
                                            and extract( month from a.trans_date ) = extract( month from b.trans_date )
                                            and extract( day from a.trans_date ) = extract( day from b.trans_date )
                                            and ( a.trans_date - b.trans_date <= interval '60 minutes' and a.trans_date - b.trans_date >= -(interval '60 minutes') )
                                            and a.trans_date <> b.trans_date )
                                        --
                                        inner join demipt3.kzmn_dwh_dim_cards c
                                        on a.card_num = c.card_num
                                        inner join demipt3.kzmn_dwh_dim_accounts ac 
                                        on c.account_num = ac.account_num
                                        inner join demipt3.kzmn_dwh_dim_clients_hist cl
                                        on ac.client = cl.client_id 
                                        --
                                        where a.terminal_city <> b.terminal_city -- уже можно сортироваться по полученному результату
                                            and cast(a.trans_date as date) = ( select min(last_update_dt) from demipt3.kzmn_meta_rep_fraud ) ; """ )

        # 2. Загрузка новых значений из стейджинга
        cursor.execute( """ insert into demipt3.kzmn_rep_fraud
                                    ( event_dt, passport, fio, phone, event_type, report_dt )
                                        select
                                            s.event_dt ,
                                            s.passport ,
                                            s.fio ,
                                            s.phone ,
                                            s.event_type ,
                                            s.report_dt
                                        from demipt3.kzmn_stg_rep_fraud s
                                        left join demipt3.kzmn_rep_fraud t 
                                        on ( s.event_dt = t.event_dt and s.passport = t.passport )
                                        where ( t.event_dt is null and t.passport is null ) ; """ )

        # 3. Очистка стейджинга-источника
        cursor.execute( """ delete from demipt3.kzmn_stg_rep_fraud ; """ )

        # 4. Фиксация транзакции
        #conn.commit()

    except Exception :
        conn.rollback()
        print( 'mart_rep_fraud3_load --> something went wrong' )

    return 1


def mart_rep_fraud4_load( conn, cursor ):
    # Инкрементальная загрузка витрины rep_fraud (тип мошенничеств -- 1):

    try:
        # 1. Формирование промежуточной материализации (что повысило производительность скрипта)
        cursor.execute( """ insert into demipt3.kzmn_stg_add_rep_fraud
                                    ( trans_date, card_num )
                                        with transactions_tbl as (
                                        select
                                            trans_id ,
                                            trans_date ,
                                            card_num ,
                                            oper_result ,
                                            amt ,
                                            oper_type
                                        from demipt3.kzmn_dwh_fact_transactions
                                            )
                                         select 
                                              c.trans_date ,
                                              c.card_num 
                                        from transactions_tbl a
                                        inner join transactions_tbl b
                                        on a.card_num = b.card_num
                                            and (a.oper_result = 'REJECT'
                                                and b.oper_result = 'REJECT')
                                            and (( a.oper_type = 'WITHDRAW' or a.oper_type = 'PAYMENT' )
			                                    and ( b.oper_type = 'WITHDRAW' or b.oper_type = 'PAYMENT' ))
                                            and a.amt > b.amt
                                            and
												( extract( year from a.trans_date ) = extract( year from b.trans_date )
												and extract( month from a.trans_date ) = extract( month from b.trans_date )
												and extract( day from a.trans_date ) = extract( day from b.trans_date )
												and ( a.trans_date - b.trans_date <= interval '20 minutes' and a.trans_date - b.trans_date >= -(interval '20 minutes') )
												and a.trans_date <> b.trans_date )
                                        inner join transactions_tbl c 
                                        on b.card_num = c.card_num
                                            and c.oper_result = 'SUCCESS'
                                            and ( c.oper_type = 'WITHDRAW' or c.oper_type = 'PAYMENT' )
                                            and b.amt > c.amt
                                            and
                                                ( extract( year from b.trans_date ) = extract( year from c.trans_date )
                                                and extract( month from b.trans_date ) = extract( month from c.trans_date )
                                                and extract( day from b.trans_date ) = extract( day from c.trans_date )
                                                and ( b.trans_date - c.trans_date <= interval '20 minutes' and b.trans_date - c.trans_date >= -(interval '20 minutes') )
                                                and b.trans_date <> c.trans_date )
                                            and ( ( a.trans_date - c.trans_date <= interval '20 minutes' and a.trans_date - c.trans_date >= -(interval '20 minutes') )
                                            	and a.trans_date <> c.trans_date ) ;  """ )
        
        # 2. Формирование стейджинга
        cursor.execute( """ insert into demipt3.kzmn_stg_rep_fraud
                                    ( event_dt, passport, fio, phone, event_type, report_dt )
                                        select
                                            add.trans_date as event_dt , 
                                            cl.passport_num as passport ,
                                            cl.last_name || ' ' || cl.first_name || ' ' || cl.patronymic as fio , 
                                            cl.phone ,
                                            '4' as event_type ,
                                            cast(trans_date as date) as report_dt
                                        from demipt3.kzmn_stg_add_rep_fraud add
                                        inner join demipt3.kzmn_dwh_dim_cards c
                                        on add.card_num = c.card_num
                                        inner join demipt3.kzmn_dwh_dim_accounts ac 
                                        on c.account_num = ac.account_num
                                        inner join demipt3.kzmn_dwh_dim_clients_hist cl
                                        on ac.client = cl.client_id
                                            and cast(add.trans_date as date) = ( select min(last_update_dt) from demipt3.kzmn_meta_rep_fraud ) ; """ )

        # 3. Загрузка новых значений из стейджинга
        cursor.execute( """ insert into demipt3.kzmn_rep_fraud
                                    ( event_dt, passport, fio, phone, event_type, report_dt )
                                        select
                                            s.event_dt ,
                                            s.passport ,
                                            s.fio ,
                                            s.phone ,
                                            s.event_type ,
                                            s.report_dt
                                        from demipt3.kzmn_stg_rep_fraud s
                                        left join demipt3.kzmn_rep_fraud t 
                                        on ( s.event_dt = t.event_dt and s.passport = t.passport )
                                        where ( t.event_dt is null and t.passport is null ) ; """ )

        # 3. Очистка стейджинга-источника и промежуточной материализации
        cursor.execute( """ delete from demipt3.kzmn_stg_rep_fraud ; """ )
        cursor.execute( """ delete from demipt3.kzmn_stg_add_rep_fraud ; """ )

        # 4. Фиксация транзакции
        #conn.commit()

    except Exception :
        conn.rollback()
        print( 'mart_rep_fraud4_load --> something went wrong' )

    return 1


def auto_SCD2_noflg(conn, cursor):
    s = ', '

    table = str(input("         your table : "))
    partition_field = str(input("    partition field : "))

    name = input("""
  custom table name : yes / no
                    : """)
    if name == 'no':
        name = f"{table}_scd2"
    elif name == 'yes':
        name = str(input("      input the name: "))


    cursor.execute( f" SELECT * FROM {table} " )
    titles = [ x[0] for x in cursor.description ]
    selected = []
    for title in titles:
        if ('end' in title and 'd' in title) or ('end' in title and 'dt' in title) or ('end' in title and 'date' in title) :
            end_date_field = title
        elif ('date' in title or 'dt' in title) or ('date' in title and 'start' in title) or ('dt' in title and 'start' in title) or ('d' in title and 'start' in title) :
            start_date_field = title
        else:
            selected.append(title)
    selected = s.join(selected)

    try:
        cursor.execute(f"""
                        CREATE TABLE {name} AS
                            SELECT {selected},
                                    {start_date_field} AS start_dt,
                                    COALESCE ( CAST (LEAD ({start_date_field}) OVER ( PARTITION BY {partition_field} ORDER BY {start_date_field}) - INTERVAL '1 day' AS DATE ), '9999-01-01') AS end_dt
                            FROM {table}
                            ORDER BY {partition_field}, {start_date_field}
                        """)
        conn.commit()
    except psycopg2.errors.DuplicateTable:
        print(f"""
table {name} already exists
""")
        conn.rollback()
    return 1


def auto_SCD2_flg(conn, cursor):
    ...


def hist_group(conn, cursor):  # made, not tested; check --> 'auto SCD2.py'
    ...


def initial_load_SCD2(conn, cursor):  # for one partition only
    s = ', '
    table = str(input("         your table : "))
    partition_field = str(input("    partition field : "))

    name = input("""
  custom table name : yes / no
                    : """)
    if name == 'no':
        name = f"{table}_with_initial_load"
    elif name == 'yes':
        name = str(input("      input the name: "))

    fact_field = str(input("input the fact field: "))

    cursor.execute( f" SELECT * FROM {table} " )
    titles = [ x[0] for x in cursor.description ]
    selected = []
    selected_func = []
    for title in titles:
        if ('d' in title and 'end' in title) or ('dt' in title and 'end' in title) or ('date' in title and 'end' in title) :
            end_date_field = title
        elif ('date' in title or 'dt' in title) or ('date' in title and 'start' in title) or ('dt' in title and 'start' in title) or ('d' in title and 'start' in title) :
            start_date_field = title
        elif title == partition_field:
            selected.append(title)
        elif title == fact_field:
            selected.append(title)
        else :
            selected.append(title) # partitions, facts AND others
            selected_func.append(title) # others ONLY

    def item_in_list():
        s = ''  # переменная повторяется намеренно
        titles = []  # переменная повторяется намеренно
        for item in selected_func:
            a = f"COALESCE( LAG (a.{item}) OVER ( PARTITION BY a.{partition_field} ORDER BY a.start_dt ), 'no data') AS {item} ,"
            titles.append(a)
        titles = s.join(titles)
        return titles

    selected = s.join(selected)

    try:
        cursor.execute(f"""
                            CREATE TABLE {name} AS
                            WITH temp_tbl AS (
                                SELECT 	{selected}, start_dt ,
                                    COALESCE( CAST (LEAD (start_dt) OVER ( PARTITION BY {partition_field} ORDER BY start_dt ) - INTERVAL '1 day' AS DATE ), '9999-01-01' ) AS end_dt
                                FROM (
                                    SELECT
                                        {selected} ,
                                        CASE 
                                            WHEN LEAD ( {fact_field} ) OVER ( PARTITION BY {partition_field} ORDER BY {start_date_field} ) = class THEN 0
                                            ELSE 1
                                        END AS class_new ,
                                        {start_date_field} as start_dt
                                    FROM {table} ) q
                                WHERE class_new = 1
                                )
                            SELECT
                                {item_in_list()}
                                COALESCE( LAG (a.{partition_field}) OVER ( PARTITION BY a.{partition_field} ORDER BY a.start_dt ), 'no {partition_field}') AS {partition_field} , 
                                COALESCE( LAG (a.{fact_field}) OVER ( PARTITION BY a.{partition_field} ORDER BY a.start_dt ), 'no {fact_field}') AS {fact_field} ,
                                COALESCE( LAG (a.start_dt) OVER ( PARTITION BY a.{partition_field} ORDER BY a.start_dt ), '01-01-1900') AS start_dt ,
                                COALESCE( LAG (a.end_dt) OVER ( PARTITION BY a.{partition_field} ORDER BY a.start_dt ), CAST(a.start_dt - INTERVAL '1 day' AS DATE) ) AS end_dt 
                            FROM temp_tbl a
                            INNER JOIN temp_tbl b
                                ON a.class = b.class
                            ORDER BY a.{partition_field}, a.start_dt
                            """)
        conn.commit()
    except psycopg2.errors.DuplicateTable:
        print(f"""
table {name} already exists
""")
        conn.rollback()

    return 1
