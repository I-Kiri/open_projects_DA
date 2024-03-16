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
                            update_dt timestamp(0) ) """ )
        print('created table {schema_name}.kzmn_stg_source_{entity_name}')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание target-таблицы (сущности "{entity_name}")
    try:
        cursor.execute( """ create table {schema_name}.kzmn_dwh_dim_{entity_name} (
                            {field_in_list()}
                            create_dt timestamp(0) ,
                            update_dt timestamp(0) ) """ )
        print('created table {schema_name}.kzmn_dwh_dim_{entity_name}')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание stg-таблицы
    try:
        cursor.execute( """ create table {schema_name}.kzmn_stg_{entity_name}( 
                            {field_in_list()}
                            update_dt timestamp(0) ) """ )
        print('created table {schema_name}.kzmn_stg_{entity_name}')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание stg_del-таблицы
    try:
        cursor.execute( """ create table {schema_name}.kzmn_stg_del_{entity_name} ( 
                            {key_field} ) """ )
        print('created table {schema_name}.kzmn_stg_del_{entity_name}')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание meta-таблицы
    try:
        cursor.execute( """ create table {schema_name}.kzmn_meta_{entity_name} (
                            schema_name varchar(20) ,
                            table_name varchar(60) ,
                            max_update_dt timestamp(0) ) """ )
        cursor.execute( """ insert into {schema_name}.kzmn_meta_{entity_name}
                                ( schema_name, table_name, max_update_dt )
                            values
                                ( '{schema_name}','kzmn_stg_source_{entity_name}', to_timestamp('1900-01-01','YYYY-MM-DD') ) """ )
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
                            dt timestamp(0) ) """ )
        print('created table {schema_name}.kzmn_stg_source_{entity_name}')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание target-таблицы (сущности "{entity_name}")
    try:
        cursor.execute( """ create table {schema_name}.kzmn_dwh_dim_{entity_name}_hist ( 
                            {field_in_list()}
                            effective_from timestamp(0) ,
                            effective_to timestamp(0) ,
                            deleted_flg char(1) ) """ )
        print('created table {schema_name}.kzmn_dwh_dim_{entity_name}_hist')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание stg-таблицы
    try:
        cursor.execute( """ create table {schema_name}.kzmn_stg_{entity_name} (
                            {field_in_list()}
                            update_dt timestamp(0) ) """ )
        print('created table {schema_name}.kzmn_stg_{entity_name}')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание meta_stg_del-таблицы
    try:
        cursor.execute( """ create table {schema_name}.kzmn_stg_del_{entity_name} (
                            {key_field} ) """ )
        print('created table {schema_name}.kzmn_stg_del_{entity_name}')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание meta-таблицы
    try:
        cursor.execute( """ create table {schema_name}.kzmn_meta_{entity_name} (
                            schema_name varchar(20) ,
                            table_name varchar(60) ,
                            last_update_dt timestamp(0) ) """ )
        cursor.execute( """ insert into {schema_name}.kzmn_meta_{entity_name}
                                    ( schema_name, table_name, last_update_dt )
                                values
                                    ( '{schema_name}','kzmn_stg_source_{entity_name}', to_timestamp('1900-01-01','YYYY-MM-DD') ) """ )
        print('created table {schema_name}.kzmn_meta_{entity_name}')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()
    conn.commit()  # необходимо зафиксировать транзакцию в любом состоянии

    return 1
    ''' )

    return 1


def scd2entity_with_processed_dt_script_generator( schema_name, entity_name, target_fields_names_and_types ):
    key_field = target_fields_names_and_types[0]

    def field_in_list_1():
        target_fields_names_and_types_1 = copy.deepcopy( target_fields_names_and_types )
        target_fields_names_and_types_1.remove( target_fields_names_and_types[0] )
        s = ''
        fields_func = []
        for field in target_fields_names_and_types_1:
            update_string = f'{field}, '
            fields_func.append( update_string )
        fields_func = s.join( fields_func )
        return fields_func
    
    def field_in_list_2():
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
                            {field_in_list_2()}
                            dt timestamp(0) ) """ )
        print('created table {schema_name}.kzmn_stg_source_{entity_name}')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание target-таблицы (сущности "{entity_name}")
    try:
        cursor.execute( """ create table {schema_name}.kzmn_dwh_dim_{entity_name}_hist (
                            {key_field} ,
                            start_dt timestamp(0) ,
                            {field_in_list_1()}
                            deleted_flag char(1) ,
                            end_dt timestamp(0) ,
                            processed_dt timestamp(0) ) """ )
        print('created table {schema_name}.kzmn_dwh_dim_{entity_name}_hist')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание stg-таблицы
    try:
        cursor.execute( """ create table {schema_name}.kzmn_stg_{entity_name} (
                            {field_in_list_2()}
                            update_dt timestamp(0) ) """ )
        print('created table {schema_name}.kzmn_stg_{entity_name}')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание meta_stg_del-таблицы
    try:
        cursor.execute( """ create table {schema_name}.kzmn_stg_del_{entity_name} (
                            {key_field} ) """ )
        print('created table {schema_name}.kzmn_stg_del_{entity_name}')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание meta-таблицы
    try:
        cursor.execute( """ create table {schema_name}.kzmn_meta_{entity_name} (
                            schema_name varchar(20) ,
                            table_name varchar(60) ,
                            last_update_dt timestamp(0) ) """ )
        cursor.execute( """ insert into {schema_name}.kzmn_meta_{entity_name}
                                    ( schema_name, table_name, last_update_dt )
                                values
                                    ( '{schema_name}','kzmn_stg_source_{entity_name}', to_timestamp('1900-01-01','YYYY-MM-DD') ) """ )
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
                                    {field_in_list()} ) """ )
        print('created table {schema_name}.kzmn_stg_{entity_name}')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание фактовой target-таблицы (сущности "{entity_name}")
    try:
        cursor.execute( """ create table {schema_name}.kzmn_dwh_fact_{entity_name} (
                                {field_in_list()} ) """ )
        print('created table {schema_name}.kzmn_dwh_fact_{entity_name}')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()  
    conn.commit()  # необходимо зафиксировать транзакцию в любом состоянии

    return 1
    ''' )

    return 1


def factentity_with_processed_dt_script_generator( schema_name, entity_name, target_fields_names_and_types ):

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
                                    {field_in_list()} ) """ )
        print('created table {schema_name}.kzmn_stg_{entity_name}')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание фактовой target-таблицы (сущности "{entity_name}")
    try:
        cursor.execute( """ create table {schema_name}.kzmn_dwh_fact_{entity_name} (
                                {field_in_list()},
                                processed_dt timestamp(0) ) """ )
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
        fields_func_1.remove( fields_func_1[0] )
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
        cursor.execute( """ delete from {schema_name}.kzmn_stg_{entity_name} """ )
        cursor.execute( """ delete from {schema_name}.kzmn_stg_del_{entity_name} """ )

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
                                                            and table_name='kzmn_stg_source_{entity_name}' ) """ )

        # 3. Захват полного среза ключей для вычисления удалений
        cursor.execute( """ insert into {schema_name}.kzmn_stg_del_{entity_name}
                                ( {key_field} )
                                    select
                                        {key_field}
                                    from {schema_name}.kzmn_stg_source_{entity_name} """ )

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
                                    where t.{key_field} is null """ )

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
                            where kzmn_dwh_dim_{entity_name}.{key_field} = tmp.{key_field} """ )

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
                                    and table_name='kzmn_stg_source_{entity_name}'""" )

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
        fields_add = copy.deepcopy( fields )
        fields_add.remove(fields_add[0])

        s = ''
        fields_func = []
        fields_func_2 = []
        fields_func_2 = s.join( f'kzmn_dwh_dim_{entity_name}_hist.{fields_add[0]} = ttemp.{fields_add[0]} ' )
        fields_add.remove(fields_add[0])

        for field in fields:
            update_string = f""" and kzmn_dwh_dim_{entity_name}_hist.{field} = ttemp.{field}"""
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
                                ( {fields_str}, update_dt )
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


def scd2load_with_processed_dt_script_generator( schema_name, entity_name, fields ):
    s = ', '
    key_field = fields[ 0 ]
    fields_str = s.join( fields )

    def selected_field_in_list( alias ):
        s = ''
        fields_func = [ ]
        for field in fields:
            update_string = f'{alias}.{field}, '
            fields_func.append( update_string )
        fields_func = s.join( fields_func )
        return fields_func

    def field_in_list():
        fields_func_1 = copy.deepcopy( fields )
        fields_func_1.remove( fields_func_1[ 0 ] )
        s = ''
        fields_func = [ ]
        for field in fields_func_1:
            update_string = f"""----
                                        or ( (s.{field} <> t.{field} or (s.{field} is null and t.{field} is not null) or (s.{field} is not null and t.{field} is null))
                                            and s.{field} not in (select t.{field} from {schema_name}.kzmn_stg_{entity_name} s inner join {schema_name}.kzmn_dwh_dim_{entity_name}_hist t on s.{key_field} = t.{key_field}) )"""
            fields_func.append( update_string )
        fields_func = s.join( fields_func )
        return fields_func

    def field_in_list_2():
        fields_func_2 = copy.deepcopy( fields )
        fields_func_2.remove( fields_func_2[ 0 ] )
        s = ''
        fields_func = [ ]
        for field in fields_func_2:
            update_string = f""" and kzmn_dwh_dim_{entity_name}_hist.{field} = ttemp.{field}"""
            fields_func.append( update_string )
        fields_func = s.join( fields_func )
        return fields_func

    def field_in_list_3():
        fields_add = copy.deepcopy( fields )
        fields_add.remove( fields_add[ 0 ] )

        s = ''
        fields_func = [ ]
        fields_func_2 = [ ]
        fields_func_2 = s.join( f'kzmn_dwh_dim_{entity_name}_hist.{fields_add[ 0 ]} = ttemp.{fields_add[ 0 ]} ' )
        fields_add.remove( fields_add[ 0 ] )

        for field in fields:
            update_string = f""" and kzmn_dwh_dim_{entity_name}_hist.{field} = ttemp.{field}"""
            fields_func.append( update_string )
        fields_func = fields_func_2 + s.join( fields_func )
        return fields_func

    print( f'''
        # Последовательная инкрементальная выгрузка данных сущности "{entity_name}" :

        try:
            # 1. Очистка стейджинговых таблиц
            cursor.execute( """delete from {schema_name}.kzmn_stg_{entity_name} ;""" )
            cursor.execute( """delete from {schema_name}.kzmn_stg_del_{entity_name} ;""" )

            # 2. Захват данных из источника (измененных с момента последней загрузки) в стейджинг
            cursor.execute( """insert into {schema_name}.kzmn_stg_{entity_name}
                                    ( {fields_str}, update_dt )
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
                                    ( {fields_str}, start_dt, end_dt, deleted_flag, processed_dt )
                                        select
                                            {selected_field_in_list( alias='s' )}
                                            s.update_dt ,
                                            coalesce(cast (lead ( s.update_dt) over ( partition by s.{key_field} order by s.update_dt ) - interval '1 day' as date), '9999-12-31' ) , 
                                            'N' ,
                                            now()
                                        from {schema_name}.kzmn_stg_{entity_name} s
                                        left join {schema_name}.kzmn_dwh_dim_{entity_name}_hist t 
                                        on s.{key_field} = t.{key_field}
                                        where t.{key_field} is null ;""")

            # 5. Обновление в приемнике "обновлений" на источнике (формат SCD2, с учетом фиктивных обновлений)
            cursor.execute( """insert into {schema_name}.kzmn_dwh_dim_{entity_name}_hist
                                    ( {fields_str}, start_dt, end_dt, deleted_flag, processed_dt ) 
                                        select distinct 
                                            {selected_field_in_list( alias='s' )}
                                            s.update_dt ,
                                            cast(null as timestamp) ,
                                            'N' ,
                                            now()
                                        from {schema_name}.kzmn_stg_{entity_name} s
                                        left join {schema_name}.kzmn_dwh_dim_{entity_name}_hist t
                                            on s.{key_field} = t.{key_field}
                                        where 1=0
                                        {field_in_list()} ;""")

            cursor.execute( """update {schema_name}.kzmn_dwh_dim_{entity_name}_hist
                                set 
                                    end_dt = ttemp.end_dt
                                from (
                                    select
                                        {selected_field_in_list( alias='a' )}
                                        coalesce((lead ( a.start_dt ) over ( partition by a.{key_field} order by a.start_dt ) - interval '1 day' ), '9999-12-31' ) as end_dt 
                                    from {schema_name}.kzmn_dwh_dim_{entity_name}_hist a
                                    inner join {schema_name}.kzmn_dwh_dim_{entity_name}_hist b
                                        on a.{key_field} = b.{key_field}
                                        and b.end_dt is null
                                ) ttemp
                            where ( kzmn_dwh_dim_{entity_name}_hist.{key_field} = ttemp.{key_field}
                                    {field_in_list_2()} ) ;""")

            # 6. Удаление удаленных записей (формат SCD2)
            cursor.execute( """insert into {schema_name}.kzmn_dwh_dim_{entity_name}_hist
                                    ( {fields_str}, start_dt, end_dt, deleted_flag, processed_dt )
                                        select 
                                            {fields_str} ,
                                            cast( now() as timestamp ) ,
                                            cast( null as timestamp ) , 
                                            'Y' ,
                                            now()
                                        from {schema_name}.kzmn_dwh_dim_{entity_name}_hist
                                        where {key_field} in (
                                            select
                                                t.{key_field}
                                            from {schema_name}.kzmn_dwh_dim_{entity_name}_hist t 
                                            left join {schema_name}.kzmn_stg_del_{entity_name} s
                                                on t.{key_field} = s.{key_field}
                                                where s.{key_field} is null
                                                )
                                            and end_dt = '9999-12-31'
                                            and deleted_flag = 'N' ;""")

            cursor.execute( """update {schema_name}.kzmn_dwh_dim_{entity_name}_hist
                                set 
                                    end_dt = ttemp.end_dt
                                from (
                                    select
                                    {fields_str} ,
                                    end_dt
                                    from (	select
                                            {selected_field_in_list( alias='a' )}
                                            a.end_dt as dt ,
                                            coalesce((lead ( a.start_dt ) over ( partition by a.{key_field} order by a.start_dt ) - interval '1 day' ), '9999-12-31' ) as end_dt
                                            from {schema_name}.kzmn_dwh_dim_{entity_name}_hist a
                                            inner join {schema_name}.kzmn_dwh_dim_{entity_name}_hist b
                                                on a.{key_field} = b.{key_field}
                                                and b.end_dt is null ) sq
                                    where dt = '9999-12-31'
                                ) ttemp
                            where kzmn_dwh_dim_{entity_name}_hist.{key_field} = ttemp.{key_field}
                                and ( {field_in_list_3()} )
                                    and kzmn_dwh_dim_{entity_name}_hist.end_dt  = '9999-12-31' ;""")
            cursor.execute( """update {schema_name}.kzmn_dwh_dim_{entity_name}_hist
                                set 
                                    end_dt = ttemp.end_dt
                                from (
                                    select
                                        {fields_str} ,
                                        start_dt ,
                                        coalesce(end_dt, '9999-12-31') as end_dt ,
                                        deleted_flag
                                    from {schema_name}.kzmn_dwh_dim_{entity_name}_hist
                                    where deleted_flag = 'Y'
                                ) ttemp
                            where kzmn_dwh_dim_{entity_name}_hist.{key_field} = ttemp.{key_field}
                                and ( {field_in_list_3()} )
                                    and kzmn_dwh_dim_{entity_name}_hist.end_dt is null ;""")

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
                                    where t.{key_field} is null """ )

        # 2. Дополнительная очистка стейджинга-источника
        cursor.execute( """ delete from {schema_name}.kzmn_stg_{entity_name} """ )

        # 3. Фиксация транзакции
        #conn.commit()

    except Exception :
        conn.rollback()
        print( '{entity_name}_fact_load --> something went wrong' )

    return 1
    ''' )

    return 1


def factload_with_processed_dt_script_generator( schema_name, entity_name, fields ):
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
                                ( {field_in_list()}, processed_dt )
                                    select
                                        {selected_field_in_list()} ,
                                        now()
                                    from {schema_name}.kzmn_stg_{entity_name} s
                                    left join {schema_name}.kzmn_dwh_fact_{entity_name} t 
                                    on s.{key_field} = t.{key_field}
                                    where t.{key_field} is null """ )

        # 2. Дополнительная очистка стейджинга-источника
        cursor.execute( """ delete from {schema_name}.kzmn_stg_{entity_name} """ )

        # 3. Фиксация транзакции
        #conn.commit()

    except Exception :
        conn.rollback()
        print( '{entity_name}_fact_load --> something went wrong' )

    return 1
    ''' )

    return 1


def cars_entity( conn, cursor ):
    # Создание в хранилище сущности "cars" :

    # Создание искусственного источника (сущности "cars")
    try:
        cursor.execute( """ create table dwh_novokuznetsk.kzmn_stg_source_cars (
                                plate_num char(9), model_name varchar(20), revision_dt date, 
                                dt timestamp(0) ) """ )
        print( '! created table dwh_novokuznetsk.kzmn_stg_source_cars' )
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание target-таблицы (сущности "cars")
    try:
        cursor.execute( """ create table dwh_novokuznetsk.kzmn_dwh_dim_cars_hist (
                                plate_num char(9) ,
                                start_dt timestamp(0) ,
                                model_name varchar(20), revision_dt date, 
                                deleted_flag char(1) ,
                                end_dt timestamp(0) ,
                                processed_dt timestamp(0) ) """ )
        print( '! created table dwh_novokuznetsk.kzmn_dwh_dim_cars_hist' )
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание stg-таблицы
    try:
        cursor.execute( """ create table dwh_novokuznetsk.kzmn_stg_cars (
                                plate_num char(9), model_name varchar(20), revision_dt date, 
                                update_dt timestamp(0) ) """ )
        print( '! created table dwh_novokuznetsk.kzmn_stg_cars' )
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание stg_del-таблицы
    try:
        cursor.execute( """ create table dwh_novokuznetsk.kzmn_stg_del_cars (
                                plate_num char(9) ) """ )
        print( '! created table dwh_novokuznetsk.kzmn_stg_del_cars' )
    except psycopg2.errors.DuplicateTable:
        conn.rollback()
        
    # Создание stg_del_add-таблицы
    try:
        cursor.execute( """ create table dwh_novokuznetsk.kzmn_stg_del_add_cars (
                                plate_num char(9) ) """ )
        print( '! created table dwh_novokuznetsk.kzmn_stg_del_cars' )
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание meta-таблицы
    try:
        cursor.execute( """ create table dwh_novokuznetsk.kzmn_meta_cars (
                                schema_name varchar(20) ,
                                table_name varchar(60) ,
                                last_update_dt timestamp(0) ) """ )
        cursor.execute( """ insert into dwh_novokuznetsk.kzmn_meta_cars
                                        ( schema_name, table_name, last_update_dt )
                                    values
                                        ( 'dwh_novokuznetsk','kzmn_stg_source_cars', to_timestamp('1900-01-01','YYYY-MM-DD') ) """ )
        print( '! created table dwh_novokuznetsk.kzmn_meta_cars' )
    except psycopg2.errors.DuplicateTable:
        conn.rollback()
    conn.commit()  # необходимо зафиксировать транзакцию в любом состоянии

    return 1


def cars_scd2_load( conn, cursor ):
    # Последовательная инкрементальная выгрузка данных сущности "cars" :

    try:
        # 1. Очистка стейджинговых таблиц
        cursor.execute( """delete from dwh_novokuznetsk.kzmn_stg_cars ;""" )
        cursor.execute( """delete from dwh_novokuznetsk.kzmn_stg_del_cars ;""" )

        # 2. Захват данных из источника (измененных с момента последней загрузки) в стейджинг
        cursor.execute( """insert into dwh_novokuznetsk.kzmn_stg_cars
                                ( plate_num, model_name, revision_dt, update_dt )
                                    select
                                        plate_num, model_name, revision_dt ,
                                        dt
                                    from dwh_novokuznetsk.kzmn_stg_source_cars
                                    where dt > ( select last_update_dt
                                                from dwh_novokuznetsk.kzmn_meta_cars
                                                where schema_name='dwh_novokuznetsk'
                                                    and table_name='kzmn_stg_source_cars' ) ;""" )

        # 3. Захват полного среза ключей для вычисления удалений
        cursor.execute( """insert into dwh_novokuznetsk.kzmn_stg_del_cars
                                ( plate_num )
                                    select
                                        plate_num
                                    from dwh_novokuznetsk.kzmn_stg_source_cars ;""" )

        # 4. Загрузка в приемник "вставок" на источнике (формат SCD2)
        cursor.execute( """insert into dwh_novokuznetsk.kzmn_dwh_dim_cars_hist
                                ( plate_num, model_name, revision_dt, start_dt, end_dt, deleted_flag, processed_dt )
                                    select
                                        s.plate_num, s.model_name, s.revision_dt, 
                                        s.update_dt ,
                                        coalesce(cast (lead ( s.update_dt) over ( partition by s.plate_num order by s.update_dt ) - interval '1 day' as date), '9999-12-31' ) , 
                                        'N' ,
                                        now()
                                    from dwh_novokuznetsk.kzmn_stg_cars s
                                    left join dwh_novokuznetsk.kzmn_dwh_dim_cars_hist t 
                                    on s.plate_num = t.plate_num
                                    where t.plate_num is null ;""" )

        # 5. Обновление в приемнике "обновлений" на источнике (формат SCD2, с учетом фиктивных обновлений)
        cursor.execute( """insert into dwh_novokuznetsk.kzmn_dwh_dim_cars_hist
                                ( plate_num, model_name, revision_dt, start_dt, end_dt, deleted_flag, processed_dt ) 
                                    select distinct 
                                        s.plate_num, s.model_name, s.revision_dt, 
                                        s.update_dt ,
                                        cast(null as timestamp) ,
                                        'N' ,
                                        now()
                                    from dwh_novokuznetsk.kzmn_stg_cars s
                                    left join dwh_novokuznetsk.kzmn_dwh_dim_cars_hist t
                                        on s.plate_num = t.plate_num
                                    where 1=0
                                    ----
                                    or ( (s.model_name <> t.model_name or (s.model_name is null and t.model_name is not null) or (s.model_name is not null and t.model_name is null))
                                        and s.model_name not in (select t.model_name from dwh_novokuznetsk.kzmn_stg_cars s inner join dwh_novokuznetsk.kzmn_dwh_dim_cars_hist t on s.plate_num = t.plate_num) )----
                                    or ( (s.revision_dt <> t.revision_dt or (s.revision_dt is null and t.revision_dt is not null) or (s.revision_dt is not null and t.revision_dt is null))
                                        and s.revision_dt not in (select t.revision_dt from dwh_novokuznetsk.kzmn_stg_cars s inner join dwh_novokuznetsk.kzmn_dwh_dim_cars_hist t on s.plate_num = t.plate_num) ) ;""" )

        cursor.execute( """update dwh_novokuznetsk.kzmn_dwh_dim_cars_hist
                            set 
                                end_dt = ttemp.end_dt
                            from (
                                select
                                    a.plate_num, a.model_name, a.revision_dt, 
                                    coalesce((lead ( a.start_dt ) over ( partition by a.plate_num order by a.start_dt ) - interval '1 day' ), '9999-12-31' ) as end_dt 
                                from dwh_novokuznetsk.kzmn_dwh_dim_cars_hist a
                                inner join dwh_novokuznetsk.kzmn_dwh_dim_cars_hist b
                                    on a.plate_num = b.plate_num
                                    and b.end_dt is null
                            ) ttemp
                        where ( kzmn_dwh_dim_cars_hist.plate_num = ttemp.plate_num
                                 and kzmn_dwh_dim_cars_hist.model_name = ttemp.model_name and kzmn_dwh_dim_cars_hist.revision_dt = ttemp.revision_dt ) ;""" )

        # 6. Удаление удаленных записей (формат SCD2)
        cursor.execute( """insert into dwh_novokuznetsk.kzmn_dwh_dim_cars_hist
                                ( plate_num, model_name, revision_dt, start_dt, end_dt, deleted_flag, processed_dt )
                                    select 
                                        plate_num, model_name, revision_dt ,
                                        cast( now() as timestamp ) ,
                                        cast( null as timestamp ) , 
                                        'Y' ,
                                        now()
                                    from dwh_novokuznetsk.kzmn_dwh_dim_cars_hist
                                    where plate_num in (
                                        select
                                            t.plate_num
                                        from dwh_novokuznetsk.kzmn_dwh_dim_cars_hist t 
                                        left join dwh_novokuznetsk.kzmn_stg_del_cars s
                                            on t.plate_num = s.plate_num
                                            where s.plate_num is null
                                            )
                                        and end_dt = '9999-12-31'
                                        and deleted_flag = 'N' ;""" )

        cursor.execute( """update dwh_novokuznetsk.kzmn_dwh_dim_cars_hist
                            set 
                                end_dt = ttemp.end_dt
                            from (
                                select
                                plate_num, model_name, revision_dt ,
                                end_dt
                                from (	select
                                        a.plate_num, a.model_name, a.revision_dt, 
                                        a.end_dt as dt ,
                                        coalesce((lead ( a.start_dt ) over ( partition by a.plate_num order by a.start_dt ) - interval '1 day' ), '9999-12-31' ) as end_dt
                                        from dwh_novokuznetsk.kzmn_dwh_dim_cars_hist a
                                        inner join dwh_novokuznetsk.kzmn_dwh_dim_cars_hist b
                                            on a.plate_num = b.plate_num
                                            and b.end_dt is null ) sq
                                where dt = '9999-12-31'
                            ) ttemp
                        where kzmn_dwh_dim_cars_hist.plate_num = ttemp.plate_num
                            and ( kzmn_dwh_dim_cars_hist.model_name = ttemp.model_name  and kzmn_dwh_dim_cars_hist.plate_num = ttemp.plate_num and kzmn_dwh_dim_cars_hist.model_name = ttemp.model_name and kzmn_dwh_dim_cars_hist.revision_dt = ttemp.revision_dt )
                                and kzmn_dwh_dim_cars_hist.end_dt  = '9999-12-31' ;""" )
        cursor.execute( """update dwh_novokuznetsk.kzmn_dwh_dim_cars_hist
                            set 
                                end_dt = ttemp.end_dt
                            from (
                                select
                                    plate_num, model_name, revision_dt ,
                                    start_dt ,
                                    coalesce(end_dt, '9999-12-31') as end_dt ,
                                    deleted_flag
                                from dwh_novokuznetsk.kzmn_dwh_dim_cars_hist
                                where deleted_flag = 'Y'
                            ) ttemp
                        where kzmn_dwh_dim_cars_hist.plate_num = ttemp.plate_num
                            and ( kzmn_dwh_dim_cars_hist.model_name = ttemp.model_name  and kzmn_dwh_dim_cars_hist.plate_num = ttemp.plate_num and kzmn_dwh_dim_cars_hist.model_name = ttemp.model_name and kzmn_dwh_dim_cars_hist.revision_dt = ttemp.revision_dt )
                                and kzmn_dwh_dim_cars_hist.end_dt is null ;""" )

        # 7. Обновление метаданных
        cursor.execute( """update dwh_novokuznetsk.kzmn_meta_cars
                                set last_update_dt = coalesce( ( select max(update_dt)
                                                                from dwh_novokuznetsk.kzmn_stg_cars ),
                                                                    ( select last_update_dt
                                                                    from dwh_novokuznetsk.kzmn_meta_cars
                                                                    where schema_name='dwh_novokuznetsk'
                                                                        and table_name='kzmn_stg_source_cars' ) )
                                where schema_name='dwh_novokuznetsk'
                                    and table_name='kzmn_stg_source_cars' ;""" )

        # 8. Фиксация транзакции
        #conn.commit()

    except Exception:
        conn.rollback()
        print( 'cars_scd2_load --> something went wrong' )

    return 1


def drivers_entity( conn, cursor ):
    # Создание в хранилище сущности "drivers" :

    # Создание искусственного источника (сущности "drivers")
    try:
        cursor.execute( """ create table dwh_novokuznetsk.kzmn_stg_source_drivers (
                            personnel_num char(5), last_name varchar(20), first_name varchar(20), middle_name varchar(20), birth_dt date, card_num char(19), driver_license_num char(12), driver_license_dt date, 
                            dt timestamp(0) ) """ )
        print('! created table dwh_novokuznetsk.kzmn_stg_source_drivers')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание target-таблицы (сущности "drivers")
    try:
        cursor.execute( """ create table dwh_novokuznetsk.kzmn_dwh_dim_drivers_hist (
                            personnel_num char(5) ,
                            start_dt timestamp(0) ,
                            last_name varchar(20), first_name varchar(20), middle_name varchar(20), birth_dt date, card_num char(19), driver_license_num char(12), driver_license_dt date, 
                            deleted_flag char(1) ,
                            end_dt timestamp(0) ,
                            processed_dt timestamp(0) ) """ )
        print('! created table dwh_novokuznetsk.kzmn_dwh_dim_drivers_hist')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание stg-таблицы
    try:
        cursor.execute( """ create table dwh_novokuznetsk.kzmn_stg_drivers (
                            personnel_num char(5), last_name varchar(20), first_name varchar(20), middle_name varchar(20), birth_dt date, card_num char(19), driver_license_num char(12), driver_license_dt date, 
                            update_dt timestamp(0) ) """ )
        print('! created table dwh_novokuznetsk.kzmn_stg_drivers')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание stg_del-таблицы
    try:
        cursor.execute( """ create table dwh_novokuznetsk.kzmn_stg_del_drivers (
                            personnel_num char(5) ) """ )
        print('! created table dwh_novokuznetsk.kzmn_stg_del_drivers')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()
        
    # Создание stg_del_add-таблицы
    try:
        cursor.execute( """ create table dwh_novokuznetsk.kzmn_stg_del_add_drivers (
                            personnel_num char(5) ) """ )
        print( '! created table dwh_novokuznetsk.kzmn_stg_del_clients' )
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание meta-таблицы
    try:
        cursor.execute( """ create table dwh_novokuznetsk.kzmn_meta_drivers (
                            schema_name varchar(20) ,
                            table_name varchar(60) ,
                            last_update_dt timestamp(0) ) """ )
        cursor.execute( """ insert into dwh_novokuznetsk.kzmn_meta_drivers
                                    ( schema_name, table_name, last_update_dt )
                                values
                                    ( 'dwh_novokuznetsk','kzmn_stg_source_drivers', to_timestamp('1900-01-01 00:00:00.000','YYYY-MM-DD') ) """ )
        print('! created table dwh_novokuznetsk.kzmn_meta_drivers')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()
    conn.commit()  # необходимо зафиксировать транзакцию в любом состоянии

    return 1


def drivers_scd2_load( conn, cursor ):
    # Последовательная инкрементальная выгрузка данных сущности "drivers" :

    try:
        # 1. Очистка стейджинговых таблиц
        cursor.execute( """delete from dwh_novokuznetsk.kzmn_stg_drivers ;""" )

        # 2. Захват данных из источника (измененных с момента последней загрузки) в стейджинг
        cursor.execute( """insert into dwh_novokuznetsk.kzmn_stg_drivers
                                ( personnel_num, last_name, first_name, middle_name, birth_dt, card_num, driver_license_num, driver_license_dt, update_dt )
                                    select
                                        personnel_num, last_name, first_name, middle_name, birth_dt, card_num, driver_license_num, driver_license_dt ,
                                        dt
                                    from dwh_novokuznetsk.kzmn_stg_source_drivers
                                    where dt > ( select last_update_dt
                                                from dwh_novokuznetsk.kzmn_meta_drivers
                                                where schema_name='dwh_novokuznetsk'
                                                    and table_name='kzmn_stg_source_drivers' ) ;""" )

        # 3. Захват полного среза ключей для вычисления удалений
        cursor.execute( """insert into dwh_novokuznetsk.kzmn_stg_del_drivers
                                ( personnel_num )
                                    select
                                        personnel_num
                                    from dwh_novokuznetsk.kzmn_stg_source_drivers ;""" )

        # 4. Загрузка в приемник "вставок" на источнике (формат SCD2)
        cursor.execute( """insert into dwh_novokuznetsk.kzmn_dwh_dim_drivers_hist
                                ( personnel_num, last_name, first_name, middle_name, birth_dt, card_num, driver_license_num, driver_license_dt, start_dt, end_dt, deleted_flag, processed_dt )
                                    select
                                        s.personnel_num, s.last_name, s.first_name, s.middle_name, s.birth_dt, s.card_num, s.driver_license_num, s.driver_license_dt, 
                                        s.update_dt ,
                                        coalesce(cast (lead ( s.update_dt) over ( partition by s.personnel_num order by s.update_dt ) - interval '1 day' as date), '9999-12-31' ) , 
                                        'N' ,
                                        now()
                                    from dwh_novokuznetsk.kzmn_stg_drivers s
                                    left join dwh_novokuznetsk.kzmn_dwh_dim_drivers_hist t 
                                    on s.personnel_num = t.personnel_num
                                    where t.personnel_num is null ;""" )

        # 5. Обновление в приемнике "обновлений" на источнике (формат SCD2, с учетом фиктивных обновлений)
        cursor.execute( """insert into dwh_novokuznetsk.kzmn_dwh_dim_drivers_hist
                                ( personnel_num, last_name, first_name, middle_name, birth_dt, card_num, driver_license_num, driver_license_dt, start_dt, end_dt, deleted_flag, processed_dt ) 
                                    select distinct 
                                        s.personnel_num, s.last_name, s.first_name, s.middle_name, s.birth_dt, s.card_num, s.driver_license_num, s.driver_license_dt, 
                                        s.update_dt ,
                                        cast(null as timestamp) ,
                                        'N' ,
                                        now()
                                    from dwh_novokuznetsk.kzmn_stg_drivers s
                                    left join dwh_novokuznetsk.kzmn_dwh_dim_drivers_hist t
                                        on s.personnel_num = t.personnel_num
                                    where 1=0
                                    ----
                                    or ( (s.last_name <> t.last_name or (s.last_name is null and t.last_name is not null) or (s.last_name is not null and t.last_name is null))
                                        and s.last_name not in (select t.last_name from dwh_novokuznetsk.kzmn_stg_drivers s inner join dwh_novokuznetsk.kzmn_dwh_dim_drivers_hist t on s.personnel_num = t.personnel_num) )----
                                    or ( (s.first_name <> t.first_name or (s.first_name is null and t.first_name is not null) or (s.first_name is not null and t.first_name is null))
                                        and s.first_name not in (select t.first_name from dwh_novokuznetsk.kzmn_stg_drivers s inner join dwh_novokuznetsk.kzmn_dwh_dim_drivers_hist t on s.personnel_num = t.personnel_num) )----
                                    or ( (s.middle_name <> t.middle_name or (s.middle_name is null and t.middle_name is not null) or (s.middle_name is not null and t.middle_name is null))
                                        and s.middle_name not in (select t.middle_name from dwh_novokuznetsk.kzmn_stg_drivers s inner join dwh_novokuznetsk.kzmn_dwh_dim_drivers_hist t on s.personnel_num = t.personnel_num) )----
                                    or ( (s.birth_dt <> t.birth_dt or (s.birth_dt is null and t.birth_dt is not null) or (s.birth_dt is not null and t.birth_dt is null))
                                        and s.birth_dt not in (select t.birth_dt from dwh_novokuznetsk.kzmn_stg_drivers s inner join dwh_novokuznetsk.kzmn_dwh_dim_drivers_hist t on s.personnel_num = t.personnel_num) )----
                                    or ( (s.card_num <> t.card_num or (s.card_num is null and t.card_num is not null) or (s.card_num is not null and t.card_num is null))
                                        and s.card_num not in (select t.card_num from dwh_novokuznetsk.kzmn_stg_drivers s inner join dwh_novokuznetsk.kzmn_dwh_dim_drivers_hist t on s.personnel_num = t.personnel_num) )----
                                    or ( (s.driver_license_num <> t.driver_license_num or (s.driver_license_num is null and t.driver_license_num is not null) or (s.driver_license_num is not null and t.driver_license_num is null))
                                        and s.driver_license_num not in (select t.driver_license_num from dwh_novokuznetsk.kzmn_stg_drivers s inner join dwh_novokuznetsk.kzmn_dwh_dim_drivers_hist t on s.personnel_num = t.personnel_num) )----
                                    or ( (s.driver_license_dt <> t.driver_license_dt or (s.driver_license_dt is null and t.driver_license_dt is not null) or (s.driver_license_dt is not null and t.driver_license_dt is null))
                                        and s.driver_license_dt not in (select t.driver_license_dt from dwh_novokuznetsk.kzmn_stg_drivers s inner join dwh_novokuznetsk.kzmn_dwh_dim_drivers_hist t on s.personnel_num = t.personnel_num) ) ;""" )

        cursor.execute( """update dwh_novokuznetsk.kzmn_dwh_dim_drivers_hist
                            set 
                                end_dt = ttemp.end_dt
                            from (
                                select
                                    a.personnel_num, a.last_name, a.first_name, a.middle_name, a.birth_dt, a.card_num, a.driver_license_num, a.driver_license_dt, 
                                    coalesce((lead ( a.start_dt ) over ( partition by a.personnel_num order by a.start_dt ) - interval '1 day' ), '9999-12-31' ) as end_dt 
                                from dwh_novokuznetsk.kzmn_dwh_dim_drivers_hist a
                                inner join dwh_novokuznetsk.kzmn_dwh_dim_drivers_hist b
                                    on a.personnel_num = b.personnel_num
                                    and b.end_dt is null
                            ) ttemp
                        where ( kzmn_dwh_dim_drivers_hist.personnel_num = ttemp.personnel_num
                                 and kzmn_dwh_dim_drivers_hist.last_name = ttemp.last_name and kzmn_dwh_dim_drivers_hist.first_name = ttemp.first_name and kzmn_dwh_dim_drivers_hist.middle_name = ttemp.middle_name and kzmn_dwh_dim_drivers_hist.birth_dt = ttemp.birth_dt and kzmn_dwh_dim_drivers_hist.card_num = ttemp.card_num and kzmn_dwh_dim_drivers_hist.driver_license_num = ttemp.driver_license_num and kzmn_dwh_dim_drivers_hist.driver_license_dt = ttemp.driver_license_dt ) ;""" )

        # 6. Удаление удаленных записей (формат SCD2)
        cursor.execute( """insert into dwh_novokuznetsk.kzmn_dwh_dim_drivers_hist
                                ( personnel_num, last_name, first_name, middle_name, birth_dt, card_num, driver_license_num, driver_license_dt, start_dt, end_dt, deleted_flag, processed_dt )
                                    select 
                                        personnel_num, last_name, first_name, middle_name, birth_dt, card_num, driver_license_num, driver_license_dt ,
                                        cast( now() as timestamp ) ,
                                        cast( null as timestamp ) , 
                                        'Y' ,
                                        now()
                                    from dwh_novokuznetsk.kzmn_dwh_dim_drivers_hist
                                    where personnel_num in (
                                        select
                                            t.personnel_num
                                        from dwh_novokuznetsk.kzmn_dwh_dim_drivers_hist t 
                                        left join dwh_novokuznetsk.kzmn_stg_del_drivers s
                                            on t.personnel_num = s.personnel_num
                                            where s.personnel_num is null
                                            )
                                        and end_dt = '9999-12-31'
                                        and deleted_flag = 'N' ;""" )

        cursor.execute( """update dwh_novokuznetsk.kzmn_dwh_dim_drivers_hist
                            set 
                                end_dt = ttemp.end_dt
                            from (
                                select
                                personnel_num, last_name, first_name, middle_name, birth_dt, card_num, driver_license_num, driver_license_dt ,
                                end_dt
                                from (	select
                                        a.personnel_num, a.last_name, a.first_name, a.middle_name, a.birth_dt, a.card_num, a.driver_license_num, a.driver_license_dt, 
                                        a.end_dt as dt ,
                                        coalesce((lead ( a.start_dt ) over ( partition by a.personnel_num order by a.start_dt ) - interval '1 day' ), '9999-12-31' ) as end_dt
                                        from dwh_novokuznetsk.kzmn_dwh_dim_drivers_hist a
                                        inner join dwh_novokuznetsk.kzmn_dwh_dim_drivers_hist b
                                            on a.personnel_num = b.personnel_num
                                            and b.end_dt is null ) sq
                                where dt = '9999-12-31'
                            ) ttemp
                        where kzmn_dwh_dim_drivers_hist.personnel_num = ttemp.personnel_num
                            and ( kzmn_dwh_dim_drivers_hist.last_name = ttemp.last_name  and kzmn_dwh_dim_drivers_hist.personnel_num = ttemp.personnel_num and kzmn_dwh_dim_drivers_hist.last_name = ttemp.last_name and kzmn_dwh_dim_drivers_hist.first_name = ttemp.first_name and kzmn_dwh_dim_drivers_hist.middle_name = ttemp.middle_name and kzmn_dwh_dim_drivers_hist.birth_dt = ttemp.birth_dt and kzmn_dwh_dim_drivers_hist.card_num = ttemp.card_num and kzmn_dwh_dim_drivers_hist.driver_license_num = ttemp.driver_license_num and kzmn_dwh_dim_drivers_hist.driver_license_dt = ttemp.driver_license_dt )
                                and kzmn_dwh_dim_drivers_hist.end_dt  = '9999-12-31' ;""" )
        cursor.execute( """update dwh_novokuznetsk.kzmn_dwh_dim_drivers_hist
                            set 
                                end_dt = ttemp.end_dt
                            from (
                                select
                                    personnel_num, last_name, first_name, middle_name, birth_dt, card_num, driver_license_num, driver_license_dt ,
                                    start_dt ,
                                    coalesce(end_dt, '9999-12-31') as end_dt ,
                                    deleted_flag
                                from dwh_novokuznetsk.kzmn_dwh_dim_drivers_hist
                                where deleted_flag = 'Y'
                            ) ttemp
                        where kzmn_dwh_dim_drivers_hist.personnel_num = ttemp.personnel_num
                            and ( kzmn_dwh_dim_drivers_hist.last_name = ttemp.last_name  and kzmn_dwh_dim_drivers_hist.personnel_num = ttemp.personnel_num and kzmn_dwh_dim_drivers_hist.last_name = ttemp.last_name and kzmn_dwh_dim_drivers_hist.first_name = ttemp.first_name and kzmn_dwh_dim_drivers_hist.middle_name = ttemp.middle_name and kzmn_dwh_dim_drivers_hist.birth_dt = ttemp.birth_dt and kzmn_dwh_dim_drivers_hist.card_num = ttemp.card_num and kzmn_dwh_dim_drivers_hist.driver_license_num = ttemp.driver_license_num and kzmn_dwh_dim_drivers_hist.driver_license_dt = ttemp.driver_license_dt )
                                and kzmn_dwh_dim_drivers_hist.end_dt is null ;""" )

        # 7. Обновление метаданных
        cursor.execute( """update dwh_novokuznetsk.kzmn_meta_drivers
                                set last_update_dt = coalesce( ( select max(update_dt)
                                                                from dwh_novokuznetsk.kzmn_stg_drivers ),
                                                                    ( select last_update_dt
                                                                    from dwh_novokuznetsk.kzmn_meta_drivers
                                                                    where schema_name='dwh_novokuznetsk'
                                                                        and table_name='kzmn_stg_source_drivers' ) )
                                where schema_name='dwh_novokuznetsk'
                                    and table_name='kzmn_stg_source_drivers' ;""" )

        # 8. Фиксация транзакции
        #conn.commit()

    except Exception:
        conn.rollback()
        print( 'drivers_scd2_load --> something went wrong' )

    return 1


def clients_entity( conn, cursor ):
    # Создание в хранилище сущности "clients" :

    # Создание искусственного источника (сущности "clients")
    try:
        cursor.execute( """ create table dwh_novokuznetsk.kzmn_stg_source_clients (
                                phone_num char(18), card_num char(19), 
                                dt timestamp(0) ) """ )
        print( '! created table dwh_novokuznetsk.kzmn_stg_source_clients' )
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание target-таблицы (сущности "clients")
    try:
        cursor.execute( """ create table dwh_novokuznetsk.kzmn_dwh_dim_clients_hist (
                                phone_num char(18) ,
                                start_dt timestamp(0) ,
                                card_num char(19), 
                                deleted_flag char(1) ,
                                end_dt timestamp(0) ,
                                processed_dt timestamp(0) ) """ )
        print( '! created table dwh_novokuznetsk.kzmn_dwh_dim_clients_hist' )
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание stg-таблицы
    try:
        cursor.execute( """ create table dwh_novokuznetsk.kzmn_stg_clients (
                                phone_num char(18), card_num char(19), 
                                update_dt timestamp(0) ) """ )
        print( '! created table dwh_novokuznetsk.kzmn_stg_clients' )
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание stg_del-таблицы
    try:
        cursor.execute( """ create table dwh_novokuznetsk.kzmn_stg_del_clients (
                                phone_num char(18) ) """ )
        print( '! created table dwh_novokuznetsk.kzmn_stg_del_clients' )
    except psycopg2.errors.DuplicateTable:
        conn.rollback()
        
    # Создание stg_del_add-таблицы
    try:
        cursor.execute( """ create table dwh_novokuznetsk.kzmn_stg_del_add_clients (
                                phone_num char(18) ) """ )
        print( '! created table dwh_novokuznetsk.kzmn_stg_del_clients' )
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание meta-таблицы
    try:
        cursor.execute( """ create table dwh_novokuznetsk.kzmn_meta_clients (
                                schema_name varchar(20) ,
                                table_name varchar(60) ,
                                last_update_dt timestamp(0) ) """ )
        cursor.execute( """ insert into dwh_novokuznetsk.kzmn_meta_clients
                                        ( schema_name, table_name, last_update_dt )
                                    values
                                        ( 'dwh_novokuznetsk','kzmn_stg_source_clients', to_timestamp('1900-01-01','YYYY-MM-DD') ) """ )
        print( '! created table dwh_novokuznetsk.kzmn_meta_clients' )
    except psycopg2.errors.DuplicateTable:
        conn.rollback()
    conn.commit()  # необходимо зафиксировать транзакцию в любом состоянии

    return 1


def clients_scd2_load( conn, cursor ):
    # Последовательная инкрементальная выгрузка данных сущности "clients" :

    try:
        # 1. Очистка стейджинговых таблиц
        cursor.execute( """delete from dwh_novokuznetsk.kzmn_stg_clients ;""" )

        # 2. Захват данных из источника (измененных с момента последней загрузки) в стейджинг
        cursor.execute( """insert into dwh_novokuznetsk.kzmn_stg_clients
                                        ( phone_num, card_num, update_dt )
                                            select
                                                phone_num, card_num ,
                                                dt
                                            from dwh_novokuznetsk.kzmn_stg_source_clients
                                            where dt > ( select last_update_dt
                                                        from dwh_novokuznetsk.kzmn_meta_clients
                                                        where schema_name='dwh_novokuznetsk'
                                                            and table_name='kzmn_stg_source_clients' ) ;""" )
        cursor.execute( """ update dwh_novokuznetsk.kzmn_stg_clients as stg
                                set update_dt = s.dt
                                    from dwh_novokuznetsk.kzmn_stg_source_clients s
                                    where stg.phone_num = s.phone_num ;""" )

        # 3. Захват полного среза ключей для вычисления удалений
        cursor.execute( """insert into dwh_novokuznetsk.kzmn_stg_del_clients
                                        ( phone_num )
                                            select
                                                phone_num
                                            from dwh_novokuznetsk.kzmn_stg_source_clients ;""" )

        # 4. Загрузка в приемник "вставок" на источнике (формат SCD2)
        cursor.execute( """insert into dwh_novokuznetsk.kzmn_dwh_dim_clients_hist
                                        ( phone_num, card_num, start_dt, end_dt, deleted_flag, processed_dt )
                                            select
                                                s.phone_num, s.card_num, 
                                                s.update_dt ,
                                                coalesce(cast (lead ( s.update_dt) over ( partition by s.phone_num order by s.update_dt ) - interval '1 second' as timestamp ), '9999-12-31 00:00:00' ) , 
                                                'N' ,
                                                now()
                                            from dwh_novokuznetsk.kzmn_stg_clients s
                                            left join dwh_novokuznetsk.kzmn_dwh_dim_clients_hist t 
                                            on s.phone_num = t.phone_num
                                            where t.phone_num is null ;""" )

        # 5. Обновление в приемнике "обновлений" на источнике (формат SCD2, с учетом фиктивных обновлений)
        cursor.execute( """insert into dwh_novokuznetsk.kzmn_dwh_dim_clients_hist
                                        ( phone_num, card_num, start_dt, end_dt, deleted_flag, processed_dt ) 
                                            select distinct 
                                                s.phone_num, s.card_num, 
                                                s.update_dt ,
                                                cast(null as timestamp) ,
                                                'N' ,
                                                now()
                                            from dwh_novokuznetsk.kzmn_stg_clients s
                                            left join dwh_novokuznetsk.kzmn_dwh_dim_clients_hist t
                                                on s.phone_num = t.phone_num
                                            where 1=0
                                            ----
                                            or ( (s.card_num <> t.card_num or (s.card_num is null and t.card_num is not null) or (s.card_num is not null and t.card_num is null))
                                                and s.card_num not in (select t.card_num from dwh_novokuznetsk.kzmn_stg_clients s inner join dwh_novokuznetsk.kzmn_dwh_dim_clients_hist t on s.phone_num = t.phone_num) ) ;""" )

        cursor.execute( """update dwh_novokuznetsk.kzmn_dwh_dim_clients_hist
                                    set 
                                        end_dt = ttemp.end_dt
                                    from (
                                        select
                                            a.phone_num, a.card_num, 
                                            coalesce((lead ( a.start_dt ) over ( partition by a.phone_num order by a.start_dt ) - interval '1 second' ), '9999-12-31' ) as end_dt
                                        from dwh_novokuznetsk.kzmn_dwh_dim_clients_hist a
                                        inner join dwh_novokuznetsk.kzmn_dwh_dim_clients_hist b
                                            on a.phone_num = b.phone_num
                                            and b.end_dt is null
                                    ) ttemp
                                where ( kzmn_dwh_dim_clients_hist.phone_num = ttemp.phone_num
                                         and kzmn_dwh_dim_clients_hist.card_num = ttemp.card_num ) ;""" )

        # 6. Удаление удаленных записей (формат SCD2)
        cursor.execute( """insert into dwh_novokuznetsk.kzmn_dwh_dim_clients_hist
                                        ( phone_num, card_num, start_dt, end_dt, deleted_flag, processed_dt )
                                            select 
                                                phone_num, card_num ,
                                                cast( now() as timestamp ) ,
                                                cast( null as timestamp ) , 
                                                'Y' ,
                                                now()
                                            from dwh_novokuznetsk.kzmn_dwh_dim_clients_hist
                                            where phone_num in (
                                                select
                                                    t.phone_num
                                                from dwh_novokuznetsk.kzmn_dwh_dim_clients_hist t 
                                                left join dwh_novokuznetsk.kzmn_stg_del_clients s
                                                    on t.phone_num = s.phone_num
                                                    where s.phone_num is null
                                                    )
                                                and end_dt = '9999-12-31'
                                                and deleted_flag = 'N' ;""" )

        cursor.execute( """update dwh_novokuznetsk.kzmn_dwh_dim_clients_hist
                                    set 
                                        end_dt = ttemp.end_dt
                                    from (
                                        select
                                        phone_num, card_num ,
                                        end_dt
                                        from (	select
                                                a.phone_num, a.card_num, 
                                                a.end_dt as dt ,
                                                coalesce((lead ( a.start_dt ) over ( partition by a.phone_num order by a.start_dt ) - interval '1 second' ), '9999-12-31' ) as end_dt
                                                from dwh_novokuznetsk.kzmn_dwh_dim_clients_hist a
                                                inner join dwh_novokuznetsk.kzmn_dwh_dim_clients_hist b
                                                    on a.phone_num = b.phone_num
                                                    and b.end_dt is null ) sq
                                        where dt = '9999-12-31'
                                    ) ttemp
                                where kzmn_dwh_dim_clients_hist.phone_num = ttemp.phone_num
                                    and ( kzmn_dwh_dim_clients_hist.card_num = ttemp.card_num  and kzmn_dwh_dim_clients_hist.phone_num = ttemp.phone_num and kzmn_dwh_dim_clients_hist.card_num = ttemp.card_num )
                                        and kzmn_dwh_dim_clients_hist.end_dt  = '9999-12-31' ;""" )
        cursor.execute( """update dwh_novokuznetsk.kzmn_dwh_dim_clients_hist
                                    set 
                                        end_dt = ttemp.end_dt
                                    from (
                                        select
                                            phone_num, card_num ,
                                            start_dt ,
                                            coalesce(end_dt, '9999-12-31') as end_dt ,
                                            deleted_flag
                                        from dwh_novokuznetsk.kzmn_dwh_dim_clients_hist
                                        where deleted_flag = 'Y'
                                    ) ttemp
                                where kzmn_dwh_dim_clients_hist.phone_num = ttemp.phone_num
                                    and ( kzmn_dwh_dim_clients_hist.card_num = ttemp.card_num  and kzmn_dwh_dim_clients_hist.phone_num = ttemp.phone_num and kzmn_dwh_dim_clients_hist.card_num = ttemp.card_num )
                                        and kzmn_dwh_dim_clients_hist.end_dt is null ;""" )

        # 7. Обновление метаданных
        cursor.execute( """update dwh_novokuznetsk.kzmn_meta_clients
                                        set last_update_dt = coalesce( ( select max(update_dt)
                                                                        from dwh_novokuznetsk.kzmn_stg_clients ),
                                                                            ( select last_update_dt
                                                                            from dwh_novokuznetsk.kzmn_meta_clients
                                                                            where schema_name='dwh_novokuznetsk'
                                                                                and table_name='kzmn_stg_source_clients' ) )
                                        where schema_name='dwh_novokuznetsk'
                                            and table_name='kzmn_stg_source_clients' ;""" )

        # 8. Фиксация транзакции
        #conn.commit()

    except Exception:
        conn.rollback()
        print( 'clients_scd2_load --> something went wrong' )

    return 1


def payments_entity( conn, cursor ):
    # Создание в хранилище сущности "payments" :
    
    # Создание stg-источника (сущности "payments")
    try:
        cursor.execute( """ create table dwh_novokuznetsk.kzmn_stg_payments ( 
                                    transaction_id varchar(9), card_num char(16), transaction_amt numeric(5,2), transaction_dt timestamp(0) ) """ )
        print('! created table dwh_novokuznetsk.kzmn_stg_payments')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание фактовой target-таблицы (сущности "payments")
    try:
        cursor.execute( """ create table dwh_novokuznetsk.kzmn_dwh_fact_payments (
                                transaction_id varchar(9), card_num char(19), transaction_amt numeric(5,2), transaction_dt timestamp(0),
                                processed_dt timestamp(0) ) """ )
        print('! created table dwh_novokuznetsk.kzmn_dwh_fact_payments')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()  
    conn.commit()  # необходимо зафиксировать транзакцию в любом состоянии

    return 1


def payments_fact_load( conn, cursor ):
    # Фактовая выгрузка сущности "payments" :

    try:
        # 1. Загрузка новых значений из стейджинга
        cursor.execute( """ insert into dwh_novokuznetsk.kzmn_dwh_fact_payments
                                    ( transaction_id, card_num, transaction_amt, transaction_dt, processed_dt )
                                        select
                                            s.transaction_id,
                                            substring( cast(s.card_num as varchar) , 1, 4 ) || ' ' || substring( cast(s.card_num as varchar) , 5, 4 ) || ' ' || substring( cast(s.card_num as varchar) , 9, 4 ) || ' ' || substring( cast(s.card_num as varchar) , 13, 4 ) ,
                                            s.transaction_amt,
                                            s.transaction_dt ,
                                            now()
                                        from dwh_novokuznetsk.kzmn_stg_payments s
                                        left join dwh_novokuznetsk.kzmn_dwh_fact_payments t 
                                        on s.transaction_id = t.transaction_id
                                        where t.transaction_id is null """ )

        # 2. Дополнительная очистка стейджинга-источника
        cursor.execute( """ delete from dwh_novokuznetsk.kzmn_stg_payments """ )

        # 3. Фиксация транзакции
        #conn.commit()

    except Exception:
        conn.rollback()
        print( 'payments_fact_load --> something went wrong' )

    return 1


def waybills_entity( conn, cursor ):
    # Создание в хранилище сущности "waybills" :

    # Создание stg-источника (сущности "waybills")
    try:
        cursor.execute( """ create table dwh_novokuznetsk.kzmn_stg_waybills ( 
                                    waybill_num char(6), driver_pers_num char(5), car_plate_num char(9), work_start_dt timestamp(0), work_end_dt timestamp(0), issue_dt timestamp(0) ) """ )
        print('! created table dwh_novokuznetsk.kzmn_stg_waybills')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()
        
    # Создание дополнительного stg-источника (сущности "waybills")
    try:
        cursor.execute( """ create table dwh_novokuznetsk.kzmn_stg_add_waybills ( 
                                    waybill_num char(6), driver_pers_num char(5), car_plate_num char(9), work_start_dt timestamp(0), work_end_dt timestamp(0), issue_dt timestamp(0), license char(12) ) """ )
        print('! created table dwh_novokuznetsk.kzmn_stg_add_waybills')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание фактовой target-таблицы (сущности "waybills")
    try:
        cursor.execute( """ create table dwh_novokuznetsk.kzmn_dwh_fact_waybills (
                                waybill_num char(6), driver_pers_num char(5), car_plate_num char(9), work_start_dt timestamp(0), work_end_dt timestamp(0), issue_dt timestamp(0),
                                processed_dt timestamp(0) ) """ )
        print('! created table dwh_novokuznetsk.kzmn_dwh_fact_waybills')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()  
    conn.commit()  # необходимо зафиксировать транзакцию в любом состоянии

    return 1


def waybills_fact_load( conn, cursor ):
    # Фактовая выгрузка сущности "waybills" :

    try:
        # 0. Выгрузка ключей в стейджинг
        cursor.execute( """ update dwh_novokuznetsk.kzmn_stg_add_waybills
                                        set driver_pers_num = coalesce( ( select
                                                                        distinct
                                                                        personnel_num
                                                                    from dwh_novokuznetsk.kzmn_dwh_dim_drivers_hist d
                                                                    inner join dwh_novokuznetsk.kzmn_stg_add_waybills w
                                                                    on d.driver_license_num = w.license ), 'XXXXX' )
                                        where driver_pers_num is null """ )

        # 1. Группировка в дополнительном стейджинге
        cursor.execute( """ insert into dwh_novokuznetsk.kzmn_stg_waybills
                                ( waybill_num, driver_pers_num, car_plate_num, work_start_dt, work_end_dt, issue_dt )
                                    select
                                        max(waybill_num) ,
                                        max(driver_pers_num) ,
                                        max(car_plate_num) ,
                                        max(work_start_dt) ,
                                        max(work_end_dt) ,
                                        max(issue_dt)
                                    from dwh_novokuznetsk.kzmn_stg_add_waybills group by waybill_num """ )
        
        # 1. Загрузка новых значений из стейджинга
        cursor.execute( """ insert into dwh_novokuznetsk.kzmn_dwh_fact_waybills
                                ( waybill_num, driver_pers_num, car_plate_num, work_start_dt, work_end_dt, issue_dt, processed_dt )
                                    select
                                        s.waybill_num, s.driver_pers_num, s.car_plate_num, s.work_start_dt, s.work_end_dt, s.issue_dt ,
                                        now()
                                    from dwh_novokuznetsk.kzmn_stg_waybills s
                                    left join dwh_novokuznetsk.kzmn_dwh_fact_waybills t 
                                    on s.waybill_num = t.waybill_num
                                    where t.waybill_num is null """ )

        # 2. Дополнительная очистка стейджинга-источников
        cursor.execute( """ delete from dwh_novokuznetsk.kzmn_stg_add_waybills """ )
        cursor.execute( """ delete from dwh_novokuznetsk.kzmn_stg_waybills """ )

        # 3. Фиксация транзакции
        #conn.commit()

    except Exception :
        conn.rollback()
        print( 'waybills_fact_load --> something went wrong' )

    return 1


def rides_entity( conn, cursor ):
    # Создание в хранилище сущности "rides" :

    # Создание первой stg1-таблицы
    try:
        cursor.execute( """ create table dwh_novokuznetsk.kzmn_stg1_rides ( 
                                    ride_id varchar(10), point_from_txt text, point_to_txt text, distance_val numeric(5,2), price_amt numeric(7,2), client_phone_num char(18) ) """ )
        print('! created table dwh_novokuznetsk.kzmn_stg1_rides')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()
    
    # Создание второй stg2-таблицы
    try:
        cursor.execute( """ create table dwh_novokuznetsk.kzmn_stg2_rides ( 
                                    ride_id varchar(10), car_plate_num char(9), ride_arrival_dt timestamp(0), ride_start_dt timestamp(0), ride_end_dt timestamp(0) ) """ )
        print('! created table dwh_novokuznetsk.kzmn_stg2_rides')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()
    
    # Создание stg-источника (сущности "rides")
    try:
        cursor.execute( """ create table dwh_novokuznetsk.kzmn_stg_rides ( 
                                    ride_id varchar(10), point_from_txt text, point_to_txt text, distance_val numeric(5,2), price_amt numeric(7,2), client_phone_num char(18), driver_pers_num char(5), car_plate_num char(9), ride_arrival_dt timestamp(0), ride_start_dt timestamp(0), ride_end_dt timestamp(0) ) """ )
        print('! created table dwh_novokuznetsk.kzmn_stg_rides')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()

    # Создание фактовой target-таблицы (сущности "rides")
    try:
        cursor.execute( """ create table dwh_novokuznetsk.kzmn_dwh_fact_rides (
                                ride_id varchar(10), point_from_txt text, point_to_txt text, distance_val numeric(5,2), price_amt numeric(7,2), client_phone_num char(18), driver_pers_num char(5), car_plate_num char(9), ride_arrival_dt timestamp(0), ride_start_dt timestamp(0), ride_end_dt timestamp(0),
                                processed_dt timestamp(0) ) """ )
        print('! created table dwh_novokuznetsk.kzmn_dwh_fact_rides')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()
    
    # Создание meta-таблицы
    try:
        cursor.execute( """ create table dwh_novokuznetsk.kzmn_meta_rides (
                            schema_name varchar(20) ,
                            table_name varchar(60) ,
                            last_update_dt timestamp(0) ) """ )
        cursor.execute( """ insert into dwh_novokuznetsk.kzmn_meta_rides
                                    ( schema_name, table_name, last_update_dt )
                                values
                                    ( 'dwh_novokuznetsk','kzmn_stg_rides', to_timestamp('1900-01-01','YYYY-MM-DD') ) """ )
        print('! created table dwh_novokuznetsk.kzmn_meta_rides')
    except psycopg2.errors.DuplicateTable:
        conn.rollback()
    conn.commit()  # необходимо зафиксировать транзакцию в любом состоянии

    return 1


def rides_fact_load( conn, cursor ):
    # Фактовая выгрузка сущности "rides" :

    try:
        # 0. Формирование данных стейджинга
        cursor.execute( """ insert into dwh_novokuznetsk.kzmn_stg_rides
                                    ( ride_id, point_from_txt, point_to_txt, distance_val, price_amt, client_phone_num, car_plate_num, ride_arrival_dt, ride_start_dt, ride_end_dt )
                                        select
                                            s1.ride_id ,
                                            s1.point_from_txt ,
                                            s1.point_to_txt ,
                                            s1.distance_val ,
                                            s1.price_amt ,
                                            s1.client_phone_num ,
                                            s2.car_plate_num ,
                                            s2.ride_arrival_dt ,
                                            s2.ride_start_dt ,
                                            s2.ride_end_dt 
                                        from dwh_novokuznetsk.kzmn_stg1_rides s1
                                        inner join dwh_novokuznetsk.kzmn_stg2_rides s2
                                        on s1.ride_id = s2.ride_id """ )
        cursor.execute( """ update dwh_novokuznetsk.kzmn_stg_rides as stg 
                                set driver_pers_num = s.driver_pers_num
                                    from dwh_novokuznetsk.kzmn_dwh_fact_waybills s
                                    where s.driver_pers_num <> 'XXXXX'
                                        and (stg.car_plate_num = s.car_plate_num
                                        and stg.ride_end_dt between s.work_start_dt and s.work_end_dt) """ )
        cursor.execute( """ delete from dwh_novokuznetsk.kzmn_stg_rides
                            where driver_pers_num is null """ )
        
        # 1. Загрузка новых значений из стейджинга
        cursor.execute( """ insert into dwh_novokuznetsk.kzmn_dwh_fact_rides
                                    ( ride_id, point_from_txt, point_to_txt, distance_val, price_amt, client_phone_num, driver_pers_num, car_plate_num, ride_arrival_dt, ride_start_dt, ride_end_dt, processed_dt )
                                        select
                                            s.ride_id, s.point_from_txt, s.point_to_txt, s.distance_val, s.price_amt, s.client_phone_num, s.driver_pers_num, s.car_plate_num, s.ride_arrival_dt, s.ride_start_dt, s.ride_end_dt ,
                                            now()
                                        from dwh_novokuznetsk.kzmn_stg_rides s
                                        left join dwh_novokuznetsk.kzmn_dwh_fact_rides t 
                                        on s.ride_id = t.ride_id
                                        where t.ride_id is null """ )

        # 2. Дополнительная очистка стейджинга-источника
        cursor.execute( """ delete from dwh_novokuznetsk.kzmn_stg1_rides """ )
        cursor.execute( """ delete from dwh_novokuznetsk.kzmn_stg2_rides """ )
        cursor.execute( """ delete from dwh_novokuznetsk.kzmn_stg_rides """ )

        # 3. Фиксация транзакции
        #conn.commit()

    except Exception:
        conn.rollback()
        print( 'rides_fact_load --> something went wrong' )

    return 1

