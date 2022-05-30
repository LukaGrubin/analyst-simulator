#импортируем библиотеки
from airflow import DAG
from airflow.operators.python import PythonOperator 
from datetime import datetime, timedelta
import requests
import pandas as pd
import pandahouse as ph
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
import connection_info as ci

#задаем аргументы и расписание для ДАГа
default_args = {
    'owner': 'l-grubin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 5, 13),
}

schedule_interval = '0 23 * * *'

# подключаемся к БД 
def ch_get_df(query):
    connection = {'host': ci.host,                   #ссылка на clickhouse
                      'database':ci.database,    #конкретная ДБ в clickhose
                      'user':ci.user,
                      'password':ci.password
                     }

    result = ph.read_clickhouse(query, connection=connection_read)

    return result

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_l_grubin():
    
    @task
    # отбираем данные из ленты
    def extract_fa():
        query = """
                SELECT 
                    user_id,
                    toDate(time) as event_date,
                    gender,
                    age,
                    os,
                    countIf(user_id, action='like') as likes,
                    countIf(user_id, action='view') as views
                FROM {db}.feed_actions
                where 
                        toDate(time) = yesterday() 
                GROUP BY user_id, toDate(time), gender, age, os
                """
        df_feed_actions = ch_get_df(query=query)
        return df_feed_actions
    
    
    @task
    # отбираем данные из мессенджера
    def extract_ma():
        query =  """
                select event_date, user_id, sum(sent) AS messages_sent, sum(recieved) AS messages_received, sum(recipients) AS users_sent,  
                sum(senders) AS users_received,
                gender,
                age,
                os  
                from 
                (SELECT toDate(time) as event_date,
                                    user_id,
                                    count(reciever_id) as sent,
                                    count(distinct reciever_id) as recipients,
                                    gender,
                                    age,
                                    os  
                FROM {db}.message_actions
                group by user_id, toDate(time), gender, age, os) as tu
                join
                (SELECT toDate(time),
                                    reciever_id,
                                    count(user_id) as recieved,
                                    count(distinct user_id) as senders,
                                    gender,
                                    age,
                                    os  
                FROM {db}.message_actions
                group by reciever_id, toDate(time), gender, os, age) as tm 
                on tu.user_id = tm.reciever_id
                where event_date = yesterday()
                group by user_id, event_date, gender, os, age
                order by user_id
                """
        df_message_actions = ch_get_df(query=query)
        return df_message_actions
    
    @task
    #группируем в одну таблицу
    def merge(df_feed_actions, df_message_actions):
        df = pd.merge(df_feed_actions, df_message_actions)
        
        
        return df
    
    # отбираем срезы по полу, ос и возрасту
    @task
    def transform_gender(df):
        df_gender = df.groupby(['gender', 'event_date']).sum().reset_index()
        df_gender['gender'] = df_gender['gender'].apply(lambda x: 'male' if x == 1 else 'female')
        df_gender['metric'] = 'gender'
        df_gender.rename(columns={'gender':'metric_value'},inplace=True)
        
        return df_gender
    
    @task
    def transform_os(df):
        df_os =  df.groupby(['os', 'event_date']).sum().reset_index()
        df_os['metric'] = 'os'
        df_os.rename(columns={'os':'metric_value'},inplace=True)
        
        return df_os
    
    @task
    def transform_age(df):
        
        def age_cat(df):
            if df < 18:
                return 'under 18'
            elif df <= 25:
                return '18-25'
            elif df <= 35:
                return '26-35'
            elif df <= 50:
                return '36-50'
            else:
                return '50+'
        
        df_age = df.groupby(['age', 'event_date']).sum().reset_index()
        
        df_age['age'] = df_age['age'].apply(age_cat)
        
        df_age = df_age.groupby(['age', 'event_date']).sum().reset_index()
        df_age['metric'] = 'age'
        df_age.rename(columns={'age':'metric_value'},inplace=True)
        
        return df_age
    
    @task
    #объединяем все срезы в один датафрейм
    def concat_tables(df_gender, df_os, df_age):
        df_final = pd.concat([df_gender, df_os, df_age]).reset_index(drop=True)
        df_final = df_final[['event_date','metric','metric_value', 'views', 'likes', 'messages_received', 'messages_sent','users_received', 'users_sent']]

        
        return df_final
    

        
    
    @task
    #загружаем данные в таблицу
    def load(df_final):  

        connection = {'host':  ci.host,,
                      'database':'test',
                      'user':ci.test_user, 
                      'password':ci.test_user_pass
                     }
        
        create = '''CREATE TABLE IF NOT EXISTS test.l_grubin_6_test
        (
        event_date datetime,
        metric TEXT,
        metric_value TEXT,
        views Int64,
        likes Int64,
        messages_received Int64,
        messages_sent Int64,
        users_received Int64,
        users_sent Int64
        ) ENGINE = MergeTree ORDER BY (event_date);
        '''
        ph.execute(create, connection=connection)
        
        
        ph.to_clickhouse(df_final, 'l_grubin_6_test', index=False, connection=connection)
        
    #запускаем таски   
    
    df_message_actions = extract_ma()
    df_feed_actions = extract_fa()
    df = merge(df_feed_actions, df_message_actions)
    df_gender = transform_gender(df)
    df_os = transform_os(df)
    df_age = transform_age(df)
    df_final = concat_tables(df_gender, df_os, df_age)
    load(df_final)
    
    
dag_l_grubin = dag_l_grubin()
