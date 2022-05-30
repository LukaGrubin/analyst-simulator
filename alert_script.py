import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import telegram
import pandahouse
from datetime import date
import io
from read_db.CH import Getch # модуль с информацией о подключении к ДБ
import sys
import os

def check_anomaly(df, metric, a=4, n=4):
    df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
    df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
    df['iqr'] = df['q75'] - df['q25']
    df['top'] =  df['q75'] + a*df['iqr']
    df['bottom'] = df['q25'] - a*df['iqr']
    
    df['top'] = df['top'].rolling(n, center=True, min_periods=1).mean()
    df['bottom'] = df['bottom'].rolling(n, center=True, min_periods=1).mean()
    
    if df[metric].iloc[-1] < df['bottom'].iloc[-1] or df[metric].iloc[-1] > df['top'].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0
    
    return is_alert, df

def run_alerts(chat=None):  
    
    chat_id = chat or os.environ.get("my_chat_id")
    bot = telegram.Bot(token=os.environ.get("REPORT_BOT_TOKEN"))
    
    #выбираем метрики из ленты новостей
    data_feed = Getch(''' SELECT
                          toStartOfFifteenMinutes(time) as ts
                        , toDate(ts) as date
                        , formatDateTime(ts, '%R') as hm
                        , uniqExact(user_id) as feed_users
                        , countIf(user_id, action='view') as views
                        , countIf(user_id, action='like') as likes
                        , countIf(user_id, action='like') /  countIf(user_id, action='view') as CTR
                    FROM simulator_20220420.feed_actions
                    WHERE ts >=  today() - 1 and ts < toStartOfFifteenMinutes(now())
                    GROUP BY ts, date, hm
                    ORDER BY ts ''').df
    #выбираем метрики из мессенджера
    data_message = Getch(''' SELECT
                          toStartOfFifteenMinutes(time) as ts
                        , toDate(ts) as date
                        , formatDateTime(ts, '%R') as hm
                        , uniqExact(user_id) as messenger_users
                        , count(reciever_id) as messages
                    FROM simulator_20220420.message_actions
                    WHERE ts >=  today() - 1 and ts < toStartOfFifteenMinutes(now())
                    GROUP BY ts, date, hm
                    ORDER BY ts ''').df
    
    data = data_feed.merge(data_message)
    metrics_list = ['feed_users', 'views', 'likes', 'CTR', 'messenger_users', 'messages']
    metrics_links = {'feed_users': 'http://superset.lab.karpov.courses/r/886'
                     , 'views' : 'http://superset.lab.karpov.courses/r/887'
                     , 'likes': 'http://superset.lab.karpov.courses/r/890'
                     ,'CTR': 'http://superset.lab.karpov.courses/r/891'
                     , 'messenger_users' : 'http://superset.lab.karpov.courses/r/889'
                     , 'messages': 'http://superset.lab.karpov.courses/r/888'}
# проверяем каждую метрику на наличие анамалий    
    for metric in metrics_list:
        df = data[['ts', 'date', 'hm', metric]].copy()
        is_alert, df = check_anomaly(df, metric)
#формируем отчет об аномалии        
        if is_alert == 1:
            msg = '''Метрика: {metric} в срезе {dt} {tm}. 
                      \nТекущее значение {current_value}. Отклонение более {last_value:.2%}.
                      \nСсылка на чарт: {metric_link}
                      \nСсылка на дашборд: {dashboard_link}
                      \n@{user_name} обрати внимание!'''.format(metric=metric
                                            , dt=df['date'].iloc[-1].date()
                                            , tm=df['hm'].iloc[-1]                 
                                            ,  current_value=round(df[metric].iloc[-1], 2)
                                            , last_value=abs(1 - df[metric].iloc[-1]/df[metric].iloc[-2])
                                            , metric_link=metrics_links[metric]
                                            , dashboard_link='https://superset.lab.karpov.courses/superset/dashboard/653/'
                                            , user_name='Lukas_gr')
#рисуем график
            sns.set(rc={'figure.figsize': (16, 10)}) # задаем размер графика
            plt.tight_layout()

            ax = sns.lineplot(x=df['ts'], y=df[metric], label=f'{metric}')
            ax = sns.lineplot(x=df['ts'], y=df['top'], label=f'> 0.75 quantille', color='y')
            ax = sns.lineplot(x=df['ts'], y=df['bottom'], label=f'< 0.25 quantille', color='y')
            

            for ind, label in enumerate(ax.get_xticklabels()): # этот цикл нужен чтобы разрядить подписи координат по оси Х,
                if ind % 2 == 0:
                    label.set_visible(True)
                else:
                    label.set_visible(False)
            ax.set(xlabel='time') # задаем имя оси Х
            ax.set(ylabel=metric) # задаем имя оси У

            ax.set_title('{}'.format(metric)) # задае заголовок графика
            ax.set(ylim=(0, None)) # задаем лимит для оси У

            # формируем файловый объект
            plot_object = io.BytesIO()
            ax.figure.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = '{0}.png'.format(metric)
            plt.close()

                # отправляем алерт
            bot.sendMessage(chat_id=chat_id, text=msg)
            bot.sendPhoto(chat_id=chat_id, photo=plot_object)

try:
    run_alerts(os.environ.get("my_chat_id")) #отправляем в нужный чат (сохранено в variables CI/CD)
except Exception as e:
    print(e)





