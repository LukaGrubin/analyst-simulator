#Импортируем библиотеки
import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import pandahouse as ph
import os
import io
import connection_info as ci

#подключаем бота
bot = telegram.Bot(token=os.environ.get("REPORT_BOT_TOKEN")) #тут токен телеграм-бота, сохраненный в variables CI/CD
chat_id = token=os.environ.get("report_chat_id")             #тут id чата сохраненный в variables CI/CD

#подключаемся к базе данных и выбираем нужные данные
connection = {'host': ci.host,                   #ссылка на clickhouse
                      'database':ci.database,    #конкретная ДБ в clickhose
                      'user':ci.user,
                      'password':ci.password
                     }

query = '''
select toDate(time) AS Day, count(distinct user_id) AS DAU, 
countIf(action='view') as Views, countIf(action='like') AS Likes, 
round((countIf(action='like') / countIf(action='view') * 100 ),2) AS CTR  
from {db}.feed_actions 
where toDate(time) > today() -7 and toDate(time) < today()
group by Day;
'''

df = ph.read_clickhouse(query, connection=connection)

#создаем ежедневный отчет
msg = ['DAILY REPORT']
yesterday_report = df.iloc[5]
yesterday_report
for key, value in yesterday_report.items():
    msg.append(f'{key}: {value}')
msg = '\n'.join(msg)

#отправляем отчет
bot.sendMessage(chat_id=chat_id, text=msg)

#создаем график
sns.set()
fig = plt.figure(figsize=(10, 20))
ax_1 = fig.add_subplot(3, 1, 1)
ax_2 = fig.add_subplot(3, 1, 2)
ax_3 = fig.add_subplot(3, 1, 3)

ax_1.plot(df['Day'], df['DAU'],
        color = 'blue',
        linewidth = 3)
ax_2.plot(df['Day'], df['Views'], color='red')
ax_2.plot(df['Day'], df['Likes'], color='orange')
ax_2.legend(['Views', 'Likes'], frameon=False)
ax_3.plot(df['Day'], df['CTR'], color = 'black',
        linewidth = 3)
ax_1.set(title='DAU')
ax_2.set(title='Views and likes')
ax_3.set(title='CTR %')

#отправляем график
plot_object = io.BytesIO()
plt.savefig(plot_object)
plot_object.name = 'test_plot.png'
plot_object.seek(0)
plt.close

bot.sendPhoto(chat_id=chat_id, photo=plot_object)
