import json
import pathlib
import airflow
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import os.path
import pendulum
from collections import defaultdict
dag=DAG(
    dag_id="lotto",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval="@daily",
)

download_launches=BashOperator(
    task_id="download_launches",
    bash_command='curl -o /tmp/launches.json -L "https://smok95.github.io/lotto/results/all.json "',dag=dag,
)


def _get_lotto_data():
    pathlib.Path("/tmp/lottos").mkdir(parents=True,exist_ok=True)
    if not os.path.isfile('/tmp/lottos/lottodata.csv'):
        lotto=defaultdict(list)
        
        with open("/tmp/launches.json") as f:
            launches=json.load(f)
            for data in launches:
                lotto['draw_no'].append(data['draw_no'])
                lotto['date'].append(data['date'])
                lotto['number_1'].append(data['numbers'][0])
                lotto['number_2'].append(data['numbers'][1])
                lotto['number_3'].append(data['numbers'][2])
                lotto['number_4'].append(data['numbers'][3])
                lotto['number_5'].append(data['numbers'][4])
                lotto['number_6'].append(data['numbers'][5])
                lotto['bonus_no'].append(data['bonus_no'])
        df=pd.DataFrame(data=lotto)
        df.to_csv('/tmp/lottos/lottodata.csv')
        return 0
    df=pd.read_csv('/tmp/lottos/lottodata.csv',index_col=0)
    l=list(df['draw_no'])
    #모든회차의 json을 불러온다음 
    url=f'https://smok95.github.io/lotto/results/all.json'
    resp=requests.get(url)
    latest_data=resp.json()
    # 만약 latest_data 최신회차랑 내 csv파일의 최신회차가 다르다면 업데이트
    no=latest_data[-1]['draw_no']
    # now : 현재 내가 가지고있는 csv의 회차 목록
    now=list(df['draw_no'])
    if no==now[-1]:
        return 0
    for i in range(now[-1]+1,no+1):
        url=f'https://smok95.github.io/lotto/results/{i}.json'

        resp=requests.get(url)
        latest_data=resp.json()

        lotto=defaultdict(list)
        lotto['draw_no'].append(latest_data['draw_no'])
        lotto['date'].append(latest_data['date'])
        lotto['number_1'].append(latest_data['numbers'][0])
        lotto['number_2'].append(latest_data['numbers'][1])
        lotto['number_3'].append(latest_data['numbers'][2])
        lotto['number_4'].append(latest_data['numbers'][3])
        lotto['number_5'].append(latest_data['numbers'][4])
        lotto['number_6'].append(latest_data['numbers'][5])
        lotto['bonus_no'].append(latest_data['bonus_no'])
        df1=pd.DataFrame(data=lotto)
        df=pd.concat([df,df1],ignore_index=True)
    df.to_csv('/tmp/lottos/lottodata.csv')

get_lotto_data=PythonOperator(
    task_id='get_lottos_data',
    python_callable=_get_lotto_data,
    dag=dag,
)



notify=BashOperator(
    task_id='notify',
    bash_command='echo "there are now $( ls /tmp/lottos/ | wc -l) files."',
    dag=dag,
)


download_launches>>get_lotto_data>>notify






