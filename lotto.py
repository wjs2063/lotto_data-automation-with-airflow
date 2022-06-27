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
from collections import defaultdict
dag=DAG(
    # DAG 이름
    dag_id="lotto",
    #시작날짜
    start_date=airflow.utils.dates.days_ago(300),
    #스케쥴 실행간격
    schedule_interval="@weekly",
)

download_launches=BashOperator(
    # task _id 설정 
    task_id="download_launches",
    # 모든회차의 로또번호를 json 형식으로 가져온다
    bash_command='curl -o /tmp/launches.json -L "https://smok95.github.io/lotto/results/all.json "',dag=dag,
)


def _get_lotto_data():
    pathlib.Path("/tmp/lottos").mkdir(parents=True,exist_ok=True)
    # 로또파일이 존재하지않으면 json 형태를 dataframe 으로 저장
    if not os.path.isfile('/tmp/lottos/lottodata.csv'):
        # launches.json 에있는 모든 로또 번호, 당첨금 등등을 저장
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
        #csv 파일로 저장
        df.to_csv('/tmp/lottos/lottodata.csv')
        return 0
    # 데이터가 존재한다면 csv파일을 불러와서 최신회차 데이터만 추가한후 다시 저장
    df=pd.read_csv('/tmp/lottos/lottodata.csv')
    l=list(df['draw_no'])
    url=f'https://www.dhlottery.co.kr/common.do?method=getLottoNumber&drwNo={int(l[-1])+1}'
    resp=requests.get(url)
    latest_data=resp.json()
    # 만약 최신회차가 아직없다면?
    if latest_data['returnValue']=='fail':
        print('아직 로또추첨이 완료되지않았거나 업로드되지않았습니다')
        return 0
    lotto=defaultdict(list)
    lotto['draw_no'].append(latest_data['draw_no'])
    lotto['date'].append(data['date'])
    lotto['number_1'].append(latest_data['numbers'][0])
    lotto['number_2'].append(latest_data['numbers'][1])
    lotto['number_3'].append(latest_data['numbers'][2])
    lotto['number_4'].append(latest_data['numbers'][3])
    lotto['number_5'].append(latest_data['numbers'][4])
    lotto['number_6'].append(latest_data['numbers'][5])
    lotto['bonus_no'].append(latest_data['bonus_no'])
    df.append(lotto)
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



