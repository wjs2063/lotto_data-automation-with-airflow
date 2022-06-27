# airflow--
로또데이터 자동화 pipline 구축

## work flow 모형

재현--> Launch(lotto 데이터 가져오기)(download_launches) --> 내 컴퓨터에 데이터 결괏값 저장(_get_lottos_data) --> 시스템알림( notify)   
 
task:  
1.download_launches  
2. _get_lottos_data  
3. notify  

task 의존성:

download_launches>> get_lottos_data >> notify

1번이 완료돼야 2번이 시작돼고 2번이 완료되어야 3번이 시작된다.

# Usage  

### STEP 1.

compose.yml 파일이있는 directory 로 이동후   
    ```  
    docker-compose up -d  
    ```  


### STEP 2.  

scheduler, webserver, postgres(db server), init  이 생성되었을것임 ( docker ps -a 로 확인)    

### STEP 3.  

vscode 로 scheduler 컨테이너 내부환경으로 진입후 /opt/airflow/dags/scheduler/lotto.py 파일생성후 소스코드 작성 

### STEP 4.  
https://localhost:8080 으로 접속 

id, password 는 compose.yml 파일에서 지정한 admin/admin 으로 접속

### STEP 5. 

해당 task 들이 lotto.py 제일 아랫부분에서 설정한 의존성에 따라 순차진행 ( 노드들 색깔로 성공/실패 구분가능) 

실패시 logs 파일에 들어가서 오류메세지를 보고 해결  


## 실패시 나오는 화면 

<img width="1406" alt="스크린샷 2022-06-27 오후 9 48 56" src="https://user-images.githubusercontent.com/76778082/175949547-778ac738-7310-448c-902e-00878518c158.png">




## 성공시 나오는 화면 

<img width="1406" alt="스크린샷 2022-06-27 오후 10 11 55" src="https://user-images.githubusercontent.com/76778082/175949729-00afccc1-03de-4b5b-9afd-d3c3a40dcaa4.png">



## 저장된 로또 데이터 csv 파일 

<img width="1406" alt="스크린샷 2022-06-27 오후 11 54 25" src="https://user-images.githubusercontent.com/76778082/175970701-abc60a7f-be70-425a-982e-0f6e65fb1835.png">



