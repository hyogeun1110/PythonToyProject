import pandas as pd
import pymysql
from sqlalchemy import create_engine
import os
import getpass
import time

# conn db
intoMyDB= input('hyogeun의 test DB에 넣기(y/n) : ')
if intoMyDB == 'y':
    host = 'localhost'
    user = 'root'
    password = '1234'
    db = 'test'
    port = 3306
else:
    host = input('host : ')      
    user = input('user : ')   
    password = getpass.getpass("Password:")  
    db = input('db : ')  
    port = int(input('port : '))

try:
    conn = pymysql.connect(host=host, user=user, password=password, db=db, port=port)
    engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}?charset=utf8".format(user=user, pw=password, host=host, db=db), encoding='utf-8')
    print('\nDB연결 성공\n')
except :
    print('!! DB 연결 실패')
    time.sleep(3)
    quit()

# 디렉토리 확인
csvFiles = [x for x in os.listdir() if x[-4:] == '.csv']
print('현재 디렉토리의 csv파일 목록 : ' + ",".join(i for i in csvFiles))
if len(csvFiles) == 0 :
    print('csv파일이 없습니다')
    time.sleep(3)
    quit()
elif len(csvFiles) == 1:
    files = list(csvFiles)
else:
    files_ = list(input('DB에 넣을 csv파일 (all 입력시 모든 파일) : ').split(','))
    if files_[0] == 'all':
        files = csvFiles     


for file_name in files:
    # read csv
    print(file_name + ' into_DB 시작...\n')
    fileSize = os.path.getsize(file_name)
    if fileSize >= 314_572_800 : # 300mb 
        df_chunk = pd.read_csv(file_name, chunksize=50000)
        df = pd.concat(df_chunk)
    else : 
        df = pd.read_csv(file_name)

    # table check    
    tables = []
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute('select * from information_schema.tables where table_schema = %s', db)
      
    for row in cur:
        tables.append(row['TABLE_NAME'])

    print('DB에 존재하는 테이블 리스트 : ' + " , ".join(tables))
    table_name = input('csv를 넣을 테이블 이름 : ')
    if table_name not in tables:
        # create table
        print('\n'+table_name + '테이블을 생성합니다')
        columns = df.columns
        col_dtypes = []

        print(columns)
        for column in columns:
            col_dtype = input(table_name + '의 컬럼, ' + column +'의 dtype (1-varchar, 2-double, 3-int) : ')
            if col_dtype == '1':
                size = input('varchar size : ')
                col_dtype = column + " varchar(" + size + ')'
            elif col_dtype =='2':
                col_dtype = column + " double"
            else :
                col_dtype = column + " int"
            col_dtypes.append(col_dtype)

        create_sql = 'CREATE TABLE ' + table_name + ' ('
        for column in col_dtypes:
            create_sql = create_sql + column + ','
        create_sql = create_sql[:-1] + ")"

        cur = conn.cursor()
        cur.execute(create_sql)
        exists_kind = 'append'
    else :
        print(table_name + '은 이미 존재하는 테이블 입니다')
        exists_kind_int = input('데이터 INSERT 방식 (1-append, 2-replace, 3-fail): ')
        if exists_kind_int == '1':
            exists_kind = 'append'
        elif exists_kind_int == '2':
            exists_kind = 'replace'
        else :
            exists_kind = 'fail'

    # insert
    try:
        print('\nData INSERT중...\n')
        df.to_sql(table_name, engine, if_exists=exists_kind, index=False)
        print('성공')
    except:
        print('!! '+table_name + '에 데이터 INSERT 실패')

conn.commit()

cur.close()
conn.close()

print('\n프로그램 종료....')
time.sleep(3)
