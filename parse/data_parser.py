import pandas as pd
import re
import os
import time

# 데이터 불러오고 컬럼이름 입력받는 코드
#---------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# 현재 디렉토리의 csv 파일 확인
while True:
    csvFilesInDir = [x for x in os.listdir() if x[-3:] == 'csv']
    print('\n현재 디렉토리의 csv파일 : ' + ",".join(i for i in csvFilesInDir)+'\n')
    if len(csvFilesInDir) == 0 :
        print('csv파일이 없습니다')
        while True:
            reCheckFile = input('명령어 입력(r-디렉토리 다시 확인, q-프로그램 종료) :')
            if reCheckFile == 'r':
                break
            elif reCheckFile =='q':
                quit()
    elif len(csvFilesInDir) == 1:
        files = list(csvFilesInDir)
        break
    else:
        filesName = input('parsing csv파일 (all 입력시 모든 파일) : ')
        if filesName == 'all':
            files = csvFilesInDir     
            break
        else :
            files = list(filesName.split(","))
            break

# 컬럼이름 맞게 입력했는지 체크하는 함수
def InputAndcheckColName(text):    
    while True :
        errorColunm=[]
        print('\n입력 가능한 컬럼 :' + ",".join(i for i in colunms))
        inputList = [i for i in input(text + ' : ').split(",") if i!=""] 
        for i in inputList:
            if i not in df.columns:
                errorColunm.append(i)
        if len(errorColunm) > 0:
            print('\n!! 잘못 입력된 컬럼 : ' + ",".join(i for i in errorColunm))
        else :
            break
    for col in inputList:
        colunms.remove(col)
    return  inputList

# 파싱 준비
for originFileName in files:
    print(originFileName + " 파싱 시작\n")
    # 파일크기가 300mb 이상일시 데이터를 5만건씩 읽어옴
    fileSize = os.path.getsize(originFileName)
    if fileSize >= 314_572_800 :# 300mb 
        df_chunk = pd.read_csv(originFileName, chunksize=50000)
        df = pd.concat(df_chunk)
    else : 
        df = pd.read_csv(originFileName)
    

    # 컬럼 이름 변경
    colNameChangeYN = input('컬럼 이름 변경(y/n) : ')
    if colNameChangeYN == 'y':
        for originColName in list(df.columns):
            chColName = input('원래 컬럼 이름 : '+ originColName +', 바꿀 컬럼 이름(공백입력시 pass) : ')
            if chColName != '':
                df.rename(columns={originColName:chColName}, inplace=True)
    colunms = list(df.columns)

    # 파서 진행할 컬럼 리스트
    # 주민등록번호 컬럼
    ResidentRegistrationNumberList = list(InputAndcheckColName('주민등록번호 컬럼'))
    # 특수문자, 공백처리가 필요한 컬럼
    specialCharacterRemoveList = list(InputAndcheckColName('특수문자 및 공백 처리할 컬럼'))
    # 특정 문자 기준으로 특정부분 까지 출력할 컬럼
    splitCharList = list(InputAndcheckColName('특정문자를 기준으로 나눈 컬럼'))
    # 날짜,시간을 원하는 형식으로 바꿀 컬럼
    dateParseList = list(InputAndcheckColName('날짜 컬럼'))
    # 이름을 성 + '씨' 로 바꿀 컬럼
    fullNameToSurnameList = list(InputAndcheckColName('이름 컬럼'))
    # 전화번호 컬럼
    phoneNumberParserList = list(InputAndcheckColName('전화번호 컬럼'))
    # 주소를 원하는 행정구역까지 바꿀 주소 컬럼
    addressParseList = list(InputAndcheckColName('주소 컬럼'))
    # 삭제할 컬럼
    dropColumnList = list(InputAndcheckColName('삭제할 컬럼'))


# 파서 코드
# 파서 추가할시 코딩 해야할것 - 컬럼이름 받은 리스트 , 파서 코드
#---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    #주민등록번호 컬럼
    if len(ResidentRegistrationNumberList) > 0 :
        print('\n주민등록번호 파싱중.....')

        def checkHipen(x):
            if '-' in x:
                result = x
            else :
                result = x[:6] + '-' + x[6:]
            return result

        def RRnumParse(colName) :
            df[colName] = df.apply(lambda x : checkHipen(str(x[colName])), axis=1)

        try:
            for colName in ResidentRegistrationNumberList:
                try:
                    RRnumParse(colName)
                except:
                    print('!! '+colName+'-컬럼 파싱 실패..')    
            print('완료')
        except:
            print('!! 주민등록번호 파싱 실패')
    
    # 특수문자, 공백이 있는 컬럼
    if len(specialCharacterRemoveList) > 0:
        parserKind = int(input('처리방법(1-특수문자 및 공백 "_"로 치환  2-특수문자 제거) : '))
        print('\n특수문자 및 공백 파싱중.....')

        specialAndEmptyPattern = re.compile(r'[^\uAC00-\uD7A30a-zA-Z0-9]')
        specialPattern = re.compile(r'[^\uAC00-\uD7A30a-zA-Z0-9\s]')

        # 특수문자 및 공백 치환
        def specialCharacterReplace(colName):
            df[colName] = df.apply(lambda x : re.sub('(([^\uAC00-\uD7A30a-zA-Z0-9])\\2{1,})', '_', specialAndEmptyPattern.sub('_', str(x[colName]))) 
                        if specialAndEmptyPattern.sub('_', str(x[colName])).strip()[-1] != '_' 
                        else  re.sub('(([^\uAC00-\uD7A30a-zA-Z0-9])\\2{1,})', '_',specialAndEmptyPattern.sub('_', str(x[colName])))[:-1]
                                        , axis=1)
        
        # 특수문자 제거
        def specialCharacterRemove(colName):
            df[colName] = df.apply(lambda x : specialPattern.sub('', str(x[colName])), axis=1)
         
        try:
            for colName in specialCharacterRemoveList:
                try:
                    if parserKind == 1:
                        specialCharacterReplace(colName)
                    elif parserKind == 2:
                        specialCharacterRemove(colName)
                except:
                    print('!! ' + colName + '-컬럼 파싱 실패..')
            print('완료')
        except:
            print('!! 특수문자 및 공백 파싱 실패')

    # 특정문자 기준으로 나눔
    if len(splitCharList) > 0:
        splitStandard = input('해당기준을 정할 특수문자 또는 공백(space bar)을 입력 : ')
        splitCount = int(input('나눈 문자열의 N번째 까지 사용(min=1), N 입력 :'))
        print('\n특정기준으로 나누기 파싱중.....')

        def checkChar(x):
            try:
                while True:
                    if splitStandard != "":
                        break         
                return x.split(splitStandard)[:splitCount]
            except:
                return 'NULL'

        def splitCharcter(colName) :
                df[colName] = df.apply(lambda x : checkChar(str(x[colName])), axis=1)

        try:
            for colName in splitCharList:
                try:
                    splitCharcter(colName)
                except:
                    print('!! ' + colName + '-컬럼 파싱 실패..')
            print("완료")
        except :
            print('!! 문자열 나누기 실패')

    # 이름 컬럼
    if len(fullNameToSurnameList) > 0:
        print('\n이름 컬럼 파싱중.....')
        # 이름 파싱 함수
        def check_name(fullName) :
            try:
                name = fullName.split(' ')[0]
                if nameParserType == 1 and " " in fullName:
                    name = fullName.split(' ')[1]

                # 스플릿한게 두글자인데
                if len(name) == 2 :
                    # 두글자 성씨인 경우
                    if name == '남궁' or name == '황보' or name == '제갈' or name == '선우' or name == '사공' or name == '서문' or name == '독고' :
                        return name
                    # 외자 이름인데 붙어있는 이름
                    elif len(fullName.split(" ")) == 1:
                        return name[nameParserType]          
                    # 이름위치 특징의 반대로 갔을경우(띄어쓰기가 된경우만)
                    else :
                        return fullName.split(" ")[int(not(nameParserType))]
                # 이름이 두번 쓰인 경우 or 붙어있는 이름 ex)홍길동 홍길동 or 홍길동
                elif len(name) == 3 :
                    indexNum = 0
                    if nameParserType == 1:
                        indexNum += 2
                    return name[indexNum]
                #  두글자 성 인 사람의 이름이 띄어쓰기가 없는경우
                elif len(name) >= 4 :
                    if nameParserType == 0:
                        return name[:2]
                    elif nameParserType == 1:
                        return name[2:]
                # 다 해당되지 않음
                else :
                    return name
            except :
                return 'NULL'

        def fullNameToSurname(colName) :
            df[colName] = df.apply(lambda x : check_name(str(x[colName])) + '씨', axis=1)

        try:
            for colName in fullNameToSurnameList:
                try:
                    nameParserType = int(input(colName + ' 컬럼의 특징(0-성+이름  1-이름+성) : '))
                    fullNameToSurname(colName)
                except:
                    print('!! ' + colName + '-컬럼 파싱 실패..')
            print("완료")
        except :
            print('!! 이름 파싱 실패')

    # 전화번호 컬럼
    if len(phoneNumberParserList) > 0:
        print('\n전화번호 컬럼 파싱중.....')
        def check_num(x):
            if '-' in x :
                return ChangeNumHyphen(x)
            else :
                return ChangeNum(x)
            
        def ChangeNumHyphen(x) :
            try:
                if x[:2] == '82':
                    return x
                else :
                    return '0' + x[2:]
            except :
                return 'NULL'
            
        def ChangeNum(x):
            try:
                if x[:2] == '82':
                    x = '0' + x[2:]
                elif x[:2] == '10' :
                    x = '0' + x
                return x[:3] + '-' + x[3:7] + '-' + x[7:11]
            except:
                return 'NULL'

        def phoneNumberParser(colName):
            df[colName] = df.apply(lambda x : check_num(str(x[colName])), axis=1)

        try:
            for colName in phoneNumberParserList:
                try:
                    phoneNumberParser(colName)
                except :
                    print('!! ' + colName + '-컬럼 파싱 실패..')
            print('완료')
        except :
            print('!! 전화번호 파싱 실패')

    # 주소 컬럼
    if len(addressParseList) > 0:
        addressParsePart = int(input('\n주소 컬럼에서 표시할 주소 입력(1-도,특별시,광역시  2-시,군,구  3-시_구  4-로,길: '))  
        print('주소 컬럼 파싱중.....')
        def changeAddress(x):
            try: 
                addressList = x.split(" ")
                if len(addressList[0]) == 4 and (addressList[0][2] == '남' or addressList[0][2] == '북') :
                    addressList[0] = addressList[0][0] + addressList[0][2]
                else :
                    addressList[0] = addressList[0][:2]
                result = '_'.join(i for i in addressList[:addressParsePart])
                if addressParsePart == 3 and (result[-1] != '구'):
                    result = '_'.join(i for i in addressList[:addressParsePart-1])
                elif addressParsePart == 4:
                    for part in addressList:
                        if part[-1] == '로' or part[-1] =='길':
                            findIndex = addressList.index(part)
                            break
                    result = '_'.join(i for i in addressList[:findIndex+1])
                return result
            except :
                return 'NULL'

        def addressParse(colName):
            df[colName] = df.apply(lambda x : changeAddress(x[colName]), axis=1)

        try:
            for colName in addressParseList:
                try:
                    addressParse(colName)
                except:
                    print('!!' + colName + '-컬럼 파싱 실패..')
            print('완료')
        except:
            print('!! 주소 파싱 실패')

    #날짜 컬럼
    if len(dateParseList) > 0:
        dateParsePart = input('\n날짜,시간 컬럼에서 표시할 날 또는 시간 : ')
        print('날짜 컬럼 파싱중.....')
        dateDic = {'년':0, '월':1, '일':2, '시':3, '분':4}
        def dateChange(x):
            try:
                if len(x.split(" ")) == 1:
                    dateList = (x.split(" ")[0].split("-")) 
                else :
                    dateList = (x.split(" ")[0].split("-")) + (x.split(" ")[1].split(":"))
                if len(dateParsePart) == 1:
                    return dateList[dateDic[dateParsePart]]
                return '_'.join(i for i in dateList[dateDic[dateParsePart[0]]:dateDic[dateParsePart[-1]]+1])
            except:
                return 'NULL'

        def dateParse(colName):
            df[colName] = df.apply(lambda x : dateChange(str(x[colName])), axis=1)

        try:
            for colName in dateParseList:
                try:
                    dateParse(colName)
                except:
                    print('!! ' + colName + '-컬럼 파싱 실패..')
            print('완료')
        except:
            print('!! 날짜 컬럼 파싱 실패')

        # 삭제할 컬럼 
        if len(dropColumnList) > 0:
            print('\n필요없는 컬럼 삭제중.....')
            def dropColumn(colName):
                df.drop([colName],axis=1, inplace=True)

            try:
                for colName in dropColumnList:
                    try:
                        dropColumn(colName)
                    except:
                        print('!! ' + colName + '-컬럼 삭제 실패..')
                print('완료')
            except:
                print('!! 컬럼 삭제 실패')
        

    # parser 완료후 다시 csv로 저장
    resultFileName = originFileName[:-4] + '_parse.csv'  
    try:
        print('\n'+originFileName + ' 파일 파싱 완료, 파일 저장중...')
        df.to_csv(resultFileName, index = False, encoding='utf-8-sig')
        print("파싱한 파일 저장 완료")
    except:
        print('!! 파일 저장 실패')

print('\n프로그램 종료.....')
time.sleep(3)
