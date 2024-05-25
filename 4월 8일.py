import pandas as pd
import re

file_pd = pd.read_csv('/home/kim/datastage/data/dsjoblog_PMOBG.dat', sep='|',names= ['Project','Job_name','Status','Started_date','End_time','Running_time'])
file_pd['End_time'] = pd.to_datetime(file_pd['End_time'])
file_pd['Running_time'] = pd.to_timedelta(file_pd['Running_time'])

# 'End_time'과 'Running_time'을 더하여 'End_time'을 업데이트
file_pd['End_time'] += file_pd['Running_time']


# 'ID' 열의 첫 번째 요소가 'BLAC'인지 확인하고 'BREC' 출력

text_test = pd.read_csv('/home/kim/datastage/data/datastage_scheduler.txt',sep = " ",engine='python'
                        ,names= ['Minutes', 'hours', 'Day', 'Month', 'Week', 'file_path', 'Project',
                                    'Engine_name','Job_name','n1','n2','n3','n4','n5','n6'])

list_month=[]
list_day = []
job_list =[]


def month(parama1,parma2):
    parama1 = str(parama1)
    parma2 = str(parma2)
    for i in range(len(text_test)):
        week = re.findall(r'[' + parama1 + r'*]', text_test.loc[i, 'Month'])
        if len(week) >0:
            
            list_month.append(i)

    for j in range(len(list_month)):
        if text_test.loc[j,'Day'] == '*':
            
            list_day.append(j)
        elif len(re.findall(r'\d+', text_test.loc[j,'Day'])) > 0:
            numbers = re.findall(r'\d+', text_test.loc[j,'Day'])

            for i in numbers:
                
                if i == str(parma2):
                    list_day.append(j)
                    #print(list_test)

    


# 1월 수요일마다 돌아가는 job (0~6 => 일~토)     
month(1 ,8)

time_dict = {str(i): 0 for i in range(24)}
time_table = {}
total_hour = []
total = ''

for i in list_day:
    
    job_list.append(text_test.loc[i,'Job_name'])
    hour = str(text_test.loc[i,'hours'])
    time_dict[hour] += 1
        
    
    total = hour + ":" +  str(text_test.loc[i,'Minutes'])
    
    total_hour.append(total)
    time_table[text_test.loc[i,'Job_name']] = total


df = pd.DataFrame(sorted(time_table.items(), key=lambda item: tuple(map(int, item[1].split(':')))), columns=['Job_name', 'Time'])

Started_date_list = []
End_time_list = []
Running_time_list = []

# 데이터를 필터링하고 리스트에 추가합니다.
for job_name in df['Job_name']:
    if job_name in file_pd['Job_name'].values:
        index = file_pd[file_pd['Job_name'] == job_name].index[0]  # 'file_pd'에서 'Job_name'이 일치하는 첫 번째 행의 인덱스를 가져옵니다.
        Started_date_list.append(file_pd.loc[index, 'Started_date'])
        End_time_list.append(file_pd.loc[index, 'End_time'])
        Running_time_list.append(file_pd.loc[index, 'Running_time'])
    else:
        Started_date_list.append(None)  # 'file_pd'에서 해당하는 값이 없을 때는 NaN을 추가합니다.
        End_time_list.append(None)  # 'file_pd'에서 해당하는 값이 없을 때는 NaN을 추가합니다.
        Running_time_list.append(None)  # 'file_pd'에서 해당하는 값이 없을 때는 NaN을 추가합니다.

# 새로운 데이터프레임을 생성합니다.
result_df = pd.DataFrame({
    'Job_name': df['Job_name'],
    'Started_date': Started_date_list,
    'End_time': End_time_list,
    'Running_time' : Running_time_list
})

# 결과를 출력합니다.
print(result_df['Started_date'])

if result_df['Started_date'].split('-')[0] == '2024' and result_df['Started_date'].split('-')[1] == '04':



# 결과를 출력합니다.