import pandas as pd
  
file_pd = pd.read_csv('/home/kim/datastage/data/dsjoblog_PMOBG.dat', sep='|',names= ['Project','Job_name','Status','Started_date','End_time','Running_time'])
file_pd['End_time'] = pd.to_datetime(file_pd['End_time'])
file_pd['Running_time'] = pd.to_timedelta(file_pd['Running_time'])

file_pd = file_pd[file_pd['Status'] != 'Aborted']
#print(file_pd)

# 'End_time'과 'Running_time'을 더하여 'End_time'을 업데이트
file_pd['End_time'] += file_pd['Running_time']

#
print(file_pd.loc[0,'Started_date'].split(' ')[0].split())



"""
if file_pd['ID'].isin(['SG22']).any():
    # 'SG22' 값을 가진 행을 선택하여 인덱스를 출력
    index = file_pd[file_pd['ID'] == 'SG22'].index[0]
  
    lat = file_pd.loc[index, 'LAT']
    lon = file_pd.loc[index, 'LON']
    pw = file_pd.loc[index, 'PW']
  
    # 위에서 가져온 값을 사용하여 새로운 데이터프레임 생성
    new_df = pd.DataFrame({'LAT': [lat], 'LON': [lon], 'PW': [pw]})
  
# 결과 출력
print(new_df)

# 
"""