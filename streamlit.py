import pandas as pd
import re
from snowflake.snowpark import Session
import streamlit as st
import matplotlib.pyplot as plt
import plotly.express as px

def get_snowpark_session() -> Session:
    connection_parameters = {
       "ACCOUNT":"dl52763.ap-northeast-2.aws",
        "USER":"penta",
        "PASSWORD":"PEnta2848!",
        "ROLE":"ACCOUNTADMIN",
        "DATABASE":"SNOWFLAKE",
        "SCHEMA":"ML",
        "WAREHOUSE":"COMPUTE_WH"
    }
    # creating snowflake session object
    return Session.builder.configs(connection_parameters).create()

if __name__ == '__main__':
    session = get_snowpark_session()
    st.sidebar.markdown("# Main page 🎈")  
    col1 , col2 = st.sidebar.columns(2)
    
    select_month = col1.selectbox('월',[str(i)+"월" for i in range(1, 13)])
    select_weekday = col2.selectbox('요일', ["일요일", "월요일", "화요일", "수요일", "목요일", "금요일", "토요일"])
    if select_weekday == "일요일":
        select = 0
    elif select_weekday == "월요일":
        select = 1
    elif select_weekday == "화요일":
        select = 2
    elif select_weekday == "수요일":
        select = 3
    elif select_weekday == "목요일":
        select = 4
    elif select_weekday == "금요일":
        select = 5
    else:
        select = 6

    #select_weekday = col2.selectbox('요일', [str(i)+"요일" for i in range(0, 7)])

    text_test = pd.read_csv('/home/kim/datastage/data/datastage_scheduler.txt',sep = " ",engine='python'
                            ,names= ['Minutes', 'hours', 'Day', 'Month', 'Week', 'file_path', 'Project',
                                     'Engine_name','Job_name','n1','n2','n3','n4','n5','n6'])

    list_month=[]
    list_week = []
    job_list =[]
    
    
    def month(parama1,parma2):
        parama1 = str(parama1)
        parma2 = str(parma2)
        for i in range(len(text_test)):
            week = re.findall(r'[' + parama1 + r'*]', text_test.loc[i, 'Month'])
            if len(week) >0:
                
                list_month.append(i)
        
        for j in range(len(list_month)):
            week = re.findall(r'[' + parma2 + r'*]', text_test.loc[j, 'Week'])
            
            if len(week) >0:
                
                list_week.append(j)
                
        #print(list_week)

   # 1월 수요일마다 돌아가는 job (0~6 => 일~토)     
    month(select_month,select)

    time_dict = {str(i): 0 for i in range(24)}
    time_table = {}
    total_hour = []
    total = ''
    for i in list_week:
        hour_match = re.findall(r'[0-9]*', str(text_test.loc[i,'hours']))  # 정규식을 사용하여 시간 추출

        if hour_match:
            job_list.append(text_test.loc[i,'Job_name'])
            hour = hour_match[0]
            time_dict[hour] += 1
             
            
            total = hour + ":" +  str(text_test.loc[i,'Minutes'])
            total_hour.append(total)
            time_table[total] = text_test.loc[i,'Job_name']
    

    # 결과 출력 => 테이블로 보여준다


    df = pd.DataFrame(sorted(time_table.items(), key=lambda item: tuple(map(int, item[0].split(':')))), columns=['Time', 'Job Name'])
    
    
    labels = list(time_dict.keys())
    labels = [label + "시" for label in labels]
    values = list(time_dict.values())

    st.title(f"{select_month} PMOBG Schedule Show")
    col1,empty1, col2,  =st.columns([1.2,0.3,1.2])
    with col1:
        st.subheader(f"매주 {select_weekday}, 시간대 별 JOB 개수")
        fig = px.bar(x=labels, y=values)
        st.plotly_chart(fig)
    with empty1:
        st.empty()
    with col2:
        st.subheader("시간대 별 JOB_NAME")
        fig = st.dataframe(df,width =1500, height = 500)
        