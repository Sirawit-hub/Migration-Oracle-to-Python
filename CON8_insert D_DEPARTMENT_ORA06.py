import cx_Oracle
import pandas as pd
import CON5_D_DEPARTMENT_ORA06_V


dsn_tns = cx_Oracle.makedsn("147.50.43.90", 1531, service_name="VIS") # if needed, place an 'r' before any parameter in order to address special characters such as ''.
con = cx_Oracle.connect(user='apps', password='apps', dsn=dsn_tns,encoding="UTF-8") # if needed, place an 'r' before any parameter in order to address special characters such as ''. For example, if your user name contains '', you'll need to place 'r' before the user name: user=r'User Name'
cursor = con.cursor()


df_dep = CON5_D_DEPARTMENT_ORA06_V.df_total_05

sql='insert into D_DEPARTMENT_ORA06(STREET_ADDRESS,REGION_NAME_EN,REGION_NAME_TH,ETL_DATE,ETL_LAST_UPDATE) values(:1,:2,:3,SYSDATE,SYSDATE)'

df_list = df_dep.values.tolist()
n = 0
for i in df_dep.iterrows():
    cursor.execute(sql,df_list[n])
    n += 1

con.commit()
print("insert success")



