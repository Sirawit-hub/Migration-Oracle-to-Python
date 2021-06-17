import cx_Oracle
import pandas as pd
import CON4_D_EMPLOYEE_ORA06_V


dsn_tns = cx_Oracle.makedsn("147.50.43.90", 1531, service_name="VIS") # if needed, place an 'r' before any parameter in order to address special characters such as ''.
con = cx_Oracle.connect(user='apps', password='apps', dsn=dsn_tns,encoding="UTF-8") # if needed, place an 'r' before any parameter in order to address special characters such as ''. For example, if your user name contains '', you'll need to place 'r' before the user name: user=r'User Name'
cursor = con.cursor()

df_emp = CON4_D_EMPLOYEE_ORA06_V.df_total

sql='insert into D_EMPLOYEE_ORA06(EMPLOYEE_NAME,EMPLOYEE_EMAIL,EMPLOYEE_PHONE_NUMBER,LAST_DATE,JOB_TITLE,EMPLOYEE_COMMISSION,MANAGER_NAME,ETL_DATE,ETL_LAST_UPDATE) values(:1,:2,:3,:4,:5,:6,:7,SYSDATE,SYSDATE)'

df_list = df_emp.values.tolist()
n = 0
for i in df_emp.iterrows():
    cursor.execute(sql,df_list[n])
    n += 1

con.commit()
print("insert success")




