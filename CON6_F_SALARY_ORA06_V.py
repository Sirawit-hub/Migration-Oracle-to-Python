import cx_Oracle
import pandas as pd
from pandas.core.frame import DataFrame
import numpy as np
from datetime import datetime

#set format dataframe / เรียงตารางให้ตรง ตาม row และ column
#pd.set_option('display.max_columns', None) #โชว์ columns ทั้งหมด
pd.set_option('display.max_rows', None) #โชว์ rows ทั้งหมด

dsn_tns = cx_Oracle.makedsn("147.50.43.90", 1531, service_name="VIS") # if needed, place an 'r' before any parameter in order to address special characters such as ''.
con = cx_Oracle.connect(user='apps', password='apps', dsn=dsn_tns,encoding="UTF-8") # if needed, place an 'r' before any parameter in order to address special characters such as ''. For example, if your user name contains '', you'll need to place 'r' before the user name: user=r'User Name'
cursor = con.cursor()

#insert data to dataframe
query_t6_1 = "select * from employees " 
df_employees_06 = pd.read_sql(query_t6_1,con)

query_t6_2 = "select * from jobs"
df_jobs_06 = pd.read_sql(query_t6_2,con)

query_t6_3 = "select DEPARTMENT_ID from departments"
df_departments_06 = pd.read_sql(query_t6_3,con)

query_t6_4 = """select e.EMPLOYEE_ID,m.EMPLOYEE_ID memployee_id,m.salary msalary
from employees e join employees m on e.manager_id = m.employee_id"""
df_manager_06 = pd.read_sql(query_t6_4,con)



#-----------------------------------------------------------------------------------------------------------
#join ตาราง employees กับ jobs  116 row
df_emp_jobs_6 = pd.merge(df_employees_06, df_jobs_06, on='JOB_ID')
#print(df_emp_jobs_6)

#join ต่อกับตาราง กับ jobhistory  110 row
df_emp_jobs_jh_6 = pd.merge(df_emp_jobs_6, df_departments_06, on='DEPARTMENT_ID')
#df_emp_jobs_jh_6 = df_emp_jobs_jh_6.sort_values(["EMPLOYEE_ID"])
#print(df_emp_jobs_jh_6)

#self join ได้ 108 row
df_emp_jobs_jh_6 = pd.merge(df_emp_jobs_jh_6,df_manager_06 , on='EMPLOYEE_ID',how='inner')
#df_emp_jobs_jh_6 = df_emp_jobs_jh_6.sort_values(["EMPLOYEE_ID"])
#print(df_emp_jobs_jh_6)

#----------------------------------------------------------------------------------------------
#ตั้งชื่อคอลัมน์ใหม่ แล้วแทนค่า NaN ด้วย 0
df_emp_jobs_jh_6["EMPLOYEE_SALARY"] = df_emp_jobs_jh_6["SALARY"].replace(np.nan,0)
df_emp_jobs_jh_6["MIN_JOB_SALARY"] = df_emp_jobs_jh_6["MIN_SALARY"].replace(np.nan,0)
df_emp_jobs_jh_6["MAX_JOB_SALARY"] = df_emp_jobs_jh_6["MAX_SALARY"].replace(np.nan,0)
df_emp_jobs_jh_6["MANAGER_SALARY"] = df_emp_jobs_jh_6["MSALARY"].replace(np.nan,0)
df_emp_jobs_jh_6["JOB_SALARY_RANKING"] = df_emp_jobs_jh_6.groupby("JOB_ID")["EMPLOYEE_SALARY"].rank("dense",ascending=False)

#แปลง float64 เป็น int64 เพื่อตัดทศนิยมออก
df_emp_jobs_jh_6["EMPLOYEE_SALARY"] = df_emp_jobs_jh_6["EMPLOYEE_SALARY"].apply(np.int64)
df_emp_jobs_jh_6["MANAGER_SALARY"] = df_emp_jobs_jh_6["MANAGER_SALARY"].apply(np.int64)
df_emp_jobs_jh_6["JOB_SALARY_RANKING"] = df_emp_jobs_jh_6["JOB_SALARY_RANKING"].apply(np.int64)


#--------------------------------------------------------------------------
#sysdate / วันปัจจุบัน   จัด format ให้โชว์แค่ปี 
datenow_year = datetime.today().strftime("%Y")
datenow_month = datetime.today().strftime("%Y%m")

#เพิ่มคอลัมน์ YEAR_CD และ MONTH_CD
df_emp_jobs_jh_6['YEAR_CD'] = datenow_year
df_emp_jobs_jh_6['MONTH_CD'] = datenow_month



#---------------------------------------เก็บค่าที่ต้องการใน df_total_6---------------------------------------------------
df_total_6=df_emp_jobs_jh_6[["YEAR_CD","MONTH_CD","EMPLOYEE_SALARY","MAX_JOB_SALARY","MIN_JOB_SALARY","MANAGER_SALARY","JOB_SALARY_RANKING"]]
#df_total_6 = df_total_6.sort_values(["EMPLOYEE_SALARY"]) #เรียงคอลัมน์ตาม EMPLOYEE_SALARY
print(df_total_6)





"""
#-------------------------------self join สำรอง ยังติดปัญหาอยู่------------------------------------------------------------------
#แปลงเป็น list
l_emp_jobs_jh_6 = df_emp_jobs_jh_6.values.tolist()
#print(df_emp_jobs_jh_6)


emp=0
man=0
l_manager_id = []
l_max_salary = []



for emp in range(0,len(l_emp_jobs_jh_6)):
    for man in range(0,len(l_emp_jobs_jh_6)):
        if l_emp_jobs_jh_6[emp][0] == l_emp_jobs_jh_6[man][9]:
            l_manager_id.append(l_emp_jobs_jh_6[emp][0])
            l_max_salary.append(l_emp_jobs_jh_6[emp][7])
            man=0     



#--------------------------------------------------------------------------------------------------
#เปลี่ยน list เป็น dataframe
df_manager_id = DataFrame(l_manager_id,columns=['MANAGER_ID'])
#print(df_manager_id)


df_max_salary = DataFrame(l_max_salary,columns=['MANAGER_SALARY'])
#print(df_max_salary)


#รวม dataframe df_manager_id  กับ df_max_salary  *** axis 1 คือ เชื่อมด้วยคอลัมน์  / axis 0 คือ เชื่อมด้วยแุถว
horizontal_stack = pd.concat([df_manager_id, df_max_salary], axis=1)
#horizontal_stack = horizontal_stack.sort_values(["MANAGER_ID"])
print(horizontal_stack)


#-------------------------------------join แล้ว ได้ 872 rows -------------------------------
df_emp_jobs_jh_6 = pd.merge(df_emp_jobs_jh_6, horizontal_stack, on='MANAGER_ID')
#print(df_emp_jobs_jh_6)
"""


