import cx_Oracle
import pandas as pd
import mysql.connector
 

dsn_tns = cx_Oracle.makedsn("147.50.43.90", 1531, service_name="VIS") # if needed, place an 'r' before any parameter in order to address special characters such as ''.
con = cx_Oracle.connect(user='apps', password='apps', dsn=dsn_tns,encoding="UTF-8") # if needed, place an 'r' before any parameter in order to address special characters such as ''. For example, if your user name contains '', you'll need to place 'r' before the user name: user=r'User Name'

cursor = con.cursor()

cursor.execute("CREATE TABLE   F_SALARY_ORA06 (SEQ       NUMBER(10),\
    YEAR_CD			        NUMBER(4),\
    MONTH_CD				NUMBER(6),\
  	EMPLOYEE_CD             NUMBER(6),\
  	JOB_CD           		NVARCHAR2(20),\
  	DEPARTMENT_CD        	NUMBER(4),\
  	EMPLOYEE_SALARY        	NUMBER(8,2),\
  	MAX_JOB_SALARY         	NUMBER(6,0),\
  	MIN_JOB_SALARY     		NUMBER(6,0),\
    MANAGER_SALARY			NUMBER(8,2),\
  	JOB_SALARY_RANKING 		NUMBER(2),\
  	ETL_DATE           		TIMESTAMP(6),\
  	ETL_LAST_UPDATE    		TIMESTAMP(6))")

print("create table F_SALARY_ORA06 success")
