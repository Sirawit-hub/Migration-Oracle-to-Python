import cx_Oracle
import pandas as pd


dsn_tns = cx_Oracle.makedsn("147.50.43.90", 1531, service_name="VIS") # if needed, place an 'r' before any parameter in order to address special characters such as ''.
con = cx_Oracle.connect(user='apps', password='apps', dsn=dsn_tns,encoding="UTF-8") # if needed, place an 'r' before any parameter in order to address special characters such as ''. For example, if your user name contains '', you'll need to place 'r' before the user name: user=r'User Name'

cursor = con.cursor()


cursor.execute("DELETE FROM F_SALARY_ORA06 where (YEAR_CD,MONTH_CD)in (select YEAR_CD,MONTH_CD from F_SALARY_ORA06_V)")

con.commit()

print(cursor.rowcount, "record(s) deleted")

