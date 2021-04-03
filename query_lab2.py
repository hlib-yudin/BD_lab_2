"""Юдін Гліб, КМ-82, лабораторна робота №2
Варіант 10
Порівняти найкращий бал з фізики у 2020 та 2019 роках (у кожному регіоні) серед тих,
кому було зараховано тест.
"""

import psycopg2
import csv

dbname = input('Введіть назву БД: ')
user = input("Введіть ім'я користувача: ")
password = input("Введіть пароль: ")
host = input("Введіть хост: ")
port = input("Введіть порт: ")
# підключення до бази даних
conn = psycopg2.connect(dbname=dbname, user=user, 
                        password=password, host=host, port=port)
cursor = conn.cursor()


def statistical_query():
    # Виконує статистичний запит до таблиці та записує результат у новий csv-файл.
    select_query = '''
    SELECT Location.RegName, TestResult.year, max(TestResult.Ball100)
    FROM TestResult JOIN Participant ON
        TestResult.OutID = Participant.OutID
    JOIN Location ON
        Participant.loc_id = Location.loc_id
    WHERE TestResult.TestName = 'Фізика' AND
        TestResult.TestStatus = 'Зараховано'
    GROUP BY Location.RegName, TestResult.year
    '''
    cursor.execute(select_query)

    with open('result_lab2.csv', 'w', encoding="utf-8") as new_csv_file:
        csv_writer = csv.writer(new_csv_file)
        csv_writer.writerow(['Область', 'Рік', 'Макс. бал з фізики'])
        for row in cursor:
            csv_writer.writerow(row)


statistical_query()



#conn.commit()
cursor.close()
conn.close()