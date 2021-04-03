"""Юдін Гліб, КМ-82, лабораторна робота №1
Варіант 10
Порівняти найкращий бал з фізики у 2020 та 2019 роках (у кожному регіоні) серед тих,
кому було зараховано тест.
"""



"""
CREATE TABLE IF NOT EXISTS tbl_zno_data (
	Year INT,
	OUTID VARCHAR(40) PRIMARY KEY,
	Birth INT,
	SEXTYPENAME VARCHAR(255),
	REGNAME VARCHAR(255),
	AREANAME VARCHAR(255),
	TERNAME VARCHAR(255),
	REGTYPENAME VARCHAR(255),
	TerTypeName VARCHAR(255),
	ClassProfileNAME VARCHAR(255),
	ClassLangName VARCHAR(255),
	EONAME VARCHAR(255),
	EOTYPENAME VARCHAR(255),
	EORegName VARCHAR(255),
	EOAreaName VARCHAR(255),
	EOTerName VARCHAR(255),
	EOParent VARCHAR(255),
	UkrTest VARCHAR(255),
	UkrTestStatus VARCHAR(255),
	UkrBall100 REAL,
	UkrBall12 REAL,
	UkrBall REAL,
	UkrAdaptScale VARCHAR(255),
	UkrPTName VARCHAR(255),
	UkrPTRegName VARCHAR(255),
	UkrPTAreaName VARCHAR(255),
	UkrPTTerName VARCHAR(255),
	histTest VARCHAR(255),
	HistLang VARCHAR(255),
	histTestStatus VARCHAR(255),
	histBall100 REAL,
	histBall12 REAL,
	histBall REAL,
	histPTName VARCHAR(255),
	histPTRegName VARCHAR(255),
	histPTAreaName VARCHAR(255),
	histPTTerName VARCHAR(255),
	mathTest VARCHAR(255),
	mathLang VARCHAR(255),
	mathTestStatus VARCHAR(255),
	mathBall100 REAL,
	mathBall12 REAL,
	mathBall REAL,
	mathPTName VARCHAR(255),
	mathPTRegName VARCHAR(255),
	mathPTAreaName VARCHAR(255),
	mathPTTerName VARCHAR(255),
	physTest VARCHAR(255),
	physLang VARCHAR(255),
	physTestStatus VARCHAR(255),
	physBall100 REAL,
	physBall12 REAL,
	physBall REAL,
	physPTName VARCHAR(255),
	physPTRegName VARCHAR(255),
	physPTAreaName VARCHAR(255),
	physPTTerName VARCHAR(255),
	chemTest VARCHAR(255),
	chemLang VARCHAR(255),
	chemTestStatus VARCHAR(255),
	chemBall100 REAL,
	chemBall12 REAL,
	chemBall REAL,
	chemPTName VARCHAR(255),
	chemPTRegName VARCHAR(255),
	chemPTAreaName VARCHAR(255),
	chemPTTerName VARCHAR(255),
	bioTest VARCHAR(255),
	bioLang VARCHAR(255),
	bioTestStatus VARCHAR(255),
	bioBall100 REAL,
	bioBall12 REAL,
	bioBall REAL,
	bioPTName VARCHAR(255),
	bioPTRegName VARCHAR(255),
	bioPTAreaName VARCHAR(255),
	bioPTTerName VARCHAR(255),
	geoTest VARCHAR(255),
	geoLang VARCHAR(255),
	geoTestStatus VARCHAR(255),
	geoBall100 REAL,
	geoBall12 REAL,
	geoBall REAL,
	geoPTName VARCHAR(255),
	geoPTRegName VARCHAR(255),
	geoPTAreaName VARCHAR(255),
	geoPTTerName VARCHAR(255),
	engTest VARCHAR(255),
	engTestStatus VARCHAR(255),
	engBall100 REAL,
	engBall12 REAL,
	engDPALevel VARCHAR(255),
	engBall REAL,
	engPTName VARCHAR(255),
	engPTRegName VARCHAR(255),
	engPTAreaName VARCHAR(255),
	engPTTerName VARCHAR(255),
	fraTest VARCHAR(255),
	fraTestStatus VARCHAR(255),
	fraBall100 REAL,
	fraBall12 REAL,
	fraDPALevel VARCHAR(255),
	fraBall REAL,
	fraPTName VARCHAR(255),
	fraPTRegName VARCHAR(255),
	fraPTAreaName VARCHAR(255),
	fraPTTerName VARCHAR(255),
	deuTest VARCHAR(255),
	deuTestStatus VARCHAR(255),
	deuBall100 REAL,
	deuBall12 REAL,
	deuDPALevel VARCHAR(255),
	deuBall REAL,
	deuPTName VARCHAR(255),
	deuPTRegName VARCHAR(255),
	deuPTAreaName VARCHAR(255),
	deuPTTerName VARCHAR(255),
	spaTest VARCHAR(255),
	spaTestStatus VARCHAR(255),
	spaBall100 REAL,
	spaBall12 REAL,
	spaDPALevel VARCHAR(255),
	spaBall REAL,
	spaPTName VARCHAR(255),
	spaPTRegName VARCHAR(255),
	spaPTAreaName VARCHAR(255),
	spaPTTerName VARCHAR(255)
);
"""

import psycopg2
import psycopg2.errorcodes
import csv
import itertools
import time
import datetime


dbname = input('Введіть назву БД: ')
user = input("Введіть ім'я користувача: ")
password = input("Введіть пароль: ")
host = input("Введіть хост: ")
port = input("Введіть порт: ")
# підключення до бази даних
conn = psycopg2.connect(dbname=dbname, user=user, 
                        password=password, host=host, port=port)
cursor = conn.cursor()
# якщо таблиця вже існує -- видаляємо її
cursor.execute('DROP TABLE IF EXISTS tbl_zno_data;')
conn.commit()


def create_table():
    """Створює таблицю для даної лабораторної роботи. Повертає заголовок csv-файлу. 
    f -- назва csv-файлу з даними."""
    with open("Odata2019File.csv", "r", encoding="cp1251") as csv_file:
        # список назв усіх колонок
        header = csv_file.readline().split(';')
        header = [word.strip('"') for word in header]
        header[-1] = header[-1].rstrip('"\n')
        # формуємо запит на створення таблиці
        columns_info = "\n\tYear INT,"
        for word in header:
            if word == 'Birth':
                columns_info += '\n\t' + word + ' INT,'
            elif 'Ball' in word:
                columns_info += '\n\t' + word + ' REAL,'
            elif word == "OUTID":
                columns_info += '\n\t' + word + ' VARCHAR(40) PRIMARY KEY,'
            else:
                columns_info += '\n\t' + word + ' VARCHAR(255),'

        # сам запит на створення таблиці
        create_table_query = '''CREATE TABLE IF NOT EXISTS tbl_zno_data (''' + columns_info.rstrip(',') + '\n);'
        cursor.execute(create_table_query)
        conn.commit()
        #print(create_table_query)
        return header

header = create_table()
#exit()


def insert_from_csv(f, year, conn, cursor, logs_f):
    """Заповнює таблицю даними з заданого csv-файлу. Оброблює ситуації, пов'язані з 
    втратою з'єднання з базою даних. Створює файл, в який записує, скільки минуло часу на
    виконання запиту.
    f -- назва csv-файлу з даними.
    year -- рік, якому відповідає даний csv-файл.
    conn -- об'єкт з'єднання з БД.
    cursor -- курсор для даної БД.
    logs_f -- файл для запису логів.
    Повертає conn i cursor (оскільки ці об'єкти можуть бути оновлені). """

    start_time = datetime.datetime.now() 
    logs_f.write(str(start_time) + " -- відкриття файлу " + f + '\n')
    
    with open(f, "r", encoding="cp1251") as csv_file:
        # починаємо читати дані з csv-файлу та формувати insert-запит
        # дані зчитуються партіями
        print("Читаємо файл " + f + ' ...' )
        csv_reader = csv.DictReader(csv_file, delimiter=';')
        batches_inserted = 0
        batch_size = 100
        inserted_all = False

        # поки не вставили всі рядки
        while not inserted_all:
            try:
                insert_query = '''INSERT INTO tbl_zno_data (year, ''' + ', '.join(header) + ') VALUES '
                count = 0
                for row in csv_reader:
                    count += 1
                    # обробляємо запис: оточуємо всі текстові рядки одинарними лапками, замінюємо в числах кому на крапку
                    for key in row:
                        if row[key] == 'null':
                            pass
                        elif key.lower() != 'birth' and 'ball' not in key.lower():
                            row[key] = "'" + row[key].replace("'", "''") + "'"
                        elif 'ball100' in key.lower():
                            row[key] = row[key].replace(',', '.')
                    insert_query += '\n\t(' + str(year) + ', ' + ','.join(row.values()) + '),'

                    # якщо набралося 100 рядків -- коммітимо транзакцію
                    if count == batch_size:
                        count = 0
                        insert_query = insert_query.rstrip(',') + ';'
                        cursor.execute(insert_query)
                        conn.commit()
                        batches_inserted += 1
                        insert_query = '''INSERT INTO tbl_zno_data (year, ''' + ', '.join(header) + ') VALUES '
                    
                # якщо досягли кінця файлу -- коммітимо транзакцію
                if count != 0:
                    insert_query = insert_query.rstrip(',') + ';'
                    cursor.execute(insert_query)
                    conn.commit()
                inserted_all = True

            except psycopg2.OperationalError as e:
                # якщо з'єднання з базою даних втрачено
                if e.pgcode == psycopg2.errorcodes.ADMIN_SHUTDOWN:
                    print("База даних впала -- чекаємо на відновлення з'єднання...")
                    logs_f.write(str(datetime.datetime.now()) + " -- втрата з'єднання\n")
                    connection_restored = False
                    while not connection_restored:
                        try:
                            # намагаємось підключитись до бази даних
                            conn = psycopg2.connect(dbname='postgres', user='postgres', 
                                password='postgres', host='localhost', port="5432")
                            cursor = conn.cursor()
                            logs_f.write(str(datetime.datetime.now()) + " -- відновлення з'єднання\n")
                            connection_restored = True
                        except psycopg2.OperationalError as e:
                            pass

                    print("З'єднання відновлено! Продовжуємо роботу...")
                    csv_file.seek(0,0)
                    csv_reader = itertools.islice(csv.DictReader(csv_file, delimiter=';'), 
                        batches_inserted * batch_size, None)

    end_time = datetime.datetime.now()
    logs_f.write(str(end_time) + " -- файл повністю оброблено\n")
    logs_f.write('Витрачено часу на даний файл -- ' + str(end_time - start_time) + '\n\n')

    return conn, cursor


logs_file = open('logs.txt', 'w')       
conn, cursor = insert_from_csv("Odata2019File.csv", 2019, conn, cursor, logs_file)
conn, cursor = insert_from_csv("Odata2020File.csv", 2020, conn, cursor, logs_file)
# всього -- 353813 + 379299 = 733112 записів     
logs_file.close()


def statistical_query():
    """Виконує статистичний запит до таблиці та записує результат у новий csv-файл. """
    select_query = '''
    SELECT regname, year, max(physBall100)
    FROM tbl_zno_data
    WHERE physTestStatus = 'Зараховано'
    GROUP BY regname, year;
    '''
    cursor.execute(select_query)

    with open('result_lab1.csv', 'w', encoding="utf-8") as new_csv_file:
        csv_writer = csv.writer(new_csv_file)
        csv_writer.writerow(['Область', 'Рік', 'Макс. бал з фізики'])
        for row in cursor:
            csv_writer.writerow(row)


statistical_query()




#conn.commit()
cursor.close()
conn.close()

