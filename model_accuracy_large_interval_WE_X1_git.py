#!/usr/bin/python3.9
#!/usr/bin/env python
# coding: utf-8

import datetime
import logging
import pathlib
import urllib
import urllib.parse
import warnings
from sys import platform

import numpy as np
import pandas as pd
import pymysql
import pyodbc
import requests
import yaml
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from sklearn.metrics import r2_score

warnings.filterwarnings("ignore")

# количество дней от текущей даты
# до нужной даты за которое грузить прогнозы моделей и выработку
today = datetime.date.today()
someday = datetime.date(2022, 9, 1)
diff = today - someday

DAYS_COUNT = int(diff.days)

# база из которой грузить модели (архив или основная на 40 дней)
WORKING_DB_ARCHIVE = "treid_03.weather_foreca_archive"
WORKING_DB = "treid_03.weather_foreca"

# Общий раздел

# Настройки для логера
if platform == "linux" or platform == "linux2":
    logging.basicConfig(
        filename="/var/log/log-execute/model_accuracy.log.txt",
        level=logging.INFO,
        format=(
            "%(asctime)s - %(levelname)s - "
            "%(funcName)s: %(lineno)d - %(message)s"
        ),
    )
elif platform == "win32":
    logging.basicConfig(
        filename=(
            f"{pathlib.Path(__file__).parent.absolute()}"
            "/model_accuracy.log.txt"
        ),
        level=logging.INFO,
        format=(
            "%(asctime)s - %(levelname)s - "
            "%(funcName)s: %(lineno)d - %(message)s"
        ),
    )

# Загружаем yaml файл с настройками
with open(
    f"{pathlib.Path(__file__).parent.absolute()}/settings.yaml", "r"
) as yaml_file:
    settings = yaml.safe_load(yaml_file)
telegram_settings = pd.DataFrame(settings["telegram"])
sql_settings = pd.DataFrame(settings["sql_db"])
pyodbc_settings = pd.DataFrame(settings["pyodbc_db"])
postgresql_settings = pd.DataFrame(settings["postgresql_db"])

# Функция отправки уведомлений в telegram на любое количество каналов
# (указать данные в yaml файле настроек)


def telegram(i, text):
    msg = urllib.parse.quote(str(text))
    bot_token = str(telegram_settings.bot_token[i])
    channel_id = str(telegram_settings.channel_id[i])

    retry_strategy = Retry(
        total=3,
        status_forcelist=[101, 429, 500, 502, 503, 504],
        method_whitelist=["GET", "POST"],
        backoff_factor=1,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("https://", adapter)
    http.mount("http://", adapter)

    http.post(
        f"https://api.telegram.org/bot{bot_token}/sendMessage?chat_id={channel_id}&text={msg}",
        timeout=10,
    )


# Функция коннекта к базе Mysql
# (для выбора базы задать порядковый номер числом !!! начинается с 0 !!!!!)


def connection(i):
    host_yaml = str(sql_settings.host[i])
    user_yaml = str(sql_settings.user[i])
    port_yaml = int(sql_settings.port[i])
    password_yaml = str(sql_settings.password[i])
    database_yaml = str(sql_settings.database[i])
    return pymysql.connect(
        host=host_yaml,
        user=user_yaml,
        port=port_yaml,
        password=password_yaml,
        database=database_yaml,
    )


# Конец Общего раздела


# Замер времени выполнения начало
start_time = datetime.datetime.now()
print(start_time)
logging.info("Старт. Расчет точности моделей.")

# Функция загрузки факта выработки
# (для выбора базы задать порядковый номер числом !!! начинается с 0 !!!!!)


def fact_load(i, dt):
    server = str(pyodbc_settings.host[i])
    database = str(pyodbc_settings.database[i])
    username = str(pyodbc_settings.user[i])
    password = str(pyodbc_settings.password[i])
    # Выбор драйвера в зависимости от ОС
    if platform == "linux" or platform == "linux2":
        connection_ms = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};SERVER="
            + server
            + ";DATABASE="
            + database
            + ";UID="
            + username
            + ";PWD="
            + password
        )
    elif platform == "win32":
        connection_ms = pyodbc.connect(
            "DRIVER={SQL Server};SERVER="
            + server
            + ";DATABASE="
            + database
            + ";UID="
            + username
            + ";PWD="
            + password
        )
    #
    mssql_cursor = connection_ms.cursor()
    mssql_cursor.execute(
        "SELECT SUBSTRING (Points.PointName ,"
        "len(Points.PointName)-8, 8) as gtp, MIN(DT) as DT,"
        " SUM(Val) as Val FROM Points JOIN PointParams ON "
        "Points.ID_Point=PointParams.ID_Point JOIN PointMains"
        " ON PointParams.ID_PP=PointMains.ID_PP WHERE "
        "PointName like 'Генерация%{G%' AND ID_Param=153 "
        "AND DT >= "
        + str(dt)
        + " AND PointName NOT LIKE "
        "'%GVIE0001%' AND PointName NOT LIKE '%GVIE0012%' "
        "AND PointName NOT LIKE '%GVIE0416%' AND PointName "
        "NOT LIKE '%GVIE0167%' AND PointName NOT LIKE "
        "'%GVIE0264%' AND PointName NOT LIKE '%GVIE0007%' "
        "AND PointName NOT LIKE '%GVIE0680%' AND PointName "
        "NOT LIKE '%GVIE0987%' AND PointName NOT LIKE "
        "'%GVIE0988%' AND PointName NOT LIKE '%GVIE0989%' "
        "AND PointName NOT LIKE '%GVIE0991%' AND PointName "
        "NOT LIKE '%GVIE0992%' AND PointName NOT LIKE "
        "'%GVIE0993%' AND PointName NOT LIKE '%GVIE0994%' "
        "AND PointName NOT LIKE '%GVIE1372%' "
        "GROUP BY SUBSTRING (Points.PointName "
        ",len(Points.PointName)-8, 8), DATEPART(YEAR, DT), "
        "DATEPART(MONTH, DT), DATEPART(DAY, DT), "
        "DATEPART(HOUR, DT) ORDER BY SUBSTRING "
        "(Points.PointName ,len(Points.PointName)-8, 8), "
        "DATEPART(YEAR, DT), DATEPART(MONTH, DT), "
        "DATEPART(DAY, DT), DATEPART(HOUR, DT);"
    )
    fact = mssql_cursor.fetchall()
    connection_ms.close()
    fact = pd.DataFrame(np.array(fact), columns=["gtp", "dt", "fact"])
    fact.drop_duplicates(
        subset=["gtp", "dt"], keep="last", inplace=True, ignore_index=False
    )
    fact["date"] = fact["dt"].astype("str").str[0:-9]
    fact["hour"] = fact["dt"].astype("str").str[-8:-6]
    return fact


fact = fact_load(
    0, f"DATEADD(HOUR, -{DAYS_COUNT} * 24, DATEDIFF(d, 0, GETDATE()))"
)

# Загрузка прогнозов моделей
logging.info("Старт. Загрузка прогнозов моделей.")
connection_forecast = connection(0)
with connection_forecast.cursor() as cursor:
    # cblg
    cblg_sql = (
        "SELECT gtp, dt, load_time, value 'cblg' FROM"
        f" {WORKING_DB_ARCHIVE} WHERE id_foreca = 14 AND (HOUR(load_time) < 15"
        " AND DATE(load_time) = DATE_ADD(DATE(dt), INTERVAL -1 DAY)) AND"
        f" DATE(dt) between DATE_ADD(CURDATE(), INTERVAL -{DAYS_COUNT} DAY)"
        " and DATE_ADD(CURDATE(), INTERVAL -1 DAY) ORDER BY gtp, dt;"
    )
    cursor.execute(cblg_sql)
    cblg_dataframe = pd.DataFrame(
        cursor.fetchall(),
        columns=["gtp_cblg", "dt_cblg", "load_time_cblg", "value_cblg"],
    )
    cblg = (
        f"SELECT gtp, dt, load_time, value 'cblg' FROM {WORKING_DB} WHERE"
        " id_foreca = 14 AND (HOUR(load_time) < 15 AND DATE(load_time) ="
        " DATE_ADD(DATE(dt), INTERVAL -1 DAY)) AND DATE(dt) between"
        f" DATE_ADD(CURDATE(), INTERVAL -{DAYS_COUNT} DAY) and"
        " DATE_ADD(CURDATE(), INTERVAL -1 DAY) ORDER BY gtp, dt;"
    )
    cursor.execute(cblg)
    cblg_df = pd.DataFrame(
        cursor.fetchall(),
        columns=["gtp_cblg", "dt_cblg", "load_time_cblg", "value_cblg"],
    )
    cblg_dataframe = pd.concat([cblg_dataframe, cblg_df], axis=0)
    cblg_dataframe.drop_duplicates(
        subset=["gtp_cblg", "dt_cblg"],
        keep="last",
        inplace=True,
        ignore_index=False,
    )

    # wpgq
    wpgq_sql = (
        "SELECT gtp, dt, load_time, value 'wpgq' FROM"
        f" {WORKING_DB_ARCHIVE} WHERE id_foreca = 13 AND (HOUR(load_time) < 15"
        " AND DATE(load_time) = DATE_ADD(DATE(dt), INTERVAL -1 DAY)) AND"
        f" DATE(dt) between DATE_ADD(CURDATE(), INTERVAL -{DAYS_COUNT} DAY)"
        " and DATE_ADD(CURDATE(), INTERVAL -1 DAY) ORDER BY gtp, dt;"
    )
    cursor.execute(wpgq_sql)
    wpgq_dataframe = pd.DataFrame(
        cursor.fetchall(),
        columns=["gtp_wpgq", "dt_wpgq", "load_time_wpgq", "value_wpgq"],
    )
    wpgq = (
        f"SELECT gtp, dt, load_time, value 'wpgq' FROM {WORKING_DB} WHERE"
        " id_foreca = 13 AND (HOUR(load_time) < 15 AND DATE(load_time) ="
        " DATE_ADD(DATE(dt), INTERVAL -1 DAY)) AND DATE(dt) between"
        f" DATE_ADD(CURDATE(), INTERVAL -{DAYS_COUNT} DAY) and"
        " DATE_ADD(CURDATE(), INTERVAL -1 DAY) ORDER BY gtp, dt;"
    )
    cursor.execute(wpgq)
    wpgq_df = pd.DataFrame(
        cursor.fetchall(),
        columns=["gtp_wpgq", "dt_wpgq", "load_time_wpgq", "value_wpgq"],
    )
    wpgq_dataframe = pd.concat([wpgq_dataframe, wpgq_df], axis=0)
    wpgq_dataframe.drop_duplicates(
        subset=["gtp_wpgq", "dt_wpgq"],
        keep="last",
        inplace=True,
        ignore_index=False,
    )

    # rp5_1da
    rp5_1da_sql = (
        "SELECT gtp, dt, load_time, value 'rp5_1da' FROM"
        f" {WORKING_DB_ARCHIVE} WHERE id_foreca = 16 AND (HOUR(load_time) < 15"
        " AND DATE(load_time) = DATE_ADD(DATE(dt), INTERVAL -1 DAY)) AND"
        f" DATE(dt) between DATE_ADD(CURDATE(), INTERVAL -{DAYS_COUNT} DAY)"
        " and DATE_ADD(CURDATE(), INTERVAL -1 DAY) ORDER BY gtp, dt;"
    )
    cursor.execute(rp5_1da_sql)
    rp5_1da_dataframe = pd.DataFrame(
        cursor.fetchall(),
        columns=[
            "gtp_rp5_1da",
            "dt_rp5_1da",
            "load_time_rp5_1da",
            "value_rp5_1da",
        ],
    )
    rp5_1da = (
        f"SELECT gtp, dt, load_time, value 'rp5_1da' FROM {WORKING_DB} WHERE"
        " id_foreca = 16 AND (HOUR(load_time) < 15 AND DATE(load_time) ="
        " DATE_ADD(DATE(dt), INTERVAL -1 DAY)) AND DATE(dt) between"
        f" DATE_ADD(CURDATE(), INTERVAL -{DAYS_COUNT} DAY) and"
        " DATE_ADD(CURDATE(), INTERVAL -1 DAY) ORDER BY gtp, dt;"
    )
    cursor.execute(rp5_1da)
    rp5_1da_df = pd.DataFrame(
        cursor.fetchall(),
        columns=[
            "gtp_rp5_1da",
            "dt_rp5_1da",
            "load_time_rp5_1da",
            "value_rp5_1da",
        ],
    )
    rp5_1da_dataframe = pd.concat([rp5_1da_dataframe, rp5_1da_df], axis=0)
    rp5_1da_dataframe.drop_duplicates(
        subset=["gtp_rp5_1da", "dt_rp5_1da"],
        keep="last",
        inplace=True,
        ignore_index=False,
    )

    # cbr_rp5
    cbr_rp5_sql = (
        "SELECT gtp, dt, load_time, value 'cbr_rp5' FROM"
        f" {WORKING_DB_ARCHIVE} WHERE id_foreca = 11 AND (HOUR(load_time) < 15"
        " AND DATE(load_time) = DATE_ADD(DATE(dt), INTERVAL -1 DAY)) AND"
        f" DATE(dt) between DATE_ADD(CURDATE(), INTERVAL -{DAYS_COUNT} DAY)"
        " and DATE_ADD(CURDATE(), INTERVAL -1 DAY) ORDER BY gtp, dt;"
    )
    cursor.execute(cbr_rp5_sql)
    cbr_rp5_dataframe = pd.DataFrame(
        cursor.fetchall(),
        columns=[
            "gtp_cbr_rp5",
            "dt_cbr_rp5",
            "load_time_cbr_rp5",
            "value_cbr_rp5",
        ],
    )
    cbr_rp5 = (
        f"SELECT gtp, dt, load_time, value 'cbr_rp5' FROM {WORKING_DB} WHERE"
        " id_foreca = 11 AND (HOUR(load_time) < 15 AND DATE(load_time) ="
        " DATE_ADD(DATE(dt), INTERVAL -1 DAY)) AND DATE(dt) between"
        f" DATE_ADD(CURDATE(), INTERVAL -{DAYS_COUNT} DAY) and"
        " DATE_ADD(CURDATE(), INTERVAL -1 DAY) ORDER BY gtp, dt;"
    )
    cursor.execute(cbr_rp5)
    cbr_rp5_df = pd.DataFrame(
        cursor.fetchall(),
        columns=[
            "gtp_cbr_rp5",
            "dt_cbr_rp5",
            "load_time_cbr_rp5",
            "value_cbr_rp5",
        ],
    )
    cbr_rp5_dataframe = pd.concat([cbr_rp5_dataframe, cbr_rp5_df], axis=0)
    cbr_rp5_dataframe.drop_duplicates(
        subset=["gtp_cbr_rp5", "dt_cbr_rp5"],
        keep="last",
        inplace=True,
        ignore_index=False,
    )

    # visualcrossing
    visualcrossing_sql = (
        "SELECT gtp, dt, load_time, value 'visualcrossing' FROM"
        f" {WORKING_DB_ARCHIVE} WHERE id_foreca = 18 AND (HOUR(load_time) < 15"
        " AND DATE(load_time) = DATE_ADD(DATE(dt), INTERVAL -1 DAY)) AND"
        f" DATE(dt) between DATE_ADD(CURDATE(), INTERVAL -{DAYS_COUNT} DAY)"
        " and DATE_ADD(CURDATE(), INTERVAL -1 DAY) ORDER BY gtp, dt;"
    )
    cursor.execute(visualcrossing_sql)
    visualcrossing_dataframe = pd.DataFrame(
        cursor.fetchall(),
        columns=[
            "gtp_visualcrossing",
            "dt_visualcrossing",
            "load_time_visualcrossing",
            "value_visualcrossing",
        ],
    )
    visualcrossing = (
        "SELECT gtp, dt, load_time, value 'visualcrossing' FROM"
        f" {WORKING_DB} WHERE id_foreca = 18 AND (HOUR(load_time) < 15 AND"
        " DATE(load_time) = DATE_ADD(DATE(dt), INTERVAL -1 DAY)) AND DATE(dt)"
        f" between DATE_ADD(CURDATE(), INTERVAL -{DAYS_COUNT} DAY) and"
        " DATE_ADD(CURDATE(), INTERVAL -1 DAY) ORDER BY gtp, dt;"
    )
    cursor.execute(visualcrossing)
    visualcrossing_df = pd.DataFrame(
        cursor.fetchall(),
        columns=[
            "gtp_visualcrossing",
            "dt_visualcrossing",
            "load_time_visualcrossing",
            "value_visualcrossing",
        ],
    )
    visualcrossing_dataframe = pd.concat(
        [visualcrossing_dataframe, visualcrossing_df], axis=0
    )
    visualcrossing_dataframe.drop_duplicates(
        subset=["gtp_visualcrossing", "dt_visualcrossing"],
        keep="last",
        inplace=True,
        ignore_index=False,
    )

    # OpenMeteo
    openmeteo_sql = (
        "SELECT gtp, dt, load_time, value 'openmeteo' FROM"
        f" {WORKING_DB_ARCHIVE} WHERE id_foreca = 20 AND (HOUR(load_time) < 15"
        " AND DATE(load_time) = DATE_ADD(DATE(dt), INTERVAL -1 DAY)) AND"
        f" DATE(dt) between DATE_ADD(CURDATE(), INTERVAL -{DAYS_COUNT} DAY)"
        " and DATE_ADD(CURDATE(), INTERVAL -1 DAY) ORDER BY gtp, dt;"
    )
    cursor.execute(openmeteo_sql)
    openmeteo_dataframe = pd.DataFrame(
        cursor.fetchall(),
        columns=[
            "gtp_openmeteo",
            "dt_openmeteo",
            "load_time_openmeteo",
            "value_openmeteo",
        ],
    )
    openmeteo = (
        f"SELECT gtp, dt, load_time, value 'openmeteo' FROM {WORKING_DB} WHERE"
        " id_foreca = 20 AND (HOUR(load_time) < 15 AND DATE(load_time) ="
        " DATE_ADD(DATE(dt), INTERVAL -1 DAY)) AND DATE(dt) between"
        f" DATE_ADD(CURDATE(), INTERVAL -{DAYS_COUNT} DAY) and"
        " DATE_ADD(CURDATE(), INTERVAL -1 DAY) ORDER BY gtp, dt;"
    )
    cursor.execute(openmeteo)
    openmeteo_df = pd.DataFrame(
        cursor.fetchall(),
        columns=[
            "gtp_openmeteo",
            "dt_openmeteo",
            "load_time_openmeteo",
            "value_openmeteo",
        ],
    )
    openmeteo_dataframe = pd.concat(
        [openmeteo_dataframe, openmeteo_df], axis=0
    )
    openmeteo_dataframe.drop_duplicates(
        subset=["gtp_openmeteo", "dt_openmeteo"],
        keep="last",
        inplace=True,
        ignore_index=False,
    )

    # tomorrow_io
    tomorrow_io_sql = (
        "SELECT gtp, dt, load_time, value 'tomorrow_io' FROM"
        f" {WORKING_DB_ARCHIVE} WHERE id_foreca = 22 AND (HOUR(load_time) < 15"
        " AND DATE(load_time) = DATE_ADD(DATE(dt), INTERVAL -1 DAY)) AND"
        f" DATE(dt) between DATE_ADD(CURDATE(), INTERVAL -{DAYS_COUNT} DAY)"
        " and DATE_ADD(CURDATE(), INTERVAL -1 DAY) ORDER BY gtp, dt;"
    )
    cursor.execute(tomorrow_io_sql)
    tomorrow_io_dataframe = pd.DataFrame(
        cursor.fetchall(),
        columns=[
            "gtp_tomorrow_io",
            "dt_tomorrow_io",
            "load_time_tomorrow_io",
            "value_tomorrow_io",
        ],
    )
    tomorrow_io = (
        "SELECT gtp, dt, load_time, value 'tomorrow_io' FROM"
        f" {WORKING_DB} WHERE id_foreca = 22 AND (HOUR(load_time) < 15 AND"
        " DATE(load_time) = DATE_ADD(DATE(dt), INTERVAL -1 DAY)) AND DATE(dt)"
        f" between DATE_ADD(CURDATE(), INTERVAL -{DAYS_COUNT} DAY) and"
        " DATE_ADD(CURDATE(), INTERVAL -1 DAY) ORDER BY gtp, dt;"
    )
    cursor.execute(tomorrow_io)
    tomorrow_io_df = pd.DataFrame(
        cursor.fetchall(),
        columns=[
            "gtp_tomorrow_io",
            "dt_tomorrow_io",
            "load_time_tomorrow_io",
            "value_tomorrow_io",
        ],
    )
    tomorrow_io_dataframe = pd.concat(
        [tomorrow_io_dataframe, tomorrow_io_df], axis=0
    )
    tomorrow_io_dataframe.drop_duplicates(
        subset=["gtp_tomorrow_io", "dt_tomorrow_io"],
        keep="last",
        inplace=True,
        ignore_index=False,
    )

connection_forecast.close()
logging.info("Прогнозы моделей загружены.")

# Склеиваем факт и прогнозы моделей, считаем среднее и максимум всех
logging.info("Старт. Склейка датафрейма для расчета.")
temp_dataframe = fact.merge(
    cblg_dataframe,
    left_on=["gtp", "dt"],
    right_on=["gtp_cblg", "dt_cblg"],
    how="left",
)
temp_dataframe = temp_dataframe.merge(
    wpgq_dataframe,
    left_on=["gtp", "dt"],
    right_on=["gtp_wpgq", "dt_wpgq"],
    how="left",
)
temp_dataframe = temp_dataframe.merge(
    rp5_1da_dataframe,
    left_on=["gtp", "dt"],
    right_on=["gtp_rp5_1da", "dt_rp5_1da"],
    how="left",
)
temp_dataframe = temp_dataframe.merge(
    cbr_rp5_dataframe,
    left_on=["gtp", "dt"],
    right_on=["gtp_cbr_rp5", "dt_cbr_rp5"],
    how="left",
)
temp_dataframe = temp_dataframe.merge(
    visualcrossing_dataframe,
    left_on=["gtp", "dt"],
    right_on=["gtp_visualcrossing", "dt_visualcrossing"],
    how="left",
)
temp_dataframe = temp_dataframe.merge(
    openmeteo_dataframe,
    left_on=["gtp", "dt"],
    right_on=["gtp_openmeteo", "dt_openmeteo"],
    how="left",
)
temp_dataframe = temp_dataframe.merge(
    tomorrow_io_dataframe,
    left_on=["gtp", "dt"],
    right_on=["gtp_tomorrow_io", "dt_tomorrow_io"],
    how="left",
)
temp_dataframe.fillna(0, inplace=True)
temp_dataframe.drop(
    [
        "gtp_cblg",
        "dt_cblg",
        "load_time_cblg",
        "gtp_wpgq",
        "dt_wpgq",
        "load_time_wpgq",
        "gtp_rp5_1da",
        "dt_rp5_1da",
        "load_time_rp5_1da",
        "gtp_cbr_rp5",
        "dt_cbr_rp5",
        "load_time_cbr_rp5",
        "gtp_visualcrossing",
        "dt_visualcrossing",
        "load_time_visualcrossing",
        "gtp_openmeteo",
        "dt_openmeteo",
        "load_time_openmeteo",
        "gtp_tomorrow_io",
        "dt_tomorrow_io",
        "load_time_tomorrow_io",
    ],
    axis="columns",
    inplace=True,
)
temp_dataframe["value_aver"] = temp_dataframe[
    [
        "value_cblg",
        "value_wpgq",
        "value_rp5_1da",
        "value_cbr_rp5",
        "value_visualcrossing",
        "value_openmeteo",
        "value_tomorrow_io",
    ]
].mean(axis=1)
temp_dataframe["value_max"] = temp_dataframe[
    [
        "value_cblg",
        "value_wpgq",
        "value_rp5_1da",
        "value_cbr_rp5",
        "value_visualcrossing",
        "value_openmeteo",
        "value_tomorrow_io",
    ]
].max(axis=1)

# находим величину ошибки по модулю
temp_dataframe["cblg"] = abs(
    temp_dataframe["fact"] - temp_dataframe["value_cblg"]
)
temp_dataframe["wpgq"] = abs(
    temp_dataframe["fact"] - temp_dataframe["value_wpgq"]
)
temp_dataframe["rp5_1da"] = abs(
    temp_dataframe["fact"] - temp_dataframe["value_rp5_1da"]
)
temp_dataframe["cbr_rp5"] = abs(
    temp_dataframe["fact"] - temp_dataframe["value_cbr_rp5"]
)
temp_dataframe["visualcrossing"] = abs(
    temp_dataframe["fact"] - temp_dataframe["value_visualcrossing"]
)
temp_dataframe["openmeteo"] = abs(
    temp_dataframe["fact"] - temp_dataframe["value_openmeteo"]
)
temp_dataframe["tomorrow_io"] = abs(
    temp_dataframe["fact"] - temp_dataframe["value_tomorrow_io"]
)

logging.info("Датафрейм для расчета подготовлен.")
temp_dataframe.to_excel("model_predicts_dataframe_X1.xlsx")
r2_score_dataframe = pd.DataFrame()

# Цикл расчета точности моделей по дням и по гтп
logging.info("Старт. Расчет точности в цикле.")
for date in temp_dataframe.date.value_counts().index:
    model_accuracy_dataframe = temp_dataframe.loc[temp_dataframe.date == date]
    for gtp in model_accuracy_dataframe.gtp.value_counts().index:
        cblg_pred = model_accuracy_dataframe.loc[
            model_accuracy_dataframe.gtp == gtp, ["value_cblg"]
        ]
        cblg_true = model_accuracy_dataframe.loc[
            model_accuracy_dataframe.gtp == gtp, ["fact"]
        ]
        cblg_r2 = r2_score(cblg_true, cblg_pred)

        wpgq_pred = model_accuracy_dataframe.loc[
            model_accuracy_dataframe.gtp == gtp, ["value_wpgq"]
        ]
        wpgq_true = model_accuracy_dataframe.loc[
            model_accuracy_dataframe.gtp == gtp, ["fact"]
        ]
        wpgq_r2 = r2_score(wpgq_true, wpgq_pred)

        rp5_1da_pred = model_accuracy_dataframe.loc[
            model_accuracy_dataframe.gtp == gtp, ["value_rp5_1da"]
        ]
        rp5_1da_true = model_accuracy_dataframe.loc[
            model_accuracy_dataframe.gtp == gtp, ["fact"]
        ]
        rp5_1da_r2 = r2_score(rp5_1da_true, rp5_1da_pred)

        cbr_rp5_pred = model_accuracy_dataframe.loc[
            model_accuracy_dataframe.gtp == gtp, ["value_cbr_rp5"]
        ]
        cbr_rp5_true = model_accuracy_dataframe.loc[
            model_accuracy_dataframe.gtp == gtp, ["fact"]
        ]
        cbr_rp5_r2 = r2_score(cbr_rp5_true, cbr_rp5_pred)

        visualcrossing_pred = model_accuracy_dataframe.loc[
            model_accuracy_dataframe.gtp == gtp, ["value_visualcrossing"]
        ]
        visualcrossing_true = model_accuracy_dataframe.loc[
            model_accuracy_dataframe.gtp == gtp, ["fact"]
        ]
        visualcrossing_r2 = r2_score(visualcrossing_true, visualcrossing_pred)

        openmeteo_pred = model_accuracy_dataframe.loc[
            model_accuracy_dataframe.gtp == gtp, ["value_openmeteo"]
        ]
        openmeteo_true = model_accuracy_dataframe.loc[
            model_accuracy_dataframe.gtp == gtp, ["fact"]
        ]
        openmeteo_r2 = r2_score(openmeteo_true, openmeteo_pred)

        tomorrow_io_pred = model_accuracy_dataframe.loc[
            model_accuracy_dataframe.gtp == gtp, ["value_tomorrow_io"]
        ]
        tomorrow_io_true = model_accuracy_dataframe.loc[
            model_accuracy_dataframe.gtp == gtp, ["fact"]
        ]
        tomorrow_io_r2 = r2_score(tomorrow_io_true, tomorrow_io_pred)

        aver_pred = model_accuracy_dataframe.loc[
            model_accuracy_dataframe.gtp == gtp, ["value_aver"]
        ]
        aver_true = model_accuracy_dataframe.loc[
            model_accuracy_dataframe.gtp == gtp, ["fact"]
        ]
        aver_r2 = r2_score(aver_true, aver_pred)

        max_pred = model_accuracy_dataframe.loc[
            model_accuracy_dataframe.gtp == gtp, ["value_max"]
        ]
        max_true = model_accuracy_dataframe.loc[
            model_accuracy_dataframe.gtp == gtp, ["fact"]
        ]
        max_r2 = r2_score(max_true, max_pred)

        data_row = {
            "date": date,
            "gtp": gtp,
            "cblg": cblg_r2,
            "wpgq": wpgq_r2,
            "rp5_1da": rp5_1da_r2,
            "cbr_rp5": cbr_rp5_r2,
            "visualcrossing": visualcrossing_r2,
            "openmeteo": openmeteo_r2,
            "tomorrow_io": tomorrow_io_r2,
            "aver_all": aver_r2,
            "max_all": max_r2,
        }
        r2_score_dataframe = r2_score_dataframe.append(
            data_row, ignore_index=True
        )


# Добавляем столбик с названием самой точной модели
model = r2_score_dataframe.drop(["gtp", "date"], axis="columns").idxmax(axis=1)
r2_score_dataframe["model"] = model
r2_score_dataframe["date"] = r2_score_dataframe["date"].astype(
    "datetime64[ns]"
)
r2_score_dataframe["gtp"] = r2_score_dataframe["gtp"].astype("str")
r2_score_dataframe.sort_values(["gtp", "date"], inplace=True)
r2_score_dataframe.reset_index(drop=True, inplace=True)
r2_score_dataframe.to_excel("r2_score_dataframe_X1.xlsx")
r2_score_dataframe.drop(
    [
        "cblg",
        "wpgq",
        "rp5_1da",
        "cbr_rp5",
        "visualcrossing",
        "openmeteo",
        "tomorrow_io",
        "aver_all",
        "max_all",
    ],
    axis="columns",
    inplace=True,
)
r2_score_dataframe = pd.pivot_table(
    r2_score_dataframe,
    values="model",
    index=["date"],
    columns=["gtp"],
    aggfunc=np.sum,
)
r2_score_dataframe.reset_index(inplace=True)
r2_score_dataframe.reset_index(drop=True, inplace=True)

# удаление пустой последней строки с сегодняшней датой, т.к. день не закончился
r2_score_dataframe.drop(r2_score_dataframe.tail(1).index, inplace=True)

most_accurate_model_dict = {"date": "most_accurate_model"}
for col in range(1, r2_score_dataframe.shape[1]):
    print(col)
    print(r2_score_dataframe.iloc[:, col].mode()[0])
    most_accurate_model_dict[
        r2_score_dataframe.iloc[:, col].name
    ] = r2_score_dataframe.iloc[:, col].mode()[0]

r2_score_dataframe = r2_score_dataframe.append(
    most_accurate_model_dict, ignore_index=True
)
r2_score_dataframe.to_excel("r2_score_dataframe_by_gtp_X1.xlsx")
logging.info("Точность моделей посчитана.")

# Замер времени выполнения конец
end_time = datetime.datetime.now()
delta = end_time - start_time
print(end_time)
print(delta)
