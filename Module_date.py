import datetime
import time
import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def get_date_info(process_date,holiday_df):
    date_info_list = [
        get_date_format(process_date),
        get_year(process_date),
        get_quarter(process_date),
        get_month(process_date),
        get_week(process_date),
        get_year_to_month(process_date),
        get_year_to_week(process_date),
        get_week_to_day(process_date),
        get_week_interval(process_date),
        get_holiday(process_date,holiday_df),
        datetime.datetime.now()
        ]
    return date_info_list

def get_date_format(process_date):
    return process_date.strftime("%Y-%m-%d")

def get_year(process_date):
    return int(process_date.year)

def get_quarter(process_date):
    return (process_date.month-1)//3 + 1

def get_month(process_date):
    return int(process_date.month)

def get_week(process_date):
    return int(process_date.strftime("%W"))+1

def get_year_to_month(process_date):
    return int(process_date.strftime("%Y%m"))

def get_year_to_week(process_date):
    return int(process_date.strftime("%Y%W"))+1

def get_week_to_day(process_date):
    return int(process_date.strftime("%u"))

def get_week_interval(process_date):
    week_start_time = process_date - datetime.timedelta(days=process_date.weekday())
    week_end_time = week_start_time + datetime.timedelta(days=6)
    week_interval = week_start_time.strftime("%Y/%m/%d")+"-"+week_end_time.strftime("%Y/%m/%d")
    return week_interval      

def get_holiday(process_date,holiday_df):
    holiday = ''
    for i, holiday_date in enumerate(holiday_df['date']):
        if (process_date.strftime("%Y-%m-%d %H:%M:%S") == str(holiday_date) and holiday_df['isholiday'][i] == "是"):
            holiday = '2'
            break
        elif (process_date.strftime("%Y-%m-%d %H:%M:%S") == str(holiday_date) and holiday_df['isholiday'][i] == "否"):
            holiday = '1'
            break
        elif int(process_date.strftime("%u")) in (6,7):
            holiday = '2'
            break
        else:
            holiday = '1'
    return holiday

def get_holiday_df(process_date):
    holiday_df = pd.DataFrame()
    url = 'https://data.ntpc.gov.tw/api/datasets/308DCD75-6434-45BC-A95F-584DA4FED251/json?'
    headers = {'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.93 Safari/537.36"}
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount('https://', adapter)
    for page in range(process_date.year-2013):
        try:
            params_dict = {'page': page, 'size': 150}
            response = session.get(url, headers=headers, params=params_dict)
            response.raise_for_status()
            api_data = response.json()
            if api_data:
                holiday_df = pd.concat([holiday_df, pd.DataFrame(api_data)], ignore_index=True)
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout, requests.exceptions.HTTPError, requests.exceptions.TooManyRedirects) as e:
            print(f"An error occurred: {e}")
            continue
    return holiday_df

def get_date_df(start_date, end_date, holiday_df):
    """
    get_date_df [
        透過傳入起始日期 & 截止日期 & holiday_list 建立大量日期
    ]

    Args:
        start_date (datetime): [起始日期]
        end_date (datetime): [截止日期]
        holiday_list (list, optional): [國定假日 列表]
    """
    date_list = [x for x in list(pd.date_range(start=start_date, end=end_date))]
    res_list = []
    res_df = pd.DataFrame()
    for ddate in date_list:
        res_list.append(get_date_info(ddate,holiday_df))
    res_df = pd.DataFrame(res_list,
                columns=["DATA_DATE","DATA_YEAR","DATA_QUARTER","DATA_MONTH","DATA_WEEK",
                         "DATA_YEAR_TO_MONTH","DATA_YEAR_TO_WEEK","DATA_WEEK_TO_DAY",
                         "DATA_YEAR_TO_WEEK_INTERVAL","IS_HOLIDAY","INSERT_DATE"])
    return res_df

def process_holidays(df, make_up_day, not_holiday_list, special_holiday_list):
    if not make_up_day:
        df.loc[df['holidaycategory'] == '補行上班日', 'isholiday'] = '是'
    df.loc[df['Date'].isin(not_holiday_list), 'isholiday'] = '否'

    for date in special_holiday_list:
        new_row = {
            'Date': date, 
            'chinese': '',
            'isholiday': '是',
            'holidaycategory': '特殊假日',
            'description': '',}
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    return df

