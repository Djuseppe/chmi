import requests
from requests.exceptions import HTTPError
from bs4 import BeautifulSoup
import logging
import pytz
import pandas as pd
import numpy as np
from datetime import datetime
from unidecode import unidecode
from functools import wraps

logger = logging.getLogger(__name__)
logging.basicConfig()
logger.setLevel(logging.DEBUG)
stream_handler = logging.StreamHandler()


def make_request(url):
    response = None
    try:
        response = requests.get(url)
        response.raise_for_status()
    except HTTPError as http_err:
        logger.error(f'HTTP error occurred: {http_err}')
    except Exception as err:
        logger.error(f'Other error occurred: {err}')
    else:
        logger.info('Success connection!')

    # parser content
    soup = BeautifulSoup(response.content, 'html.parser')
    return soup


def write_to_file(file_name, data):
    with open(file_name, 'w') as f:
        f.write(data)


def parse_table(table):
    data = []
    # table = soup.find('table', attrs={'class': 'portlet-table-alternate'})
    # table_body = table.find('tbody')

    rows = table.find_all('tr')
    for row in rows:
        cols = row.find_all('td')
        cols = [unidecode(ele.text.strip()).replace(',', '.') for ele in cols]
        cols = [ele[:-3] if '°C' in ele else ele for ele in cols]
        data.append([ele for ele in cols if ele])
    return data


def convert_to_df(
        data, time_format='%Y.%m.%d %H:%M:%S.%z',
        tz_prague=pytz.timezone('Europe/Prague')
):
    time_val = datetime.now(tz_prague).strftime(time_format)
    if isinstance(data, list) and isinstance(data[0], list):
        df = pd.DataFrame(data[1:], columns=data[0])
        df['time'] = [time_val for _ in range(df.shape[0])]
    else:
        df = None
        logger.debug('data passed to df is not list of lists.')
    return df


class Decorators(object):
    @classmethod
    def data_checker(cls, func):
        @wraps(func)
        def wrapped(*args, **kwargs):
            data = args[1]
            if isinstance(data, list) and isinstance(data[0], list):
                res = func(*args, **kwargs)
            else:
                logger.debug('data passed to df is not list of lists.')
                res = None
            return res
        return wrapped


class CHMIParser:
    def __init__(
            self,
            url=r'http://portal.chmi.cz/files/portal/docs/meteo/opss/pocasicko_nejnovejsi/st_pudni_teploty_cz.html',
            time_format='%Y.%m.%d %H:%M:%S.%z',
            tz_prague=pytz.timezone('Europe/Prague')

    ):
        self.url = url
        self.time_format = time_format
        self.tz_prague = tz_prague

    def make_request(self):
        response = None
        try:
            response = requests.get(self.url)
            response.raise_for_status()
        except HTTPError as http_err:
            logger.error(f'HTTP error occurred: {http_err}')
        except Exception as err:
            logger.error(f'Other error occurred: {err}')
        else:
            logger.info('Success connection!')
        # parser content
        soup = BeautifulSoup(response.content, 'html.parser')
        return soup, response.status_code

    @staticmethod
    def get_table(soup):
        result = soup.find_all('table')
        if len(result) > 1:
            return result[2]
        else:
            logger.debug('Got number of found tables < 1.')

    @staticmethod
    def parse_table(table):
        data = []
        rows = table.find_all('tr')
        for row in rows:
            cols = row.find_all('td')
            cols = [unidecode(ele.text.strip()).replace(',', '.').strip(' degC') for ele in cols]
            # cols = [ele[:-5] if 'degC' in ele else ele for ele in cols]
            data.append([ele for ele in cols if ele])
        return data

    @Decorators.data_checker
    def convert_to_df(self, data):
        time_val = datetime.now(self.tz_prague).strftime(self.time_format)
        cols = [
            'Stanice',
            'Teplota pudy v hloubce 5 cm',
            'Teplota pudy v hloubce 10 cm',
            'Teplota pudy v hloubce 20 cm',
            'Teplota pudy v hloubce 50 cm',
            'Teplota pudyv hloubce 100 cm'
        ]
        df = pd.DataFrame(data[1:], columns=cols)
        df.loc[:, cols[1:]] = df.loc[:, cols[1:]].astype(float)
        for col in [i for i in cols if i not in ['Stanice', 'time']]:
            df[col] = df[col].astype(float)
        df['time'] = [time_val for _ in range(df.shape[0])]
        return df

    def parse(self):
        soup, _ = self.make_request()
        _table = self.get_table(soup)
        table = self.parse_table(_table)
        df = self.convert_to_df(table)
        return df


def main():
    # with open('data/soup.csv', 'r') as f:
    #     data = f.read()
    # print(data)
    # url = 'http://portal.chmi.cz/aktualni-situace/'
    # 'aktualni-stav-pocasi/ceska-republika/stanice/profesionalni-stanice/tabulky/teplota-pudy'
    # time_format = '%Y.%m.%d %H:%M:%S.%z'
    # tz_prague = pytz.timezone('Europe/Prague')
    # url = 'http://portal.chmi.cz/files/portal/docs/meteo/opss/pocasicko_nejnovejsi/st_pudni_teploty_cz.html'
    # soup = make_request(url)
    # result = soup.find_all('table')
    # # table_list = list()
    # table = parse_table(result[2])
    # df = convert_to_df(table)

    parser = CHMIParser()
    df = parser.parse()
    print()
    # for i, table in enumerate(result):
    #     if i == 2:
    #         print()
    #     table_list.append(parse_table(table))
    # # if not page.status_code
    # soup = BeautifulSoup(page.content, 'html.parser')
    # print()
    # page.raise_for_status()
    # table = bs.find(lambda tag: tag.name=='table' and tag.has_attr('id') and tag['id']=="Table1")


if __name__ == '__main__':
    main()
