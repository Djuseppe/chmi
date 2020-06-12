import requests
from requests.exceptions import HTTPError
from bs4 import BeautifulSoup
import logging
import pytz
import pandas as pd
from datetime import datetime
from unidecode import unidecode
from functools import wraps

logger = logging.getLogger(__name__)
logging.basicConfig()
logger.setLevel(logging.DEBUG)
stream_handler = logging.StreamHandler()


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
            time_format='%Y.%m.%d %H:%M:%S',
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
            logger.info('Success connection at time {}!'.format(datetime.now().strftime(self.time_format)))
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
            cols = [ele.text.strip().replace(',', '.').strip(' °C') for ele in cols]
            # cols = [ele[:-5] if 'degC' in ele else ele for ele in cols]
            data.append([ele for ele in cols if ele])
        return data

    @Decorators.data_checker
    def convert_to_df(self, data):
        time_val = pd.to_datetime(datetime.now().strftime(self.time_format)).tz_localize(self.tz_prague)
        # time_val = datetime.now(self.tz_prague)  # .strftime(self.time_format)
        cols = [
            'Stanice',
            'Teplota pudy v hloubce 5 cm',
            'Teplota pudy v hloubce 10 cm',
            'Teplota pudy v hloubce 20 cm',
            'Teplota pudy v hloubce 50 cm',
            'Teplota pudy v hloubce 100 cm'
        ]
        df = pd.DataFrame(data[1:], columns=cols)
        df.loc[:, cols[1:]] = df.loc[:, cols[1:]].astype(float)
        for col in [i for i in cols if i not in ['Stanice', 'time']]:
            df[col] = df[col].astype(float)
        df.index = [time_val for _ in range(df.shape[0])]
        df.Stanice = df.Stanice.apply(lambda x: unidecode(x))
        return df

    def parse(self):
        soup, _ = self.make_request()
        _table = self.get_table(soup)
        table = self.parse_table(_table)
        df = self.convert_to_df(table)
        return df


def main():
    parser = CHMIParser()
    client = DataFrameClient(
        host=self.host, port=self.port, database=self.dbname,
        username=self.user, password=self.password,
        ssl=self.ssl, verify_ssl=self.verify_ssl
    )
    # df = parser.parse()
    print()


if __name__ == '__main__':
    main()
