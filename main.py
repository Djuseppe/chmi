import logging
import argparse
from parser_lib import CHMIParser
from influxdb import DataFrameClient


logger = logging.getLogger(__name__)
logging.basicConfig()
logger.setLevel(logging.DEBUG)
stream_handler = logging.StreamHandler()


def parse_args():
    parser = argparse.ArgumentParser(
        description='example code to play with InfluxDB')
    parser.add_argument('--host', type=str, required=False,
                        default='vpn.feramat.com',
                        help='hostname of influx db')
    parser.add_argument('--port', type=int, required=False, default=8086,
                        help='port of influx db')
    parser.add_argument('--username', type=str, required=False,
                        default='user',
                        help='username')
    parser.add_argument('--password', type=str, required=False,
                        default='b3WDCGoyCsiUTk9pX1dd',
                        help='password')
    parser.add_argument('--database', type=str, required=False,
                        default='db0',
                        help='database name')
    parser.add_argument('--ssl', type=bool, required=False,
                        default=True,
                        help='use ssl')
    return parser.parse_args()


def main(host, port, username, password, database, ssl):
    parser = CHMIParser()
    client = DataFrameClient(
        host=host, port=port,
        username=username, password=password,
        database=database, ssl=ssl, verify_ssl=ssl
    )
    df = parser.parse()
    client.write_points(
        dataframe=df.dropna(), database=database, measurement='soil_temp',
        tag_columns=['Stanice'],
        protocol='line'
    )


if __name__ == '__main__':
    args = parse_args()
    main(
        host=args.host, port=args.port, username=args.username,
        password=args.password, database=args.database, ssl=args.ssl
    )
