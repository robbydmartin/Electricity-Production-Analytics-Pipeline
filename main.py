from data.electricity_api import poll_api
from kafka_utils.create_topic import create_single_topic

def main():

    poll_api()
    create_single_topic('electricity-production')

if __name__ == '__main__':
    main()