import os

# Db Variables
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')
POSTGRES_DB_NAME = os.getenv('POSTGRES_DB_NAME')
POSTGRES_USERNAME = os.getenv('POSTGRES_USERNAME')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
MONGO_HOST = os.getenv('MONGO_HOST')
MONGO_PORT = os.getenv('MONGO_PORT')
MONGO_USERNAME = os.getenv('MONGO_USERNAME')
MONGO_PASSWORD = os.getenv('MONGO_PASSWORD')
MONGO_DB_NAME = os.getenv('MONGO_DB_NAME')
MONGO_COLLECTION = os.getenv('MONGO_COLLECTION', "articleContent")
DB_MAX_RETRIES = int(os.getenv('DB_MAX_RETRIES', "3"))

# Threading Variables
THREADS_PER_CORE = int(os.getenv('THREADS_PER_CORE', "3"))

# Retry mechanism
PROGRAM_TIMEOUT = float(os.getenv('PROGRAM_TIMEOUT', "10800"))

# Logging
LOG_FREQUENCY = int(os.getenv('LOG_FREQUENCY', "25"))

# Memory
SOURCE_FILTER_CACHE_SIZE = int(os.getenv('SOURCE_FILTER_CACHE_SIZE', '5'))

# Uncomment IOCs to include. They must be mapped to their ids in the database
# IOC string ids are found here: https://github.com/malicialab/iocsearcher?tab=readme-ov-file#supported-iocs
iocIdToIdMapping = {
        "url": 1,
        "fqdn": 2,
        "ip4": 3,
        "ip6": 4,
        # "ip4Net": 5,
        "md5": 6,
        "sha1": 7,
        "sha256": 8,
        "email": 9,
        #"phoneNumber": 10,
        #"copyright": 11,
        "cve": 12,
        # "onionAddress": 13,
        #"facebookHandle": 14,
        "githubHandle": 15,
        #"instagramHandle": 16,
        #"linkedinHandle": 17,
        #"pinterestHandle": 18,
        "telegramHandle": 19,
        #"twitterHandle": 20,
        #"whatsappHandle": 21,
        #"youtubeHandle": 22,
        #"youtubeChannel": 23,
        #"googleAdsense": 24,
        #"googleAnalytics": 25,
        #"googleTagManager": 26,
        "bitcoin": 27,
        # "bitcoincash": 28,
        #"cardano": 29,
        #"dashcoin": 30,
        "dogecoin": 31,
        "ethereum": 32,
        #"litecoin": 33,
        "monero": 34,
        # "ripple": 35,
        #"tezos": 36,
        #"tronix": 37,
        #"zcash": 38,
        #"webmoney": 39,
        # "icp": 40,
        # "iban": 41,
        #"trademark": 42,
        # "uuid": 43,
        "packageName": 44,
        # "nif": 45,
        "registry": 46,
        "filename": 47,
        "filepath": 48,
        "sha512": 49
    }

working_directory = os.getcwd()
data_folder_directory = os.path.join(working_directory, 'data/')
ioc_patterns_file = os.path.join(data_folder_directory, 'ioc_patterns.ini')

LOGGER_FORMAT = "%(asctime)s %(levelname)s P%(process)d [%(name)s]: %(message)s"
