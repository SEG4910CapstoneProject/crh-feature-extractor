# feature-extractor

This service extracts data (such as IOCs) algorithmically and classifies article categories using a rule-based approach.

## Environment Variables

### Database
| Environment Variable | Description                                                     |
|----------------------|-----------------------------------------------------------------|
| POSTGRES_HOST        | The hostname of the postgres database                           |
| POSTGRES_PORT        | The port of the postgres database                               |
| POSTGRES_DB_NAME     | The name of the db for the postgres database                    |
| POSTGRES_USERNAME    | Postgres username for access. Should contain write permissions  |
| POSTGRES_PASSWORD    | Postgres password for authentication                            |
| MONGO_HOST           | The hostname of the mongodb databas                             |
| MONGO_PORT           | The port of the mongodb database                                |
| MONGO_USERNAME       | The username for access. Should container write permisions      |
| MONGO_PASSWORD       | Mongodb password for authentication                             |
| MONGO_DB_NAME        | Mongodb Database name                                           |
| DB_MAX_RETRIES       | The maximum allowable retries when db commands fail. Default: 3 |

### Other
| Environment Variable | Description                                                                                                                                                  |
|----------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------|
| THREADS_PER_CORE     | The number of threads to create per core. This number should be greater than 1 due to the large number of blocking Database read and write calls. Default: 3 |
| PROGRAM_TIMEOUT      | If the execution of this service exceeds this time in seconds. It will automatically force shutdown. Default: 10800 seconds / 3 hours                        |
| LOG_FREQUENCY        | The frequency the program will report completed article extraction. For example if 10, then every 10th completion will log to console. Default: 25           |

## Configuring IOC Extractor
The ioc extractor supports many IOCs. To configure which iocs are available, modify `iocIdToIdMapping` in `src/config.py`.
The mapping consists of the IOC id according to [IOCSearcher](https://github.com/malicialab/iocsearcher) followed by the 
type id in the postgres database.

### Modifying IOC Searcher Patterns
To modify the regex expressions, edit the `data/ioc_patterns.ini` file. When changes are made you may have to modify normalization
functions in `src/ioc_searcher.py`. Find the function to override in the [IOCSearcher](https://github.com/malicialab/iocsearcher)
library and define your changes there. 

## Running Unit Tests
To run unit tests execute the below command. Must be executed on root of the project as working directory
```commandline
python -m unittest discover
```
## Running the Service
To run the service execute the below command  Must be executed on root of the project as working directory.
```commandline
python ./__main__.py
```
