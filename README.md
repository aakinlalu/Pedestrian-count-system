
## Pedestrian Counting System

### Code structure
---
pedenstrian-count-system  
    |-configs  
    ||-configs.py   
    |-data  
    |-dashboard  
    |   |-app.py  
    |-logger  
    |   |-logger.py  
    |-pedestrian_count_system  
    |   |-api_call.py  
    |   |-etl.py  
    |-tests  
    |-main.py  

### Component Definition
---
#### Configs
The directory contains python call to AWS authentication.

#### data
The directory is a temp data location for experimentation

#### dashboard
The directory contains script that performs visualization report of Top 10 locations by Day and by Month.

#### logger
The directory contains a script that perform logging of events

#### pedestrian_count_system
The directory contains the core functons of the application. The api_call has functions that utilitise the urls to fetch data either month by month or year by year or both. The etl script has spark functions to read the files fetched by api_call which are json and write them as parquet files.

#### tests
Test directory

#### main
The script deploys the application in terminal. The script provides answer to the following question:
    1. The top ten locations by day.
    2. The top ten locations by month

### Build and Deploy
---
To build and deploy locally: Please refer to Makefile for detailsl
```shell
make
```
