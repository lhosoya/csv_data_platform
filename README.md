CSV data platform


### Install make
### Install docker
### Install docker-compose


### Run docker-compose
```docker-compose -f ./docker-compose-test.yml up -d ```

### Delete everything /w volumes
```docker-compose -f ./docker-compose-test.yml down -v ```

### Delete everything /wo the volumes
```docker-compose -f ./docker-compose-test.yml down ```

### Case maths
#### 1M events
| Events/day  | Events/time |
| ------------- |:-------------:|
| 1M events / 86400s      | 12 events/s     |
| 1M events / 1440min      | 695 events/min     |
| 1M events / 24h      | 41667 events/h     |


#### 10M events
| Events/day  | Events/time |
| ------------- |:-------------:|
| 10M events / 86400s      | 116 events/s     |
| 10M events / 1440min      | 6945 events/min     |
| 10M events / 24h      | 416667 events/h     |


>#### 100M events
| Events/day  | Events/time |
| ------------- |:-------------:|
| 100M events / 86400s      | 1158 events/s    |
| 100M events / 1440min      | 69445 events/min     |
| 100M events / 24h      | 4166667 events/h    |


If up to 10M events/day -> partition by date may be enough
I equal or higher than 100M events/day -> partition by date + hour may perform better for faster analysis (minute-by-minute, second-by-second)