CSV data platform


### Install make, docker, docker-compose in ubuntu/wsl2
```sudo apt-get update && sudo apt-get install -y make docker.io docker-compose```


### Move to ./infra first!
```cd infra```

### Build the airflow image, airflow is replacing a compute resource to mimic cloud resources.
```make build-docker```

### Deploy everything + data and connections
```make deploy-all```

### Delete everything /w the volumes
```make clean-all ```

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


#### 100M events - Not mentioned
| Events/day  | Events/time |
| ------------- |:-------------:|
| 100M events / 86400s      | 1158 events/s    |
| 100M events / 1440min      | 69445 events/min     |
| 100M events / 24h      | 4166667 events/h    |

Case review assumptions:

- If up to 10M events/day -> partition by date may be enough
- I equal or higher than 100M events/day -> partition by date + hour may perform better for faster analysis (minute-by-minute, second-by-second)
- **Batch vs streaming** Batch is a better alternative due to analytics and upsert information to be processed/reprocessed, streaming is only possible if going straight to warehouse + presentation layer in milliseconds/seconds of data availability.
- Availability and Consistency are the key for this since Partition tolerance isn't necessary for this case.
- Orchestration, lineage & observability -> Using Airflow, Clickhouse & Metabase to understand and deep-dive metadata.
- Duplication removal done at code level.

Quality:
- Tests are enforced during silver layer, since the raw layer should be getting messages from the topic/sink/landing and data can be late.
- SLI/SLO freshness is determined by the business need (Airflow schedule or API trigger, could also be a stream scenario). Pipeline success rate is by data available, but maintaining idempotency and redundancy on bronze layer.
- Alerting -> Done at Metabase level &/or Airflow level during pipeline execution.

Scalability:
- Case is already planned to scale, except removing pipeline runtime from Airflow Worker to a specific clustered environment (Glue, Databricks, Snowflake, etc.)


Focuses:
- Scaling
- Deploy
- Usage
- Idempotency
- Analytics

Current Holes:
- Airflow worker, no clustered runtimes.
- All in docker, in a PROD/DEV environment to be scalable.
- No integration with hive-metastores (glue, athena, databricks, snowflake metastores), which means working with Clickhouse/Trino/Minio is a little more difficult, especially when building as delta metadata.
- Airflow executions/tasks breakdown, either by notebook or better tasks.
- DRY, KIS -> lacked a bit due to simplicity over being a clean execution.
