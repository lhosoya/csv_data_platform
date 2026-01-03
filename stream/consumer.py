
# %%
from kafka import KafkaConsumer
# NOTE: Requires port-forward: kubectl port-forward -n kafka svc/my-cluster-kafka-external-bootstrap 19094:9094
consumer = KafkaConsumer('test-topic',
                         bootstrap_servers='localhost:19094',
                         auto_offset_reset='earliest',
                         group_id='py-group',
                         value_deserializer=lambda v: v.decode('utf-8'))
for msg in consumer:
    print(msg.value)
# %%
