#%%
from kafka import KafkaProducer

print('a')
#%%
# NOTE: Requires port-forward: kubectl port-forward -n kafka svc/my-cluster-kafka-external-bootstrap 19094:9094
producer = KafkaProducer(bootstrap_servers='localhost:9094',
                         value_serializer=lambda v: v.encode('utf-8'))
producer.send('test', 'with docker everythings easier!')
producer.flush()
# %%