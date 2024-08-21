from kafka import KafkaProducer
import time
import json
from tqdm import tqdm

pro = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x:json.dumps(x).encode('utf-8')
        )

start = time.time()

for i in tqdm(range(10)):
    data = {'str': 'value' + str(i)}
    pro.send('topic1', value=data)
    pro.flush()
    time.sleep(0.1)

end = time.time()
print("[DONE]:", end - start)



