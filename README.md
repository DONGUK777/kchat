# kchat
- Python chat program using Apache Kafka

## Test

## KAFKA Producer
```bash
$ python src/kchat/kafka/pro.py
[DONE]: 0.029494762420654297
```

```bash
$ $KAFKA_HOME/bin/kafka-console-consumer.sh --topic topic1 --from-beginning --bootstrap-server localhost:9092

{"str": "value0"}
{"str": "value1"}
{"str": "value2"}
{"str": "value3"}
{"str": "value4"}
{"str": "value5"}
{"str": "value6"}
{"str": "value7"}
{"str": "value8"}
{"str": "value9"}
```
