from kafka import KafkaProducer, KafkaConsumer
import json
import time
import threading
import uuid
from json import loads

# Exam
def producer():
    p = KafkaProducer(
        bootstrap_servers=['ec2-43-203-219-5.ap-northeast-2.compute.amazonaws.com:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    print("채팅 프로그램 - 메세지 발신자")
    print("메세지를 입력하세요. (종료시 'exit' 입력)")

    while True:
        msg = input("YOU: ")
        if msg == 'exit':
            break

    data = {'message': msg, 'time': time.time()}
    p.send('chat-group', value=data)

    p.flush()

def consumer():
    unique_group_id = f'chat_group{uuid.uuid4()}'
    consumer = KafkaConsumer(
        'chat-group',
        group_id=unique_group_id,
        bootstrap_servers=['ec2-43-203-219-5.ap-northeast-2.compute.amazonaws.com:9092'],
        value_deserializer=lambda x: loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        #group_id="chat-group",
        enable_auto_commit=True,
    )

    try:
        for m in consumer:
            data = m.value
            print(f"[FRIEND] {data}")

    except KeyboardInterrupt:
        print("채팅 종료")

    finally:
        consumer.close()

if __name__ == "__main__":
    # 먼저 Consumer를 시작
    consumer_thread = threading.Thread(target=consumer)
    consumer_thread.start()

    # Consumer가 실행된 후 Producer 시작
    time.sleep(1)  # 잠깐 대기하여 Consumer가 시작될 시간을 확보
    producer_thread = threading.Thread(target=producer)
    producer_thread.start()

    consumer_thread.join()
    producer_thread.join()
