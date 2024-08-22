from kafka import KafkaProducer, KafkaConsumer
import json
from json import loads
import threading
import time
import datetime


messages = []

def receiver():
    global messages
    consumer = KafkaConsumer(
        'chat-group',
        bootstrap_servers=['ec2-43-203-219-5.ap-northeast-2.compute.amazonaws.com:9092'],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda x: loads(x.decode('utf-8'))
    )

    try:
        for m in consumer:
            data = m.value
            # 메시지를 리스트에 추가
            messages.append(f"{data['username']}: {data['message']} (받은 시간: {datetime.datetime.fromtimestamp(m.timestamp // 1000)})")
            # 메시지를 출력
            print(f"{data['username']}: {data['message']} (받은 시간: {datetime.datetime.fromtimestamp(m.timestamp // 1000)})")

    except KeyboardInterrupt:
        print("채팅종료")

    finally:
        consumer.close()

def sender(username):
    pro = KafkaProducer(
        bootstrap_servers=['ec2-43-203-219-5.ap-northeast-2.compute.amazonaws.com:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    while True:
        m = input("YOU: ")
        data = {'username': username, 'message': m, 'time': time.time()}
        pro.send('chat-group', value=data)
        pro.flush()

        if m.lower() == 'exit':
            print("채팅종료")
            break

if __name__ == "__main__":
    print("채팅 프로그램 - 대화를 시작하세요")

    username = input("채팅방에서 사용할 이름을 입력해주세요: ")
    producer_thread = threading.Thread(target=sender, args=(username,))
    consumer_thread = threading.Thread(target=receiver)

    producer_thread.start()
    consumer_thread.start()

    producer_thread.join()
    consumer_thread.join()

