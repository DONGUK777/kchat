from kafka import KafkaProducer
import json
import time

pro = KafkaProducer(
        bootstrap_servers=['ec2-43-203-219-5.ap-northeast-2.compute.amazonaws.com:9092'],
        value_serializer=lambda x:json.dumps(x).encode('utf-8'),
)

print("채팅 프로그램 - 메시지 발신자")
print("메시지를 입력하세요. (종료 시 'exit' 입력)") 

while True:
    m = input("YOU: ")
    if m.lower() == 'exit':
        break

    data = m
    pro.send('chat-group', value=data)
    pro.flush()


print("채팅 종료")




