from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers='localhost:9092')
future = producer.send('foobar', 'some_message_bytes')
result = future.get(timeout=60)
print(result)
