from confluent_kafka import Producer
import sys
import time

BROKER_LST = 'kafka01:9092,kafka02:9092,kafka03:9092'


class SimpleProducer:
    # 생성자
    def __init__(self, topic, duration=None):
        self.topic = topic
        self.duration = duration if duration is not None else 60
        # 이걸로 옵션을 지정한다. 키밸류 형태로 추가할 수 있다.
        self.conf = {'bootstrap.servers': BROKER_LST}
        # 프로듀서 프로그램의 시작점. self.conf는 딕셔너리이다. 이걸로 옵션을 지정한다. 키밸류 형태로 추가할 수 있다.
        self.producer = Producer(self.conf)

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(self, err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' %
                             (msg.topic(), msg.partition(), msg.offset()))

    def produce(self):
        cnt = 0
        while cnt < self.duration:
            try:
                # Produce line (without newline)
                self.producer.produce(
                    topic=self.topic,
                    key=str(cnt),
                    value=f'hello world: {cnt}',
                    on_delivery=self.delivery_callback)
                    # on_delivery는 비동기식 전송일 때 지정한다. 비동기식은 데이터를 보내고 ack 응답을 받지않고도 계속해서 보낼 수 있는 전송을 의미
                    # ack응답은 프로듀서의 어느 한 부분에 큐형태로 쌓이게 된다.
                    # on_delivery 파라미터에 ack 응답들의 후속처리를 해주는 로직이 담긴 함수가 온다

            # 프로듀서는 데이터를 사실 바로바로 전송하지 않고 어떤 메모리 공간에 메시지를 어느정도 쌓아두다가 전송한다. 즉 배치단위로 전송한다.
            # buffer error는 매번 이 공간이 꽉 차거나 문제가 있을 때 만나는 exception이다.
            except BufferError:
                sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                                 len(self.producer))

            # Serve delivery callback queue.
            # NOTE: Since produce() is an asynchronous API this poll() call
            #       will most likely not serve the delivery callback for the
            #       last produce()d message.
            # 프로듀서 내부 이벤트를 처리해주는 함수(에러콜백 / 전송 성공콜백 / 백그라운드 네트워크 I/O 처리 / 버퍼에 있는 메시지 전송하도록 트리거)
            # 메모리 공간을 비워주기 때문에 poll은 주기적으로 호출이 되어야 한다.
            self.producer.poll(0)
            cnt += 1
            time.sleep(1)  # 1초 대기

        # Wait until all messages have been delivered
        sys.stderr.write('%% Waiting for %d deliveries\n' % len(self.producer))
        # 프로그램 종료 전 queue에 데이터가 남아있다면 마저 전송시켜줌
        self.producer.flush()


if __name__ == '__main__':
    # duration은 몇 초 동안
    simple_producer = SimpleProducer(topic='lesson.ch5-1.simple.producer', duration=60)
    simple_producer.produce()