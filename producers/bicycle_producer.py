import time
import json
import logging
from confluent_kafka import Producer
from apis.seoul_data.realtime_bicycle import RealtimeBicycle
from datetime import datetime


BROKER_LST = 'kafka01:9092,kafka02:9092,kafka03:9092'


class BicycleProducer():

    def __init__(self, topic):
        self.topic = topic
        self.conf = {'bootstrap.servers': BROKER_LST,
                     'compression.type':'lz4'}          # producer 압축옵션 지정 추가
        self.producer = Producer(self.conf)
        self._set_logger()

    def _set_logger(self):
        logging.basicConfig(
            format='%(asctime)s [%(levelname)s]:%(message)s',
            level=logging.INFO,
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self.log = logging.getLogger(__name__)

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(self, err, msg):
        if err:
            self.log.error('%% Message failed delivery: %s\n' % err)
        else:
            pass

    def produce(self):
        rt_bycicle = RealtimeBicycle(dataset_nm='bikeList')   # API 호출 클래스
        while True:
            now_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')       # now_dt 선. 스트링 데이터
            items = rt_bycicle.call()   # call : API 호출 메소드
            for item in items:          # items는 리스트, item은 딕셔너리 형태
                # 컬럼명 변경
                item['STT_ID'] = item.pop('stationId')
                item['STT_NM'] = item.pop('stationName')
                item['TOT_RACK_CNT'] = item.pop('rackTotCnt')
                item['TOT_PRK_CNT'] = item.pop('parkingBikeTotCnt')
                item['RATIO_PRK_RACK'] = item.pop('shared')
                item['STT_LTTD'] = item.pop('stationLatitude')
                item['STT_LGTD'] = item.pop('stationLongitude')


                # 컬럼 추가
                item['CRT_DTTM'] = now_dt   # now_dt는 스트링 데이터

                # produce
                # json.dumps는 딕셔너리 형태 데이터를 읽어 스트링의 json 형태로 변환하는 함수
                # on_delivery 파라미터가 있으니 이 프로듀서는 비동기식 프로듀서다.
                try:
                    self.producer.produce(
                        topic=self.topic,
                        key=json.dumps({'STT_ID': item['STT_ID'],'CRT_DTTM':item['CRT_DTTM']}, ensure_ascii=False),
                        value=json.dumps(item, ensure_ascii=False),
                        on_delivery=self.delivery_callback
                    )

                except BufferError:
                    self.log.error('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                                     len(self.producer))

            # Serve delivery callback queue.
            # NOTE: Since produce() is an asynchronous API this poll() call
            #       will most likely not serve the delivery callback for the
            #       last produce()d message.
            self.producer.poll(0)

            # Wait until all messages have been delivered
            self.log.info('%% Waiting for %d deliveries\n' % len(self.producer))
            self.producer.flush()

            # 15초 대기
            time.sleep(15)


if __name__ == '__main__':
    bicycle_producer = BicycleProducer(topic='apis.seouldata.rt-bicycle')
    bicycle_producer.produce()