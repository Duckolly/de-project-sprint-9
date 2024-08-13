import logging
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask
from app_config import AppConfig
from stg_loader.stg_message_processor_job import StgMessageProcessor
from dotenv import load_dotenv

# TODO: Проверить, кажется нафиг не нужно
load_dotenv()

app = Flask(__name__)


@app.get('/ping')
def health():
    return 'pong'


if __name__ == '__main__':
    app.logger.setLevel(logging.DEBUG) # TODO: Заменить уровень логгирования

    config = AppConfig()
    proc = StgMessageProcessor(
        consumer=config.kafka_consumer(),
        producer=config.kafka_producer(),
        redis_client=config.redis_client(),
        pg_connect=config.pg_warehouse_db(),
        batch_size=config.batch_size,
        logger=app.logger
    )

    scheduler = BackgroundScheduler()
    scheduler.add_job(
        func=proc.run,
        trigger="interval",
        seconds=config.DEFAULT_JOB_INTERVAL
    )
    scheduler.start()

    app.run(
        debug=True,
        host='0.0.0.0',
        use_reloader=False
    )
