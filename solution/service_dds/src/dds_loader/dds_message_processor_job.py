from datetime import datetime
from logging import Logger
from dds_loader.repository.dds_repository import OrderDdsBuilder
from dds_loader.repository.dds_repository import DdsRepository
from lib.kafka_connect.kafka_connectors import KafkaConsumer
from lib.kafka_connect.kafka_connectors import KafkaProducer
from lib.pg import PgConnect

# Initialize the DdsMessageProcessor with Kafka consumer, producer, PostgreSQL connection, batch size and logger.
class DdsMessageProcessor:
    def __init__(self, consumer: KafkaConsumer, producer: KafkaProducer, pg_connect: PgConnect, batch_size: int, logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._pg_connect = pg_connect
        self._batch_size = batch_size
        self._logger = logger
        
# Process messages from Kafka and write them to the DDS layer.
    def run(self) -> None:
        start_message = f"{datetime.utcnow()}: START"
        self._logger.info(start_message)
        dds_repository = DdsRepository(self._pg_connect)
        for _ in range(self._batch_size):
            msg = self._consumer.consume() # Получение сообщения из Kafka
            if msg is None:
                continue # Пропуск итерации, если сообщение отсутствует

            dds_builder = OrderDdsBuilder(msg["payload"]) # Создание DDS сообщения
            products = msg["payload"]["products"] # Извлечение продуктов из сообщения
            categories = set(p["category"] for p in products)
            
            self._logger.info(f"\n****** ЗАПОЛНЕНИЕ ХАБОВ В УРОВНЕ DDS ******\n")
            dds_repository.h_user_insert(dds_builder.h_user())
            dds_repository.h_product_insert(dds_builder.h_product())
            dds_repository.h_restaurant_insert(dds_builder.h_restaurant())
            dds_repository.h_order_insert(dds_builder.h_order())
            dds_repository.h_category_insert(dds_builder.h_category())

            self._logger.info(f"\n****** ЗАПОЛНЕНИЕ СПУТНИКОВ В УРОВНЕ DDS ******\n")
            dds_repository.s_user_names_insert(dds_builder.s_user_names())
            dds_repository.s_product_names_insert(dds_builder.s_product_names())
            dds_repository.s_restaurant_names_insert(dds_builder.s_restaurant_names())
            dds_repository.s_order_cost_insert(dds_builder.s_order_cost())
            dds_repository.s_order_status_insert(dds_builder.s_order_status())

            self._logger.info(f"\n****** ЗАПОЛНЕНИЕ ССЫЛОК В УРОВНЕ DDS ******\n")
            dds_repository.l_order_user_insert(dds_builder.l_order_user())
            for product in products:
                if product["category"] in categories:
                    dds_repository.l_product_restaurant_insert(dds_builder.l_product_restaurant())
                    dds_repository.l_order_product_insert(dds_builder.l_order_product())
                    dds_repository.l_product_category_insert(dds_builder.l_product_category())
            self._logger.info(f"\n****** Сообщение записано в базу данных ******\n")

            dest_msg_product = dds_builder.cdm_product_msg()
            self._logger.info(f"\n******\n Сообщение о продуктах со структурой: \n{dest_msg_product}\n******\n")
            dest_msg_category = dds_builder.cdm_category_msg()
            self._logger.info(f"\n******\n Сообщение о категириях со структурой: \n{dest_msg_category}\n******\n")
            self._producer.produce(dest_msg_product + dest_msg_category)
            self._logger.info(f"\n****** СООБЩЕНИЯ ЗАПИСАНЫ ******\n")

        self._logger.info(f"{datetime.utcnow()}: FINISH")

    def construct_output_message(self, original_message: dict, category_mappings: dict) -> dict:
        products = {
                    p_id: {
                            **p, "h_category_pk": category_mappings.get(p["category"], '')} 
                            for p_id, p in original_message["payload"]["products"].items()}
        return {
            "user_id": original_message["payload"]["user"]["user_id"],
            "products": products
        }