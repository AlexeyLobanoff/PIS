# -*- coding: utf-8 -*-
"""
Модуль работы с MongoDB: подключение, вставка документов.
"""

import logging
import os
from typing import Any, Callable, Optional

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
from datetime import datetime
from parser import ParsedRow

load_dotenv()

logger = logging.getLogger(__name__)

_TIMEOUT_MS = int(os.getenv("MONGO_TIMEOUT_MS", "5000"))
_INSERT_BATCH_SIZE = int(os.getenv("DB_INSERT_BATCH_SIZE", "5000"))
_QUERY_BATCH_SIZE = int(os.getenv("DB_QUERY_BATCH_SIZE", "1000"))


class MongoManager:
    """
    Подключение к удалённому MongoDB по URI и сохранение данных.
    Формат документа: фиксированные поля + массив entries для вариативной части.
    """

    def __init__(
            self,
            uri: str,
            database: str = os.getenv("MONGO_DATABASE", "etl_db"),
            collection: str = os.getenv("MONGO_COLLECTION", "records"),
            log_callback: Optional[Callable[[str], None]] = None,
    ):
        self.uri = uri
        self.database_name = database
        self.collection_name = collection
        self.log_callback = log_callback or (lambda msg: None)
        self._client: Optional[MongoClient] = None
        self._db = None
        self._coll = None

    def _log(self, msg: str) -> None:
        self.log_callback(msg)
        logger.info(msg)

    def connect(self) -> bool:
        try:
            self._client = MongoClient(self.uri, serverSelectionTimeoutMS=_TIMEOUT_MS)
            self._client.admin.command("ping")
            self._db = self._client[self.database_name]
            self._coll = self._db[self.collection_name]

            # СОЗДАЕМ УНИКАЛЬНЫЙ ИНДЕКС (Л/С + Период)
            # Это гарантирует, что пара этих полей будет уникальной
            self._coll.create_index(
                [("Лицевой счет", 1), ("Период", 1)],
                unique=True
            )

            self._log("Подключено к MongoDB. Уникальный индекс проверен.")
            return True
        except Exception as e:
            self._log(f"Ошибка подключения: {e}")
            return False

    def disconnect(self) -> None:
        """Закрывает соединение."""
        if self._client is not None:
            try:
                self._client.close()
                self._log("Соединение с MongoDB закрыто")
            except Exception as e:
                logger.warning("Ошибка при закрытии соединения: %s", e)
            self._client = None
            self._db = None
            self._coll = None

    def is_connected(self) -> bool:
        """Проверяет, установлено ли соединение."""
        if self._client is None:
            return False
        try:
            self._client.admin.command("ping")
            return True
        except Exception:
            return False

    def _row_to_document(self, row):
        """Преобразует объект ParsedRow в словарь с русскими ключами для MongoDB"""
        return {
            "Лицевой счет": row.account,
            "ФИО": row.full_name,
            "Адрес": row.address,
            "Период": row.period_display,  # Наше красивое "19 Мая"
            "Сортировка_периода": row.period_sort,  # Оставляем для технических нужд (сортировки)
            "Общая сумма": row.total_amount,
            "Услуги": row.entries,  # Внутри уже русский формат
            "Дата загрузки": datetime.now()
        }

    def insert_one(self, row: ParsedRow) -> bool:
        """
        Вставляет один документ в коллекцию.
        Возвращает True при успехе, False при ошибке.
        OperationFailure (в т.ч. аутентификация) пробрасывается выше.
        """
        if self._client is None:
            self._log("Нет подключения к MongoDB")
            return False
        try:
            doc = self._row_to_document(row)
            self._coll.insert_one(doc)
            return True
        except OperationFailure:
            raise
        except Exception as e:
            err_msg = f"Ошибка вставки в MongoDB: {e}"
            self._log(err_msg)
            logger.exception(err_msg)
            return False

    def insert_many(
            self,
            rows: list,
            progress_callback: Optional[Callable[[int, int], None]] = None,
            batch_size: int = _INSERT_BATCH_SIZE,
    ) -> tuple[int, int]:
        """
        Вставляет список ParsedRow в коллекцию батчами (insert_many).
        Возвращает (успешно_вставлено, всего).
        """
        if self._client is None:
            self._log("Нет подключения к MongoDB")
            return 0, len(rows)
        total = len(rows)
        inserted = 0
        try:
            for start in range(0, total, batch_size):
                chunk = rows[start: start + batch_size]
                docs = [self._row_to_document(row) for row in chunk]
                self._coll.insert_many(docs)
                inserted += len(docs)
                if progress_callback and total:
                    progress_callback(min(start + len(docs), total), total)
        except OperationFailure as e:
            if getattr(e, "code", None) == 13:
                self._log(
                    "Ошибка аутентификации MongoDB. Укажите в URI логин и пароль, "
                    "например: mongodb://user:password@host:27017/"
                )
            else:
                self._log(f"Ошибка MongoDB: {e}")
            logger.exception("insert_many")
        return inserted, total

    def get_all_documents(self, progress_callback: Optional[Callable[[int, int], None]] = None) -> list[dict[str, Any]]:
        """
        Возвращает все документы из коллекции для экспорта в отчёт.
        При ошибке возвращает пустой список.
        """
        if self._client is None:
            self._log("Нет подключения к MongoDB")
            return []
        try:
            total = self._coll.count_documents({})
            cursor = self._coll.find({}, {"_id": 0}).batch_size(_QUERY_BATCH_SIZE)
            docs = []
            current = 0
            for doc in cursor:
                docs.append(doc)
                current += 1
                if progress_callback and total > 0 and (current % _QUERY_BATCH_SIZE == 0 or current == total):
                    progress_callback(current, total)
            return docs
        except Exception as e:
            err_msg = f"Ошибка чтения из MongoDB: {e}"
            self._log(err_msg)
            logger.exception(err_msg)
            return []

    def clear_collection(self) -> bool:
        """
        Очищает всю коллекцию, удаляя все документы.
        Возвращает True при успехе, False при ошибке.
        """
        if self._client is None:
            self._log("Нет подключения к MongoDB")
            return False
        try:
            result = self._coll.delete_many({})
            deleted_count = result.deleted_count
            self._log(f"Удалено {deleted_count} документов из коллекции")
            return True
        except Exception as e:
            err_msg = f"Ошибка при очистке коллекции: {e}"
            self._log(err_msg)
            logger.exception(err_msg)
            return False
