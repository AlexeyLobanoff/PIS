# -*- coding: utf-8 -*-
"""
Модуль работы с MongoDB: подключение, вставка документов.
"""

import logging
from typing import Any, Callable, Optional

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure

from parser import ParsedRow

logger = logging.getLogger(__name__)


class MongoManager:
    """
    Подключение к удалённому MongoDB по URI и сохранение данных.
    Формат документа: фиксированные поля + массив entries для вариативной части.
    """

    def __init__(
        self,
        uri: str,
        database: str = "etl_db",
        collection: str = "records",
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
        """
        Устанавливает соединение с MongoDB.
        Обрабатывает pymongo.errors.ConnectionFailure.
        Возвращает True при успехе, False при ошибке.
        """
        try:
            self._client = MongoClient(self.uri, serverSelectionTimeoutMS=5000)
            self._client.admin.command("ping")
            self._db = self._client[self.database_name]
            self._coll = self._db[self.collection_name]
            self._log("Подключение к MongoDB успешно")
            return True
        except ConnectionFailure as e:
            err_msg = f"Ошибка подключения к MongoDB (удалённый сервер): {e}"
            self._log(err_msg)
            logger.exception(err_msg)
            return False
        except Exception as e:
            err_msg = f"Ошибка при подключении к MongoDB: {e}"
            self._log(err_msg)
            logger.exception(err_msg)
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

    def _row_to_document(self, row: ParsedRow) -> dict:
        """Преобразует ParsedRow в документ для MongoDB."""
        return {
            "account": row.account,
            "full_name": row.full_name,
            "address": row.address,
            "period": row.period,
            "entries": row.entries,
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
        batch_size: int = 5000,
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
                chunk = rows[start : start + batch_size]
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

    def get_all_documents(self) -> list[dict[str, Any]]:
        """
        Возвращает все документы из коллекции для экспорта в отчёт.
        При ошибке возвращает пустой список.
        """
        if self._client is None:
            self._log("Нет подключения к MongoDB")
            return []
        try:
            cursor = self._coll.find({}, {"_id": 0})
            return list(cursor)
        except Exception as e:
            err_msg = f"Ошибка чтения из MongoDB: {e}"
            self._log(err_msg)
            logger.exception(err_msg)
            return []