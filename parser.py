# -*- coding: utf-8 -*-
"""
Модуль парсинга и валидации текстовых данных.
Формат: разделитель ";", поля 1-4 фиксированные, поле 5 вариативное.
Сценарий А: 1 поле (Сумма). Сценарий Б: группы по 3 (Сумма, Прибор, Показание).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional


@dataclass
class ParsedRow:
    """Результат успешного разбора строки."""
    account: int
    full_name: str
    address: str
    period: int
    entries: list  # list of dicts: {"amount", "device", "reading"} (device/reading могут быть None)
    raw_line: str = ""


def _normalize_number(s: str) -> str:
    """Замена запятой на точку в числовых значениях."""
    if not s:
        return s
    return s.strip().replace(",", ".")


def _parse_int(s: str) -> Optional[int]:
    """Парсинг int."""
    s = s.strip()
    try:
        return int(s)
    except ValueError:
        return None


def _parse_float(s: str) -> Optional[float]:
    """Парсинг float с заменой запятой на точку."""
    s = _normalize_number(s)
    try:
        return float(s)
    except ValueError:
        return None


def validate_address(address: str) -> bool:
    """
    Адрес должен содержать минимум 3 сегмента (город/населённый пункт, улица, дом),
    разделённые запятой.
    """
    if not address or not address.strip():
        return False
    segments = [seg.strip() for seg in address.split(",") if seg.strip()]
    return len(segments) >= 3


def parse_variable_part(variable_fields: list) -> Optional[list]:
    """
    Парсинг вариативной части.
    Сценарий А: ровно 1 поле — Сумма.
    Сценарий Б: количество полей кратно 3 — группы (Сумма, Название прибора, Показание).
    Возвращает список записей или None при невалидном количестве полей.
    """
    if not variable_fields:
        return None
    n = len(variable_fields)
    if n == 1:
        amount = _parse_float(variable_fields[0])
        if amount is None:
            return None
        return [{"amount": amount, "device": None, "reading": None}]
    if n % 3 != 0:
        return None
    entries = []
    for i in range(0, n, 3):
        amount = _parse_float(variable_fields[i])
        device = variable_fields[i + 1].strip() if i + 1 < n else ""
        reading = _parse_float(variable_fields[i + 2])
        if amount is None or reading is None:
            return None
        entries.append({"amount": amount, "device": device, "reading": reading})
    return entries


class DataProcessor:
    """
    Построчное чтение файла и разбор строк в заданном формате.
    Поддерживает кодировки utf-8 и cp1251.
    """

    def __init__(self, log_callback: Optional[Callable[[str], None]] = None):
        self.log_callback = log_callback or (lambda msg: None)

    def _log(self, msg: str) -> None:
        self.log_callback(msg)

    def process_line(self, line: str, line_num: int) -> tuple[ParsedRow | None, str | None]:
        """
        Обрабатывает одну строку.
        Возвращает (ParsedRow, None) при успехе или (None, "причина ошибки") при ошибке.
        """
        line = line.strip()
        if not line:
            return None, "Пустая строка"

        parts = line.split(";")
        if parts and parts[-1] == "":
            parts = parts[:-1]

        if len(parts) < 5:
            return None, f"Недостаточно полей (ожидается минимум 5, получено {len(parts)})"

        account_str = parts[0].strip()
        full_name = parts[1].strip()
        address = parts[2].strip()
        period_str = parts[3].strip()
        variable_fields = [p.strip() for p in parts[4:] if p is not None]

        account = _parse_int(account_str)
        if account is None:
            return None, f"Лицевой счёт должен быть целым числом: '{account_str}'"

        if not full_name:
            return None, "ФИО не может быть пустым"

        if not validate_address(address):
            return None, (
                f"Адрес должен содержать минимум 3 сегмента (населённый пункт, улица, дом): "
                f"'{address[:50]}{'...' if len(address) > 50 else ''}'"
            )

        period = _parse_int(period_str)
        if period is None:
            return None, f"Период должен быть целым числом: '{period_str}'"

        entries = parse_variable_part(variable_fields)
        if entries is None:
            n = len(variable_fields)
            return None, (
                f"Вариативная часть: ожидается 1 поле (сумма) или число полей кратное 3 "
                f"(сумма, прибор, показание), получено полей: {n}"
            )

        return (
            ParsedRow(
                account=account,
                full_name=full_name,
                address=address,
                period=period,
                entries=entries,
                raw_line=line,
            ),
            None,
        )

    def process_file(
        self,
        filepath: str,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> tuple[list, list]:
        """
        Читает файл построчно, кодировки utf-8 и cp1251.
        Возвращает (список успешных ParsedRow, список (номер_строки, причина_ошибки, исходная_строка)).
        """
        successful = []
        errors = []
        encodings = ["utf-8", "cp1251"]
        lines = []
        used_encoding = None

        for encoding in encodings:
            try:
                with open(filepath, "r", encoding=encoding) as f:
                    lines = f.readlines()
                used_encoding = encoding
                self._log(f"Файл прочитан в кодировке {encoding}")
                break
            except UnicodeDecodeError:
                continue
            except OSError as e:
                self._log(f"Ошибка чтения файла: {e}")
                return [], [(0, str(e))]

            self._log("Не удалось прочитать файл (поддерживаются utf-8, cp1251)")
            return [], [(0, "Ошибка кодировки файла")]

        total = len(lines)
        for i, line in enumerate(lines, start=1):
            row, err = self.process_line(line, i)
            if err:
                errors.append((i, err, line.strip()))
            else:
                successful.append(row)
            if progress_callback and total and (i % 100 == 0 or i == total):
                progress_callback(i, total)

        return successful, errors