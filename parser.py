# -*- coding: utf-8 -*-
import re
from dataclasses import dataclass
from typing import Callable, Optional, Tuple, List, Dict, Any


@dataclass
class ParsedRow:
    """Результат разбора строки: период теперь как День и Месяц."""
    account: int
    full_name: str
    address: str
    flats: str
    period_raw: int
    period_display: str
    period_sort: str
    total_amount: float
    entries: List[Dict[str, Any]]
    raw_line: str = ""


def _format_period_as_date(period_int: int) -> Tuple[str, str]:
    """
    Преобразует 519 в ('05-19', '19 Мая').
    """
    months = [
        "", "Января", "Февраля", "Марта", "Апреля", "Мая", "Июня",
        "Июля", "Августа", "Сентября", "Октября", "Ноября", "Декабря"
    ]

    try:
        s = str(period_int)

        if len(s) < 2:
            return "00-00", "Ошибка"

        day = int(s[-2:])
        month_num = int(s[:-2]) if len(s) > 2 else 0

        if month_num < 1 or month_num > 12:
            return f"00-{day:02d}", f"{day} (Месяц?)"

        month_name = months[month_num]

        display = f"{day} {month_name}"
        sort_key = f"{month_num:02d}-{day:02d}"

        return sort_key, display

    except Exception:
        return "00-00", "Ошибка формата"


def _normalize_number(s: str) -> str:
    if not s:
        return s
    return s.strip().replace(",", ".")


def _parse_float(s: str) -> Optional[float]:
    s = _normalize_number(s)
    try:
        return float(s)
    except Exception:
        return None


class DataProcessor:

    def __init__(self, log_callback: Optional[Callable[[str], None]] = None):
        self.log_callback = log_callback or (lambda msg: None)

    def process_line(self, line: str, line_num: int) -> Tuple[Optional[ParsedRow], Optional[str]]:

        raw_line = line.strip()

        if not raw_line:
            return None, None

        # Разбиваем строку
        parts = [p.strip() for p in raw_line.split(";")]

        if parts and not parts[-1]:
            parts = parts[:-1]

        if len(parts) < 5:
            return None, f"Недостаточно полей: {len(parts)}"

        try:

            # 1. Лицевой счет
            acc = int(parts[0])

            # 2. ФИО
            name = parts[1]

            # 3. Адрес
            addr = parts[2]

            addr_parts = [a.strip() for a in addr.split(",")]

            # удаляем звездочки
            addr_parts = [p.replace("*", "").strip() for p in addr_parts]

            # удаляем пустые
            addr_parts = [p for p in addr_parts if p]

            flats = ""

            if len(addr_parts) > 3:
                base_address = ", ".join(addr_parts[:3])
                flats = ",".join(addr_parts[3:])
            else:
                base_address = ", ".join(addr_parts)

            if flats == "0":
                flats = ""

            # 4. Период
            period_match = re.search(r'\d+', parts[3])

            if not period_match:
                return None, f"Ошибка периода: {parts[3]}"

            period_val = int(period_match.group())

            sort_key, human_date = _format_period_as_date(period_val)

            # 5. Общая сумма
            total = _parse_float(parts[4])

            if total is None:
                return None, f"Ошибка в сумме: {parts[4]}"

            # 6. Услуги
            services = []
            pairs_part = parts[5:]

            if len(pairs_part) % 2 != 0:

                if pairs_part and not pairs_part[-1]:
                    pairs_part = pairs_part[:-1]
                else:
                    return None, f"Непарные поля услуг (всего {len(pairs_part)})"

            for i in range(0, len(pairs_part), 2):

                srv_name = pairs_part[i]
                srv_sum = _parse_float(pairs_part[i + 1])

                if srv_name and srv_sum is not None:

                    services.append({
                        "Счёт и услуга": srv_name,
                        "Сумма": srv_sum
                    })

            return ParsedRow(
                account=acc,
                full_name=name,
                address=base_address,
                flats=flats,
                period_raw=period_val,
                period_display=human_date,
                period_sort=sort_key,
                total_amount=total,
                entries=services,
                raw_line=raw_line
            ), None

        except Exception as e:
            return None, f"Ошибка парсинга: {str(e)}"

    def process_file(self, filepath: str, progress_callback=None) -> Tuple[List[ParsedRow], List[Tuple]]:

        successful = []
        errors = []
        lines = []

        for enc in ["utf-8", "cp1251", "utf-8-sig"]:

            try:

                with open(filepath, "r", encoding=enc) as f:
                    lines = f.readlines()

                break

            except Exception:
                continue

        if not lines:
            return [], [(0, "Файл пуст или не читается", "")]

        total = len(lines)

        for i, line in enumerate(lines, start=1):

            row, err = self.process_line(line, i)

            if err:
                errors.append((i, err, line.strip()))

            elif row:
                successful.append(row)

            if progress_callback and i % 100 == 0:
                progress_callback(i, total)

        return successful, errors