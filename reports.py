# -*- coding: utf-8 -*-
"""
Модуль отчётности: errors_report.txt и экспорт в Excel с круговой диаграммой.
"""

import logging
from pathlib import Path
from typing import Any

import pandas as pd
from openpyxl import load_workbook
from openpyxl.chart import PieChart, Reference

logger = logging.getLogger(__name__)


def generate_errors_report(
    filepath: str,
    errors: list[tuple[int, str]],
    encoding: str = "utf-8",
) -> None:
    """
    Генерирует errors_report.txt с указанием причины ошибки для каждой плохой строки.
    Формат: номер_строки: причина
    """
    path = Path(filepath)
    report_path = path.parent / "errors_report.txt"
    try:
        with open(report_path, "w", encoding=encoding) as f:
            f.write("Отчёт об ошибках парсинга\n")
            f.write("=" * 60 + "\n\n")
            for line_num, reason in errors:
                f.write(f"Строка {line_num}: {reason}\n")
        logger.info("Отчёт об ошибках сохранён: %s", report_path)
    except OSError as e:
        logger.exception("Ошибка записи errors_report.txt: %s", e)
        raise


def export_to_excel_with_chart(
    output_path: str,
    documents: list[dict[str, Any]],
    stats: dict[str, int | float],
    encoding: str = "utf-8",
) -> None:
    """
    Экспортирует корректные данные из MongoDB в Excel и строит круговую диаграмму
    (статистика обработки: успешно / ошибки в %).
    stats: {"processed": N, "success": M, "errors": K} или с процентами.
    """
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    # Разворачиваем documents в плоскую таблицу: по одной строке на каждую entry
    rows_data = []
    for doc in documents:
        base = {
            "account": doc.get("account"),
            "full_name": doc.get("full_name"),
            "address": doc.get("address"),
            "period": doc.get("period"),
        }
        entries = doc.get("entries") or []
        for ent in entries:
            row = {
                **base,
                "amount": ent.get("amount"),
                "device": ent.get("device"),
                "reading": ent.get("reading"),
            }
            rows_data.append(row)

    df = pd.DataFrame(rows_data)

    # Если данных нет — создаём пустой DataFrame с колонками
    if df.empty:
        df = pd.DataFrame(columns=["account", "full_name", "address", "period", "amount", "device", "reading"])

    # Записываем лист с данными через pandas + openpyxl (для диаграммы нужен openpyxl)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Данные", index=False)

        # Лист со статистикой для диаграммы
        processed = int(stats.get("processed", 0)) or 1
        success = int(stats.get("success", 0))
        errors_count = int(stats.get("errors", 0))
        pct_ok = (success / processed * 100) if processed else 0
        pct_err = (errors_count / processed * 100) if processed else 0

        stats_df = pd.DataFrame({
            "Категория": ["Успешно", "Ошибки"],
            "Количество": [success, errors_count],
            "Процент": [round(pct_ok, 1), round(pct_err, 1)],
        })
        stats_df.to_excel(writer, sheet_name="Статистика", index=False)

    # Добавляем круговую диаграмму на лист "Статистика"
    try:
        wb = load_workbook(output_path)
        ws_stats = wb["Статистика"]
        chart = PieChart()
        chart.title = "Статистика обработки"
        data = Reference(ws_stats, min_col=2, min_row=2, max_row=3)  # Количество: успешно, ошибки
        cats = Reference(ws_stats, min_col=1, min_row=2, max_row=3)
        chart.add_data(data, titles_from_data=True)
        chart.set_categories(cats)
        chart.style = 10
        ws_stats.add_chart(chart, "E2")
        wb.save(output_path)
    except Exception as e:
        logger.warning("Не удалось добавить круговую диаграмму: %s", e)

    logger.info("Excel-отчёт сохранён: %s", output_path)