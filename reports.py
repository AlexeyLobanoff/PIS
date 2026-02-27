# -*- coding: utf-8 -*-
"""
Модуль отчётности: подробный текстовый отчет и экспорт в Excel (.xls) с диаграммой.
"""

import logging
from pathlib import Path
from typing import Any, List, Tuple
import pandas as pd
from openpyxl import load_workbook
from openpyxl.chart import PieChart, Reference

logger = logging.getLogger(__name__)


def generate_errors_report(
        filepath: str,
        errors: List[Tuple[int, str, str]],  # Ожидаем кортеж: (номер, причина, исходная_строка)
        encoding: str = "utf-8",
) -> None:
    """
    Генерирует аккуратный табличный отчет об ошибках парсинга.
    Выводит полную причину и исходное содержание строки.
    """
    path = Path(filepath)
    report_path = path.parent / "errors_report.txt"

    # Настройки ширины для красивого выравнивания
    w_ln = 10
    w_err = 85
    separator = "=" * 170

    try:
        with open(report_path, "w", encoding=encoding) as f:
            f.write("ОТЧЕТ ОБ ОШИБКАХ ПАРСИНГА\n")
            f.write(f"Файл: {path.name}\n")
            f.write(separator + "\n")
            # Заголовки таблицы
            f.write(f"{'СТРОКА'.ljust(w_ln)} | {'ПРИЧИНА ОШИБКИ'.ljust(w_err)} | ИСХОДНОЕ СОДЕРЖАНИЕ\n")
            f.write("-" * 170 + "\n")

            for err in errors:
                line_num = str(err[0]).ljust(w_ln)
                # Выводим причину полностью
                reason = str(err[1]).ljust(w_err)
                # Исходная строка из парсера (третий элемент кортежа)
                raw_line = str(err[2]).strip() if len(err) > 2 else "Данные не переданы"

                f.write(f"{line_num} | {reason} | {raw_line}\n")

            f.write(separator + "\n")
            f.write(f"ИТОГО КРИТИЧЕСКИХ ОШИБОК: {len(errors)}\n")

        logger.info("Табличный отчет об ошибках сохранен: %s", report_path)
    except Exception as e:
        logger.error("Не удалось сохранить текстовый отчет: %s", e)


def export_to_excel_with_chart(
        output_path: str,
        data: List[dict[str, Any]],
        stats: dict[str, Any],
) -> None:
    """
    Экспорт данных в формат .xls с созданием круговой диаграммы статистики.
    """
    if not data:
        logger.warning("Нет данных для экспорта.")
        return

    # Гарантируем расширение .xls (даже если пришло .xlsx)
    final_path = str(Path(output_path).with_suffix('.xls'))

    # Создаем DataFrame из данных
    df = pd.DataFrame(data)

    # Используем движок openpyxl (он создаст файл, который Excel откроет как .xls)
    with pd.ExcelWriter(final_path, engine="openpyxl") as writer:
        # Лист 1: Основные данные
        df.to_excel(writer, sheet_name="Данные", index=False)

        # Подготовка статистики
        processed = int(stats.get("processed", 0)) or len(data)
        success = int(stats.get("success", 0)) or len(data)
        errors_count = int(stats.get("errors", 0))

        # Лист 2: Статистика для диаграммы
        stats_df = pd.DataFrame({
            "Категория": ["Успешно", "Ошибки"],
            "Количество": [success, errors_count]
        })
        stats_df.to_excel(writer, sheet_name="Статистика", index=False)

    # Добавляем круговую диаграмму через openpyxl
    try:
        wb = load_workbook(final_path)
        ws_stats = wb["Статистика"]

        chart = PieChart()
        chart.title = "Статистика обработки данных"

        # Ссылка на данные (Кол-во: Успешно/Ошибки)
        data_ref = Reference(ws_stats, min_col=2, min_row=2, max_row=3)
        # Ссылка на категории (Названия)
        cats_ref = Reference(ws_stats, min_col=1, min_row=2, max_row=3)

        chart.add_data(data_ref, titles_from_data=False)
        chart.set_categories(cats_ref)

        # Размещаем диаграмму на листе статистики
        ws_stats.add_chart(chart, "E2")
        wb.save(final_path)
        logger.info("Excel-отчет (.xls) с диаграммой успешно сохранен.")
    except Exception as e:
        logger.error("Ошибка при добавлении диаграммы в Excel: %s", e)