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
import pandas as pd
from typing import List, Tuple, Any
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
    Экспорт данных в формат .xlsx с созданием круговой диаграммы статистики.
    """
    if not data:
        logger.warning("Нет данных для экспорта.")
        return

    # Гарантируем расширение .xlsx
    final_path = str(Path(output_path).with_suffix('.xlsx'))

    # Создаем DataFrame из данных
    df = pd.DataFrame(data)

    # Используем движок openpyxl
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
        logger.info("Excel-отчет (.xlsx) с диаграммой успешно сохранен.")
    except Exception as e:
        logger.error("Ошибка при добавлении диаграммы в Excel: %s", e)


def generate_html_errors_report(filepath: str, errors: List[Tuple]) -> None:
    """
    Генерирует HTML-отчет с интерактивной сортировкой.
    """
    path = Path(filepath)
    # Если filepath - это путь к отчету, сохраняем там.
    # Если это путь к исходному файлу, создаем рядом .html
    report_path = path.with_suffix('.errors.html')

    try:
        with open(report_path, "w", encoding="utf-8") as f:
            f.write("<!DOCTYPE html>\n<html>\n<head>\n<meta charset='utf-8'>\n")
            f.write("<title>Отчет об ошибках</title>\n")
            f.write("<style>\n")
            f.write("body { font-family: sans-serif; margin: 20px; background-color: #f8f9fa; }\n")
            f.write("h1 { color: #dc3545; }\n")
            f.write(
                "table { border-collapse: collapse; width: 100%; background: white; box-shadow: 0 1px 3px rgba(0,0,0,0.2); }\n")
            f.write("th, td { border: 1px solid #dee2e6; padding: 12px; text-align: left; }\n")
            f.write("th { background-color: #e9ecef; cursor: pointer; position: sticky; top: 0; }\n")
            f.write("th:hover { background-color: #dee2e6; }\n")
            f.write("tr:nth-child(even) { background-color: #f2f2f2; }\n")
            f.write(".error-msg { color: #d63384; font-weight: bold; }\n")
            f.write("</style>\n</head>\n<body>\n")

            f.write(f"<h1>Отчет об ошибках парсинга</h1>\n")
            f.write(f"<p><strong>Файл:</strong> {path.name}</p>\n")
            f.write(f"<p><strong>Всего ошибок:</strong> {len(errors)}</p>\n")
            f.write("<p><small><i>Подсказка: нажмите на заголовок столбца для сортировки</i></small></p>\n")

            f.write("<table id='errorTable'>\n")
            f.write("<thead><tr>")
            f.write("<th onclick='sortTable(0)'>№ Строки</th>")
            f.write("<th onclick='sortTable(1)'>Причина ошибки</th>")
            f.write("<th onclick='sortTable(2)'>Исходные данные</th>")
            f.write("</tr></thead>\n<tbody>\n")

            for err in errors:
                line_num = err[0]
                reason = err[1]
                # Берем исходную строку (она обычно 3-м элементом в кортеже ошибок)
                raw_data = err[2] if len(err) > 2 else "Нет данных"

                f.write(f"<tr><td>{line_num}</td>")
                f.write(f"<td class='error-msg'>{reason}</td>")
                f.write(f"<td><code>{raw_data}</code></td></tr>\n")

            f.write("</tbody></table>\n")

            # Скрипт для сортировки
            f.write("""
            <script>
            function sortTable(n) {
                var table, rows, switching, i, x, y, shouldSwitch, dir, switchcount = 0;
                table = document.getElementById("errorTable");
                switching = true;
                dir = "asc";
                while (switching) {
                    switching = false;
                    rows = table.rows;
                    for (i = 1; i < (rows.length - 1); i++) {
                        shouldSwitch = false;
                        x = rows[i].getElementsByTagName("TD")[n];
                        y = rows[i + 1].getElementsByTagName("TD")[n];

                        var xVal = x.innerHTML.toLowerCase();
                        var yVal = y.innerHTML.toLowerCase();

                        // Если сортируем первый столбец (числа), преобразуем в число
                        if (n === 0) {
                            xVal = parseInt(xVal) || 0;
                            yVal = parseInt(yVal) || 0;
                        }

                        if (dir == "asc") {
                            if (xVal > yVal) { shouldSwitch = true; break; }
                        } else if (dir == "desc") {
                            if (xVal < yVal) { shouldSwitch = true; break; }
                        }
                    }
                    if (shouldSwitch) {
                        rows[i].parentNode.insertBefore(rows[i + 1], rows[i]);
                        switching = true;
                        switchcount ++;
                    } else {
                        if (switchcount == 0 && dir == "asc") {
                            dir = "desc";
                            switching = true;
                        }
                    }
                }
            }
            </script>
            """)
            f.write("</body>\n</html>\n")

        logger.info(f"HTML отчет успешно создан: {report_path}")
    except Exception as e:
        logger.error(f"Не удалось создать HTML отчет: {e}")

def export_to_excel_combined(filepath: str, success_data: List[Any], errors_data: List[Tuple[int, str, str]]) -> None:
    """
    Экспортирует успешные данные и ошибки в один Excel файл на разные листы.
    """
    # 1. Подготавливаем успешные данные
    success_list = []
    for row in success_data:
        # Поддержка и объектов ParsedRow, и словарей (на всякий случай)
        is_dict = isinstance(row, dict)
        acc = row.get("Лицевой счет") if is_dict else getattr(row, 'account', '')
        fio = row.get("ФИО") if is_dict else getattr(row, 'full_name', '')
        addr = row.get("Адрес") if is_dict else getattr(row, 'address', '')
        period = row.get("Период") if is_dict else getattr(row, 'period_display', '')
        total = row.get("Общая сумма") if is_dict else getattr(row, 'total_amount', 0.0)
        entries = row.get("Услуги") if is_dict else getattr(row, 'entries', [])

        # Форматируем услуги
        services_str = ", ".join(
            [f"{item.get('Счёт и услуга', '')}: {item.get('Сумма', 0)}" for item in entries]) if entries else "-"

        success_list.append({
            "Лицевой счет": acc,
            "ФИО": fio,
            "Адрес": addr,
            "Период": period,
            "Общая сумма": total,
            "Услуги": services_str
        })

    df_success = pd.DataFrame(success_list)

    # 2. Подготавливаем данные с ошибками
    errors_list = []
    for err in errors_data:
        errors_list.append({
            "Строка в файле": err[0],
            "Причина ошибки": err[1],
            "Исходный текст строки": err[2] if len(err) > 2 else ""
        })

    df_errors = pd.DataFrame(errors_list)

    # 3. Записываем оба DataFrame в один Excel-файл на разные листы
    with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
        if not df_success.empty:
            df_success.to_excel(writer, sheet_name='Успешные данные', index=False)
        else:

            pd.DataFrame([{"Сообщение": "Успешных записей нет"}]).to_excel(writer, sheet_name='Успешные данные',
                                                                           index=False)

        if not df_errors.empty:
            df_errors.to_excel(writer, sheet_name='Ошибки парсинга', index=False)
        else:
            pd.DataFrame([{"Сообщение": "Ошибок не найдено (Всё идеально!)"}]).to_excel(writer,
                                                                                        sheet_name='Ошибки парсинга',
                                                                                        index=False)

def export_to_csv(
        output_path: str,
        data: List[dict[str, Any]],
) -> None:
    """
    Экспорт данных в формат CSV.
    """
    if not data:
        logger.warning("Нет данных для экспорта.")
        return

    # Гарантируем расширение .csv
    final_path = str(Path(output_path).with_suffix('.csv'))

    # Создаем DataFrame из данных
    df = pd.DataFrame(data)

    # Экспорт в CSV
    df.to_csv(final_path, index=False, encoding='utf-8-sig')

    logger.info("CSV-отчет успешно сохранен: %s", final_path)