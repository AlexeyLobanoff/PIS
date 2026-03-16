from pathlib import Path

def generate_errors_report(filepath, errors):
    report_path = Path(filepath).parent / "errors_report.txt"

    with open(report_path, "w", encoding="utf-8") as f:

        f.write("ОТЧЕТ ОБ ОШИБКАХ\n")
        f.write("=" * 60 + "\n")

    for err in errors:
        line_num, reason, raw_line = err

        f.write(f"Строка: {line_num}\n")
        f.write(f"Причина: {reason}\n")
        f.write(f"Данные: {raw_line}\n")
        f.write("-" * 60 + "\n")

    f.write(f"\nВсего ошибок: {len(errors)}\n")

