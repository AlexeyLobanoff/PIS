import tkinter as tk
from tkinter import filedialog, messagebox
from pathlib import Path

class FileLoaderApp:

    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Загрузка txt файла")
        self.root.geometry("400x200")

        self.selected_file_path = None

    # кнопка выбора файла
        self.btn_select_file = tk.Button(
        self.root,
        text="Выбрать txt файл",
        command=self.select_file
    )
        self.btn_select_file.pack(pady=20)

    # название файла
        self.file_label = tk.Label(self.root, text="Файл не выбран")
        self.file_label.pack(pady=10)

    # кнопка чтения
        self.btn_read_file = tk.Button(
        self.root,
        text="Прочитать файл",
        command=self.read_file
    )
        self.btn_read_file.pack(pady=10)


def select_file(self):
    
    path = filedialog.askopenfilename(
        filetypes=[("Text files", "*.txt"), ("All files", "*.*")]
    )

    if path:
        self.selected_file_path = path
        self.file_label.config(text=f"Выбран: {Path(path).name}")


def generate_errors_report(self, filepath, errors):
    

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


def read_file(self):


    if not self.selected_file_path:
        messagebox.showwarning("Внимание", "Сначала выберите файл")
        return

    errors = []
    success_count = 0

    try:

        with open(self.selected_file_path, "r", encoding="utf-8") as file:

            for line_number, line in enumerate(file, start=1):

                line = line.strip()

                if not line:
                    continue

                parts = line.split(";")

                # проверка минимального формата
                if len(parts) < 5:
                    errors.append((line_number, "Недостаточно полей", line))
                    continue

                success_count += 1
                print(f"Строка {line_number}: {line}")

        # создание отчета
        if errors:
            self.generate_errors_report(self.selected_file_path, errors)

            messagebox.showinfo(
                "Готово",
                f"Обработано строк: {success_count + len(errors)}\n"
                f"Ошибок: {len(errors)}\n"
                f"Создан файл errors_report.txt"
            )

        else:
            messagebox.showinfo("Готово", "Ошибок не найдено")

    except Exception as e:
        messagebox.showerror("Ошибка", str(e))


if __name__ == "__main__":
    app = FileLoaderApp()
    app.root.mainloop()
