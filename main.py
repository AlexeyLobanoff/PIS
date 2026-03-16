import tkinter as tk
from tkinter import filedialog, messagebox
from pathlib import Path
from parser import DataProcessor
from reports import generate_errors_report


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

    def read_file(self):

        if not self.selected_file_path:
            messagebox.showwarning("Внимание", "Сначала выберите файл")
            return

        try:

            processor = DataProcessor()

            success_rows, errors = processor.process_file(self.selected_file_path)

            if errors:

                generate_errors_report(self.selected_file_path, errors)

                messagebox.showinfo(
                    "Готово",
                    f"Обработано строк: {len(success_rows) + len(errors)}\n"
                    f"Ошибок: {len(errors)}\n"
                    f"Создан файл errors_report.txt"
                )

            else:

                messagebox.showinfo(
                    "Готово",
                    f"Обработано строк: {len(success_rows)}\n"
                    f"Ошибок не найдено"
                )

        except Exception as e:
            messagebox.showerror("Ошибка", str(e))


if __name__ == "__main__":
    app = FileLoaderApp()
    app.root.mainloop()