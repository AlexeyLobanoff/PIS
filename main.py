import tkinter as tk
from tkinter import filedialog, messagebox
from pathlib import Path


class FileLoaderApp:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Загрузка txt-файла")
        self.root.geometry("400x200")

        self.selected_file_path = None

        # Кнопка выбора файла
        self.btn_select_file = tk.Button(
            self.root,
            text="Выбрать txt-файл",
            command=self.select_file
        )
        self.btn_select_file.pack(pady=20)

        # Метка выбранного файла
        self.file_label = tk.Label(self.root, text="Файл не выбран")
        self.file_label.pack(pady=10)

        # Кнопка чтения файла
        self.btn_read_file = tk.Button(
            self.root,
            text="Прочитать файл",
            command=self.read_file
        )
        self.btn_read_file.pack(pady=10)

    def select_file(self):
        """Выбор txt-файла через диалоговое окно"""
        path = filedialog.askopenfilename(
            filetypes=[("Text files", "*.txt"), ("Все файлы", "*.*")]
        )
        if path:
            self.selected_file_path = path
            self.file_label.config(text=f"Выбран: {Path(path).name}")

    def read_file(self):
        """Чтение файла построчно"""
        if not self.selected_file_path:
            messagebox.showwarning("Внимание", "Сначала выберите файл.")
            return

        try:
            with open(self.selected_file_path, "r", encoding="utf-8") as file:
                for line_number, line in enumerate(file, start=1):
                    line = line.strip()
                    print(f"Строка {line_number}: {line}")
        except Exception as e:
            messagebox.showerror("Ошибка", f"Ошибка чтения файла: {e}")


if __name__ == "__main__":
    app = FileLoaderApp()
    app.root.mainloop()