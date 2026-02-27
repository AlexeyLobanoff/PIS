# -*- coding: utf-8 -*-
import logging
import queue
import threading
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, scrolledtext

# Твои рабочие модули (parser.py, database.py, reports.py должны быть в той же папке)
from database import MongoManager
from parser import DataProcessor
from reports import export_to_excel_with_chart, generate_errors_report

# Продвинутый интерфейс (CustomTkinter)
try:
    import customtkinter as ctk

    HAS_CTK = True
except ImportError:
    HAS_CTK = False

# --- Настройка логирования в файл и консоль ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.FileHandler("etl_app.log", encoding="utf-8")],
)
logger = logging.getLogger(__name__)


def _run_in_thread(target):
    """Запуск функции в фоновом потоке, чтобы GUI не зависал при тяжелых операциях"""
    t = threading.Thread(target=target, daemon=True)
    t.start()
    return t


class ETLApp:
    def __init__(self):
        # Настройка графической темы (Dark Mode по умолчанию)
        if HAS_CTK:
            ctk.set_appearance_mode("dark")
            ctk.set_default_color_theme("blue")
            self.root = ctk.CTk()
        else:
            self.root = tk.Tk()

        self.root.title("Система обработки данных ЖКХ")
        self.root.geometry("950x850")

        # Очередь для передачи логов и команд из рабочих потоков в главный поток GUI
        self.log_queue = queue.Queue()

        # Инициализация парсера с callback-функцией для вывода логов
        self.processor = DataProcessor(log_callback=lambda m: self.log_queue.put(("log", m)))

        # Переменные для хранения временных данных и статистики
        self._last_parsed_data = []
        self._last_stats = {}

        self._setup_ui()
        self._update_loop()

    def _setup_ui(self):
        """Создание и размещение элементов управления"""
        pad = {"padx": 20, "pady": 10}

        # --- Секция настроек (URI и Таблица) ---
        self.settings_frame = ctk.CTkFrame(self.root) if HAS_CTK else tk.Frame(self.root)
        self.settings_frame.pack(fill="x", **pad)

        # Строка подключения
        (ctk.CTkLabel(self.settings_frame, text="Строка подключения MongoDB:") if HAS_CTK else tk.Label(
            self.settings_frame, text="URI:")).pack()
        self.entry_uri = (ctk.CTkEntry(self.settings_frame, width=800) if HAS_CTK else tk.Entry(self.settings_frame))
        self.entry_uri.pack(pady=5, padx=20)
        self.entry_uri.insert(0,
                              "mongodb://dfyz:sDkazRHG6gNL@dfyz-mongo.thesongofsaya.dev:27017/dfyz_db?authSource=admin")

        # Имя коллекции
        (ctk.CTkLabel(self.settings_frame, text="Имя таблицы (коллекции):") if HAS_CTK else tk.Label(
            self.settings_frame, text="Таблица:")).pack()
        self.entry_collection = (
            ctk.CTkEntry(self.settings_frame, width=300) if HAS_CTK else tk.Entry(self.settings_frame))
        self.entry_collection.pack(pady=5)
        self.entry_collection.insert(0, "records")

        # --- Секция кнопок управления ---
        self.btn_frame = ctk.CTkFrame(self.root, fg_color="transparent") if HAS_CTK else tk.Frame(self.root)
        self.btn_frame.pack(fill="x", padx=20)

        self.btn_parse = ctk.CTkButton(self.btn_frame, text="1. ПАРСИНГ ФАЙЛА", command=self.on_parse,
                                       fg_color="#34495e", height=45)
        self.btn_parse.pack(side="left", padx=5, expand=True, fill="x")

        self.btn_save = ctk.CTkButton(self.btn_frame, text="2. ЗАГРУЗКА В БД", command=self.on_save, fg_color="#27ae60",
                                      height=45)
        self.btn_save.pack(side="left", padx=5, expand=True, fill="x")

        self.btn_export = ctk.CTkButton(self.btn_frame, text="3. ЭКСПОРТ В EXCEL", command=self.on_export,
                                        fg_color="#2980b9", height=45)
        self.btn_export.pack(side="left", padx=5, expand=True, fill="x")
        self.btns = [self.btn_parse, self.btn_save, self.btn_export]

        # --- Прогресс-бар ---
        self.progress = ctk.CTkProgressBar(self.root, width=800) if HAS_CTK else tk.ttk.Progressbar(self.root,
                                                                                                    length=800)
        self.progress.pack(pady=15)
        if HAS_CTK: self.progress.set(0)

        # --- Окно вывода логов (Консоль) ---
        self.log_area = scrolledtext.ScrolledText(self.root, bg="#1a1a1a", fg="#00ff00", font=("Consolas", 11))
        self.log_area.pack(fill="both", expand=True, **pad)

    def _update_loop(self):
        """Метод обновляет UI, вычитывая данные из очереди log_queue"""
        try:
            while True:
                rtype, data = self.log_queue.get_nowait()
                if rtype == "log":
                    self.log_area.insert(tk.END, f" {data}\n")
                    self.log_area.see(tk.END)
                elif rtype == "state":
                    for b in self.btns: b.configure(state=data)
                elif rtype == "progress":
                    current, total = data
                    val = current / total
                    self.progress.set(val) if HAS_CTK else self.progress.configure(value=val * 100)
        except queue.Empty:
            pass
        self.root.after(100, self._update_loop)

    def _log(self, msg):
        """Внутренняя отправка лога в очередь"""
        self.log_queue.put(("log", msg))

    def on_parse(self):
        """Обработчик выбора файла и парсинга"""
        path = filedialog.askopenfilename(filetypes=[("Text", "*.txt"), ("Все файлы", "*.*")])
        if not path: return

        self.log_queue.put(("state", "disabled"))

        def work():
            try:
                self._log(f"--- Начат парсинг: {Path(path).name} ---")
                success_rows, errors = self.processor.process_file(path)
                self._last_parsed_data = success_rows
                self._last_stats = {"processed": len(success_rows) + len(errors), "success": len(success_rows),
                                    "errors": len(errors)}
                if errors:
                    generate_errors_report(path, errors)
                    self._log(f"Найдены ошибки ({len(errors)}). Отчет создан в папке с файлом.")
                self._log(f"Успешно обработано строк: {len(success_rows)}")
            finally:
                self.log_queue.put(("state", "normal"))

        _run_in_thread(work)

    def on_save(self):
        """Загрузка распарсенных данных в MongoDB с отображением прогресса"""
        uri = self.entry_uri.get().strip()
        coll_name = self.entry_collection.get().strip() or "records"

        if not self._last_parsed_data:
            messagebox.showwarning("!", "Нет данных! Сначала распарсите текстовый файл.")
            return

        self.log_queue.put(("state", "disabled"))

        def work():
            try:
                mongo = MongoManager(uri, collection=coll_name, log_callback=self._log)
                if mongo.connect():
                    self._log(f"Загрузка в таблицу '{coll_name}'...")
                    mongo.insert_many(
                        self._last_parsed_data,
                        progress_callback=lambda c, t: self.log_queue.put(("progress", (c, t)))
                    )
                    mongo.disconnect()
                    self._log("Загрузка в БД успешно завершена.")
            finally:
                self.log_queue.put(("state", "normal"))

        _run_in_thread(work)

    def on_export(self):
        """Экспорт всей коллекции из БД в Excel с автоматическим расширением файла"""
        uri = self.entry_uri.get().strip()
        coll_name = self.entry_collection.get().strip() or "records"

        path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel Workbook", "*.xlsx")],
            title="Сохранить отчет"
        )
        if not path: return

        # ГАРАНТИЯ РАСШИРЕНИЯ: Если пользователь стёр .xlsx или ввёл другое, исправляем
        final_path = str(Path(path).with_suffix('.xls'))

        self.log_queue.put(("state", "disabled"))

        def work():
            try:
                mongo = MongoManager(uri, collection=coll_name, log_callback=self._log)
                if mongo.connect():
                    self._log(f"Извлечение данных из '{coll_name}' для отчета...")
                    docs = mongo.get_all_documents()
                    mongo.disconnect()

                    if not docs:
                        self._log("Ошибка: Таблица пуста, нечего экспортировать.")
                        return

                    # Используем текущую статистику или считаем по факту из БД
                    stats = self._last_stats if self._last_stats else {"processed": len(docs), "success": len(docs),
                                                                       "errors": 0}

                    export_to_excel_with_chart(final_path, docs, stats)
                    self._log(f"Excel-отчет успешно создан: {final_path}")
            except Exception as e:
                self._log(f"Ошибка при экспорте: {e}")
            finally:
                self.log_queue.put(("state", "normal"))

        _run_in_thread(work)


if __name__ == "__main__":
    app = ETLApp()
    app.root.mainloop()