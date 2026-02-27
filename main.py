# -*- coding: utf-8 -*-
"""
Точка входа ETL-приложения: GUI и оркестрация парсера, БД и отчётов.
"""

import logging
import queue
import threading
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, scrolledtext
from tkinter import ttk

from database import MongoManager
from parser import DataProcessor
from reports import export_to_excel_with_chart, generate_errors_report

# Попытка использовать customtkinter для современного вида
try:
    import customtkinter as ctk  # type: ignore[import-untyped]
    HAS_CTK = True
except ImportError:
    HAS_CTK = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.FileHandler("etl_app.log", encoding="utf-8")],
)
logger = logging.getLogger(__name__)


def _run_in_thread(target, daemon=True):
    """Запускает target в отдельном потоке."""
    t = threading.Thread(target=target, daemon=daemon)
    t.start()
    return t


class LogHandler(logging.Handler):
    """Перенаправляет логи в очередь для вывода в GUI."""

    def __init__(self, log_queue: queue.Queue):
        super().__init__()
        self.log_queue = log_queue

    def emit(self, record):
        try:
            msg = self.format(record)
            self.log_queue.put(("log", msg))
        except Exception:
            self.handleError(record)


class ETLApp:
    """Главное окно приложения: выбор файла, логи, прогресс, статистика, отчёты."""

    def __init__(self):
        self.root = ctk.CTk() if HAS_CTK else tk.Tk()
        if HAS_CTK:
            ctk.set_appearance_mode("system")
            ctk.set_default_color_theme("blue")
        self.root.title("ETL — Парсинг и загрузка в MongoDB")
        self.root.geometry("800x620")
        self.root.minsize(600, 500)

        self.log_queue = queue.Queue()
        self._after_id = None
        self._current_file = ""
        self._mongo_manager: MongoManager | None = None
        self._last_stats = {}  # для круговой диаграммы в Excel

        self._build_ui()
        self._process_log_queue()
        self._install_log_handler()

    def _build_ui(self):
        """Собирает интерфейс."""
        # Фрейм настроек
        frame_settings = (ctk.CTkFrame(self.root) if HAS_CTK else ttk.LabelFrame(self.root, text="Подключение"))
        frame_settings.pack(fill=tk.X, padx=10, pady=5)

        ttk.Label(frame_settings, text="MongoDB URI:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=3)
        self.uri_var = tk.StringVar(
            value="mongodb://dfyz:sDkazRHG6gNL@dfyz-mongo.thesongofsaya.dev:27017/dfyz_db?authSource=admin"
        )
        self.entry_uri = (ctk.CTkEntry(frame_settings, textvariable=self.uri_var, width=400)
                          if HAS_CTK else ttk.Entry(frame_settings, textvariable=self.uri_var, width=50))
        self.entry_uri.grid(row=0, column=1, sticky=tk.EW, padx=5, pady=3)
        frame_settings.columnconfigure(1, weight=1)

        # Файл
        frame_file = (ctk.CTkFrame(self.root) if HAS_CTK else ttk.Frame(self.root))
        frame_file.pack(fill=tk.X, padx=10, pady=5)

        ttk.Button(frame_file, text="Выбрать файл", command=self._on_select_file).pack(side=tk.LEFT, padx=5, pady=5)
        self.label_file = ttk.Label(frame_file, text="Файл не выбран", foreground="gray")
        self.label_file.pack(side=tk.LEFT, padx=5, pady=5)

        # Кнопки действий
        frame_actions = (ctk.CTkFrame(self.root) if HAS_CTK else ttk.Frame(self.root))
        frame_actions.pack(fill=tk.X, padx=10, pady=5)

        self.btn_process = ttk.Button(frame_actions, text="Обработать и загрузить в MongoDB", command=self._on_process)
        self.btn_process.pack(side=tk.LEFT, padx=5, pady=5)
        self.btn_export = ttk.Button(frame_actions, text="Экспорт в Excel", command=self._on_export)
        self.btn_export.pack(side=tk.LEFT, padx=5, pady=5)

        # Прогресс
        frame_progress = (ctk.CTkFrame(self.root) if HAS_CTK else ttk.Frame(self.root))
        frame_progress.pack(fill=tk.X, padx=10, pady=5)
        self.progress = ttk.Progressbar(frame_progress, mode="determinate")
        self.progress.pack(fill=tk.X, padx=5, pady=5)
        self.label_progress = ttk.Label(frame_progress, text="")
        self.label_progress.pack(anchor=tk.W, padx=5)

        # Статистика
        frame_stats = (ctk.CTkFrame(self.root) if HAS_CTK else ttk.LabelFrame(self.root, text="Статистика"))
        frame_stats.pack(fill=tk.X, padx=10, pady=5)
        self.label_stats = ttk.Label(
            frame_stats,
            text="Обработано: 0 | Успешно: 0 (0%) | Ошибок: 0 (0%)",
            font=("", 10),
        )
        self.label_stats.pack(anchor=tk.W, padx=5, pady=5)

        # Логи
        frame_log = (ctk.CTkFrame(self.root) if HAS_CTK else ttk.LabelFrame(self.root, text="Логи"))
        frame_log.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        self.log_text = scrolledtext.ScrolledText(frame_log, height=12, state=tk.DISABLED, wrap=tk.WORD)
        self.log_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        if HAS_CTK:
            self.log_text.configure(font=ctk.CTkFont(family="Consolas", size=11))
    def _install_log_handler(self):
        """Подключает вывод логов в очередь."""
        handler = LogHandler(self.log_queue)
        handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        logging.getLogger().addHandler(handler)

    def _log(self, msg: str, level: str = "info"):
        """Пишет сообщение в виджет логов и в очередь для обработки в main thread."""
        self.log_queue.put(("log", msg))

    def _process_log_queue(self):
        """Обрабатывает очередь логов в главном потоке."""
        try:
            while True:
                msg_type, payload = self.log_queue.get_nowait()
                if msg_type == "log":
                    self._append_log(payload)
                elif msg_type == "progress":
                    self._update_progress(payload[0], payload[1])
                elif msg_type == "stats":
                    self._update_stats(payload)
                elif msg_type == "done":
                    self._on_work_done()
        except queue.Empty:
            pass
        self._after_id = self.root.after(200, self._process_log_queue)

    def _append_log(self, msg: str):
        self.log_text.configure(state=tk.NORMAL)
        self.log_text.insert(tk.END, msg.rstrip() + "\n")
        self.log_text.see(tk.END)
        self.log_text.configure(state=tk.DISABLED)

    def _update_progress(self, current: int, total: int):
        if total <= 0:
            self.progress["value"] = 0
            self.label_progress["text"] = ""
        else:
            self.progress["maximum"] = total
            self.progress["value"] = current
            self.label_progress["text"] = f"{current} / {total}"

    def _update_stats(self, stats: dict):
        self._last_stats = stats
        processed = stats.get("processed", 0)
        success = stats.get("success", 0)
        errors = stats.get("errors", 0)
        pct_ok = (success / processed * 100) if processed else 0
        pct_err = (errors / processed * 100) if processed else 0
        self.label_stats["text"] = (
            f"Обработано: {processed} | Успешно: {success} ({pct_ok:.1f}%) | Ошибок: {errors} ({pct_err:.1f}%)"
        )

    def _on_work_done(self):
        self.btn_process["state"] = tk.NORMAL
        self.btn_export["state"] = tk.NORMAL

    def _on_select_file(self):
        path = filedialog.askopenfilename(
            title="Выберите текстовый файл",
            filetypes=[("Текстовые файлы", "*.txt"), ("Все файлы", "*.*")],
        )
        if path:
            self._current_file = path
            self.label_file["text"] = Path(path).name
            self.label_file["foreground"] = "black"

    def _on_process(self):
        if not self._current_file:
            messagebox.showwarning("Внимание", "Сначала выберите файл.")
            return
        uri = self.uri_var.get().strip()
        if not uri:
            messagebox.showwarning("Внимание", "Введите URI подключения к MongoDB.")
            return

        self.btn_process["state"] = tk.DISABLED
        self.btn_export["state"] = tk.DISABLED
        self.progress["value"] = 0
        self.label_progress["text"] = ""

        def work():
            try:
                self._log("Начало обработки файла...")
                processor = DataProcessor(log_callback=lambda m: self.log_queue.put(("log", m)))

                def progress_cb(cur, total):
                    self.log_queue.put(("progress", (cur, total)))

                successful, errors = processor.process_file(self._current_file, progress_callback=progress_cb)
                processed = len(successful) + len(errors)
                self.log_queue.put(("stats", {"processed": processed, "success": len(successful), "errors": len(errors)}))

                # Отчёт об ошибках
                if errors:
                    try:
                        generate_errors_report(self._current_file, errors)
                        self._log("Создан отчёт об ошибках: errors_report.txt")
                    except Exception as e:
                        self._log(f"Ошибка создания отчёта об ошибках: {e}")
                        logger.exception("errors_report")

                # Подключение к MongoDB и вставка
                mongo = MongoManager(uri, log_callback=lambda m: self.log_queue.put(("log", m)))
                if not mongo.connect():
                    self._log("Загрузка в MongoDB пропущена из-за ошибки подключения.")
                else:
                    inserted, total = mongo.insert_many(successful, progress_callback=progress_cb)
                    self._log(f"В MongoDB вставлено записей: {inserted} из {total}")
                    mongo.disconnect()
                    self._mongo_manager = mongo  # не храним соединение, только для справки

                self._log("Обработка завершена.")
            except Exception as e:
                self._log(f"Ошибка: {e}")
                logger.exception("process")
            finally:
                self.log_queue.put(("done", None))

        _run_in_thread(work)

    def _on_export(self):
        uri = self.uri_var.get().strip()
        if not uri:
            messagebox.showwarning("Внимание", "Введите URI подключения к MongoDB.")
            return

        path = filedialog.asksaveasfilename(
            title="Сохранить Excel",
            defaultextension=".xlsx",
            filetypes=[("Excel", "*.xlsx"), ("Все файлы", "*.*")],
        )
        if not path:
            return

        self.btn_export["state"] = tk.DISABLED

        stats_snapshot = dict(self._last_stats) if self._last_stats else {}

        def work():
            try:
                mongo = MongoManager(uri, log_callback=lambda m: self.log_queue.put(("log", m)))
                if not mongo.connect():
                    self._log("Экспорт отменён: нет подключения к MongoDB.")
                    self.log_queue.put(("done", None))
                    return
                docs = mongo.get_all_documents()
                mongo.disconnect()
                if not stats_snapshot:
                    stats_snapshot["processed"] = len(docs)
                    stats_snapshot["success"] = len(docs)
                    stats_snapshot["errors"] = 0
                export_to_excel_with_chart(path, docs, stats_snapshot)
                self._log(f"Экспорт в Excel выполнен: {path}")
            except Exception as e:
                self._log(f"Ошибка экспорта: {e}")
                logger.exception("export")
            finally:
                self.log_queue.put(("done", None))

        _run_in_thread(work)

    def run(self):
        self.root.mainloop()
        if self._after_id:
            self.root.after_cancel(self._after_id)


if __name__ == "__main__":
    app = ETLApp()
    app.run()