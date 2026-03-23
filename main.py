import json
import logging
import queue
import threading
import tkinter as tk
from reports import generate_html_errors_report
from pathlib import Path
from tkinter import filedialog, messagebox, scrolledtext
import tkinter.ttk as ttk
from reports import export_to_excel_combined
try:
    import winsound
    HAS_SOUND = True
except ImportError:
    HAS_SOUND = False

try:
    import matplotlib.pyplot as plt
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False

# Твои рабочие модули (parser.py, database.py, reports.py должны быть в той же папке)
from database import MongoManager
from parser import DataProcessor
from reports import export_to_excel_with_chart, generate_errors_report, generate_html_errors_report, export_to_csv

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

    def show_tree_menu(self, event):
        self.tree_menu.post(event.x_root, event.y_root)

    def copy_tree(self):
        selected = self.data_tree.selection()
        if selected:
            values = self.data_tree.item(selected[0])['values']
            text = "\t".join(str(v) for v in values)
            self.root.clipboard_clear()
            self.root.clipboard_append(text)

    def select_all_tree(self, event=None):
        for item in self.data_tree.get_children():
            self.data_tree.selection_add(item)

    def paste_to_entry(self, event=None):
        try:
            clipboard = self.root.clipboard_get()
            self.search_entry.insert(tk.INSERT, clipboard)
        except tk.TclError:
            pass
        return "break"

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
                    current, total, op = data
                    val = current / total
                    if op == "db":
                        self.progress_db.set(val) if HAS_CTK else self.progress_db.configure(value=val * 100)
                    elif op == "export":
                        self.progress_reports.set(val) if HAS_CTK else self.progress_reports.configure(value=val * 100)
                elif rtype == "notify":
                    messagebox.showinfo("Уведомление", data)
        except queue.Empty:
            pass
        self.root.after(100, self._update_loop)

    def load_config(self):
        """Загрузка настроек из config.json"""
        config_path = Path("config.json")
        if config_path.exists():
            try:
                with open(config_path, "r", encoding="utf-8") as f:
                    config = json.load(f)
                uri = config.get("uri", "")
                coll = config.get("collection", "records")
                theme = config.get("theme", "dark")
                if uri:
                    self.entry_uri.delete(0, tk.END)
                    self.entry_uri.insert(0, uri)
                if coll:
                    self.entry_collection.delete(0, tk.END)
                    self.entry_collection.insert(0, coll)
                if theme != self.theme:
                    self.toggle_theme()  # переключит и установит
                self._log("Настройки загружены из config.json")
            except Exception as e:
                self._log(f"Ошибка загрузки config.json: {e}")
        else:
            self._log("config.json не найден, используются значения по умолчанию")

    def save_config(self):
        """Сохранение настроек в config.json"""
        config = {
            "uri": self.entry_uri.get().strip(),
            "collection": self.entry_collection.get().strip() or "records",
            "theme": self.theme
        }
        try:
            with open("config.json", "w", encoding="utf-8") as f:
                json.dump(config, f, indent=4, ensure_ascii=False)
            self._log("Настройки сохранены в config.json")
        except Exception as e:
            self._log(f"Ошибка сохранения config.json: {e}")

    def _log(self, msg):
        """Внутренняя отправка лога в очередь"""
        self.log_queue.put(("log", msg))

    def test_connection(self):
        """Тест соединения с MongoDB"""
        uri = self.entry_uri.get().strip()
        if not uri:
            messagebox.showwarning("Внимание", "Введите URI для подключения.")
            return
        self.log_queue.put(("state", "disabled"))

        def work():
            try:
                mongo = MongoManager(uri, log_callback=self._log)
                if mongo.connect():
                    mongo.disconnect()
                    self.log_queue.put(("notify", "Соединение с MongoDB успешно установлено и закрыто."))
                else:
                    self.log_queue.put(("notify", "Не удалось подключиться к MongoDB. Проверьте URI и сеть."))
            except Exception as e:
                self.log_queue.put(("notify", f"Ошибка при тестировании соединения: {e}"))
            finally:
                self.log_queue.put(("state", "normal"))

        _run_in_thread(work)

    def toggle_theme(self):
        if HAS_CTK:
            if self.theme == "dark":
                ctk.set_appearance_mode("light")
                self.theme = "light"
                self.btn_theme.configure(text="Переключить на Темную")
            else:
                ctk.set_appearance_mode("dark")
                self.theme = "dark"
                self.btn_theme.configure(text="Переключить на Светлую")
    def select_file(self):
        """Выбор файла для парсинга"""
        path = filedialog.askopenfilename(filetypes=[("Text", "*.txt"), ("Все файлы", "*.*")])
        if path:
            self.selected_file_path = path
            self.file_label.configure(text=f"Выбран: {Path(path).name}")

    def on_parse(self):
        """Обработчик парсинга выбранного файла"""
        if not self.selected_file_path:
            messagebox.showwarning("Внимание", "Сначала выберите файл для парсинга.")
            return

        path = self.selected_file_path
        self.log_queue.put(("state", "disabled"))

        def work():
            try:
                self._log(f"--- Начат парсинг: {Path(path).name} ---")
                success_rows, errors = self.processor.process_file(path)
                self._last_parsed_data = success_rows
                self._last_parsed_errors = errors
                self._last_stats = {"processed": len(success_rows) + len(errors), "success": len(success_rows),
                                    "errors": len(errors)}
                self.populate_tree(success_rows)
                if errors:
                    generate_errors_report(path, errors)
                    generate_html_errors_report(path, errors)
                    self._log(f"Найдены ошибки ({len(errors)}). Отчеты созданы в папке с файлом.")
                self._log(f"Успешно обработано строк: {len(success_rows)}")
                self.log_queue.put(("stats", f"Обработано: {self._last_stats['processed']}, Успешно: {self._last_stats['success']}, Ошибок: {self._last_stats['errors']}"))
                self.log_queue.put(("notify", f"Парсинг завершен! Обработано: {self._last_stats['processed']}, Успешно: {self._last_stats['success']}, Ошибок: {self._last_stats['errors']}"))
            finally:
                self.log_queue.put(("state", "normal"))

        _run_in_thread(work)

    def populate_tree(self, rows):
        # 1. Сначала очищаем таблицу
        self.data_tree.delete(*self.data_tree.get_children())

        if not rows:
            return

        # 2. ПОЛУЧАЕМ ЛИМИТ (то, чего не хватало)
        try:
            limit_val = self.display_limit_entry.get().strip()
            limit = int(limit_val) if limit_val else 50
        except ValueError:
            limit = 50
            self._log("Ошибка: Некорректный лимит, использую 50 строк по умолчанию.")

        # 3. СОЗДАЕМ display_rows (определение переменной)
        display_rows = rows[:limit]

        # 4. ЗАПОЛНЯЕМ ТАБЛИЦУ
        for i, row in enumerate(display_rows):
            # Проверяем, пришел ли нам объект или словарь (из БД)
            is_dict = isinstance(row, dict)

            # Универсальное получение данных (поддерживает и старый, и новый формат)
            acc = row.get("Лицевой счет") if is_dict else getattr(row, 'account', '-')
            fio = row.get("ФИО") if is_dict else getattr(row, 'full_name', '-')
            addr = row.get("Адрес") if is_dict else getattr(row, 'address', '-')
            period = row.get("Период") if is_dict else getattr(row, 'period_display', '-')
            total = row.get("Общая сумма") if is_dict else getattr(row, 'total_amount', 0.0)
            entries = row.get("Услуги") if is_dict else getattr(row, 'entries', [])

            # Форматируем список услуг в одну строку
            services_str = "-"
            if entries:
                services_str = ", ".join(
                    [f"{item.get('Счёт и услуга', 'Услуга')}: {item.get('Сумма', 0)}" for item in entries])

            # Вставляем данные в Treeview
            self.data_tree.insert("", "end", values=(
                i + 1,
                acc,
                fio,
                addr,
                period,
                f"{total:.2f}",
                services_str
            ))

        self._log(f"Отображено {len(display_rows)} из {len(rows)} записей.")
    def on_search(self):
        """Поиск в спарсенных данных"""
        query = self.search_entry.get().strip().lower()
        if not self._last_parsed_data:
            messagebox.showinfo("Информация", "Нет данных для поиска. Сначала распарсите файл.")
            return
        filtered = [row for row in self._last_parsed_data if query in str(row.account).lower() or query in row.full_name.lower()]
        self.populate_tree(filtered)
        self._log(f"Найдено {len(filtered)} записей по запросу '{query}'")

    def on_show_stats(self):
        """Показать статистику в виде круговой диаграммы"""
        if not self._last_stats:
            messagebox.showinfo("Информация", "Нет статистики. Сначала распарсите файл.")
            return
        if not HAS_MATPLOTLIB:
            messagebox.showerror("Ошибка", "Matplotlib не установлен. Установите для просмотра графиков.")
            return

        labels = ['Успешно', 'Ошибки']
        sizes = [self._last_stats['success'], self._last_stats['errors']]
        colors = ['#27ae60', '#e74c3c']

        plt.figure(figsize=(6, 6))
        plt.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%', startangle=90)
        plt.title('Статистика обработки данных')
        plt.axis('equal')
        plt.show()

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
                        progress_callback=lambda c, t: self.log_queue.put(("progress", (c, t, "db")))
                    )
                    mongo.disconnect()
                    self._log("Загрузка в БД успешно завершена.")
                    self.log_queue.put(("notify", "Загрузка в базу данных завершена успешно!"))
            finally:
                self.log_queue.put(("state", "normal"))

        _run_in_thread(work)

    def on_clear(self):
        """Очистка коллекции в MongoDB"""
        uri = self.entry_uri.get().strip()
        coll_name = self.entry_collection.get().strip() or "records"

        result = messagebox.askyesno("Подтверждение", f"Вы уверены, что хотите очистить коллекцию '{coll_name}'? Все данные будут удалены!")
        if not result:
            return

        self.log_queue.put(("state", "disabled"))

        def work():
            try:
                mongo = MongoManager(uri, collection=coll_name, log_callback=self._log)
                if mongo.connect():
                    if mongo.clear_collection():
                        self.log_queue.put(("notify", f"Коллекция '{coll_name}' успешно очищена."))
                    mongo.disconnect()
            except Exception as e:
                self.log_queue.put(("notify", f"Ошибка при очистке коллекции: {e}"))
            finally:
                self.log_queue.put(("state", "normal"))

        _run_in_thread(work)

    def on_export(self):
        """Экспорт результатов последнего парсинга в Excel (Успех + Ошибки)"""
        errors_data = getattr(self, '_last_parsed_errors', [])
        if errors_data:
            # Передаем путь к исходному файлу, отчет создастся рядом
            generate_html_errors_report(self.file_label.cget("text"), errors_data)
        # Проверяем, запускал ли пользователь парсинг
        if not hasattr(self, '_last_parsed_data') and not hasattr(self, '_last_parsed_errors'):
            messagebox.showwarning("Внимание", "Сначала выберите файл и запустите парсинг!")
            return

        path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel отчет", "*.xlsx")],
            title="Сохранить отчет о парсинге"
        )
        if not path:
            return

        # Защита от кривого ввода расширения пользователем
        final_path = str(Path(path).with_suffix('.xlsx'))

        self.btn_export.configure(state="disabled")
        self._log("Формирование объединенного Excel-отчета...")

        def work():
            try:
                # Берем данные из памяти приложения
                success_data = getattr(self, '_last_parsed_data', [])
                errors_data = getattr(self, '_last_parsed_errors', [])

                # Вызываем нашу новую функцию
                export_to_excel_combined(final_path, success_data, errors_data)

                self._log(f"Отчет успешно сохранен: {final_path}")
                self.log_queue.put(
                    ("notify", f"Отчет сохранен!\n\nУспешных: {len(success_data)}\nОшибок: {len(errors_data)}"))
            except Exception as e:
                self._log(f"Ошибка при создании Excel: {e}")
            finally:
                self.log_queue.put(("state", "normal"))

        # Запускаем в фоне, чтобы интерфейс не завис
        _run_in_thread(work)

    def on_export_csv(self):
        """Экспорт всей коллекции из БД в CSV"""
        uri = self.entry_uri.get().strip()
        coll_name = self.entry_collection.get().strip() or "records"

        path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV File", "*.csv")],
            title="Сохранить CSV отчет"
        )
        if not path: return

        # ГАРАНТИЯ РАСШИРЕНИЯ: Если пользователь стёр .csv или ввёл другое, исправляем
        final_path = str(Path(path).with_suffix('.csv'))

        self.log_queue.put(("state", "disabled"))

        def work():
            try:
                mongo = MongoManager(uri, collection=coll_name, log_callback=self._log)
                if mongo.connect():
                    self._log(f"Извлечение данных из '{coll_name}' для CSV отчета...")
                    docs = mongo.get_all_documents(progress_callback=lambda c, t: self.log_queue.put(("progress", (c, t, "export"))))
                    mongo.disconnect()

                    if not docs:
                        self._log("Ошибка: Таблица пуста, нечего экспортировать.")
                        return

                    if len(docs) > 10000:
                        self._log(f"Предупреждение: Экспорт {len(docs)} записей может занять время. Пожалуйста, подождите...")

                    export_to_csv(final_path, docs)
                    self._log(f"CSV-отчет успешно создан: {final_path}")
                    self.log_queue.put(("notify", f"Экспорт в CSV завершен! Отчет сохранен: {final_path}"))
            except Exception as e:
                self._log(f"Ошибка при экспорте в CSV: {e}")
            finally:
                self.log_queue.put(("state", "normal"))

        _run_in_thread(work)


if __name__ == "__main__":
    app = ETLApp()
    app.root.mainloop()