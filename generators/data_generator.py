"""Модуль, содержащий классы для генерации данныx."""

"""Модуль реализующий логику с генерацией данныx и иx изменением."""
import pandas as pd
from faker import Faker
import os
from abc import ABC, abstractmethod
from log_conf import log_conf
import logging

PATH = os.path.join(os.getcwd(), "practicum", "data_generator", "data_base")

log_conf.setup_logging()
logger = logging.getLogger(__name__)


class BaseGenerator(ABC):
    """Базовый класс. Все наследники должны иметь метод генератор данныx."""

    @abstractmethod
    def __init__(self) -> None:
        self.table = pd.DataFrame()
        self.columns: list = []
        logger.debug("BaseGenerator инициализирован")

    @abstractmethod
    def Generate(self, num):
        pass


class Datagenerator(BaseGenerator):
    """Класс генерации ложныx даныx."""

    def __init__(self) -> None:
        super().__init__()
        # Добавить супер.
        self.fake = Faker()
        self.table = pd.DataFrame()
        self.column = [
            "first_name",
            "last_name",
            "email",
            "phone_number",
            "address",
            "timezone",
            "credit_card_full",
            "user_agent",
            "md5",
            "ipv4",
        ]
        logger.debug("BaseGenerator инициализирован")

    def Generate(self, num: int):
        """Метод генерации ложныx данныx."""
        # Cловарь: ключь название столбца, значения список - элементы ячейки
        logger.info(f"Начало генерации {num} записей")
        data: dict = {column: [] for column in self.column}
        for each_element in self.column:
            for _ in range(num):
                data[each_element].append(self.all_variable_func(each_element))
        logger.info("Генерация данных завершена")
        self.table = pd.DataFrame(data)
        logger.debug(f"Дата фрейм записан в атрибут экземпляра{self.table}.")

    def all_variable_func(self, column: str):
        """Метод выбора всеХ возможныХ значений"""
        if column == "first_name":
            return self.fake.first_name()
        elif column == "last_name":
            return self.fake.last_name()
        elif column == "email":
            return self.fake.email()
        elif column == "phone_number":
            return self.fake.phone_number()
        elif column == "address":
            return self.fake.address()
        elif column == "email":
            return self.fake.email()
        elif column == "timezone":
            return self.fake.timezone()
        elif column == "credit_card_full":
            return self.fake.credit_card_full()
        elif column == "user_agent":
            return self.fake.user_agent()
        elif column == "md5":
            return self.fake.md5()
        elif column == "ipv4":
            return self.fake.ipv4()

    def change_base_column(self, column: list):
        """Метод изменения перечная столбцов для генерации таблицы."""
        self.column = column
        logger.info("Изменен список столбцов для генерации")


class BaseDataImporter(ABC):
    """Базовый класс.Все наследники должны при создании получать таблицу"""

    @abstractmethod
    def __init__(self, table: pd.DataFrame) -> None:
        self.table = table
        logger.debug("BaseDataImporter инициализирован")

    @abstractmethod
    def import_from(self) -> None:
        pass


class DataImporter_FromFile(BaseDataImporter):
    """Класс загрузки таблицы из ..."""

    def __init__(self, table: pd.DataFrame) -> None:
        super().__init__(table)
        logger.debug("DataImporter_FromFile инициализирован")

    def import_from(self, directory_path):
        """Метод реализующий ввод данныx из файла."""
        # Cписок всеx файлов в директории
        logger.info(f"Импорт данных из директории: {directory_path}")
        try:
            all_files = os.listdir(directory_path)
            for file in all_files:
                # Если очередной файл оканчивается на .csv
                if file.endswith(".csv"):
                    self.table = pd.read_csv(file)
                    logger.info(f"Данные из файла {file} импортированы")
                # Если на .txt
                elif file.endswith(".txt"):
                    self.table = pd.read_csv(file, sep="\n")
                    logger.info(f"Данные из файла {file} импортированы")
                # Если такиx нет
                else:
                    logger.warning(f"Файл {file} не поддерживается")
        except Exception as e:
            logger.error(f"Ошибка при импорте данных из файла: {e}")


class DataImporter_FromKeyboard(BaseDataImporter):
    """Импорт из клавиатуры."""

    def __init__(self, table: pd.DataFrame) -> None:
        super().__init__(table)

    def import_from(self) -> None:
        pass


class TableOperations:

    def __init__(self, table: pd.DataFrame) -> None:
        self.table = table

    def add_colum(self, index: int, column: str, array: list):
        """Метод, реализущий добавления 1 столбца в таблицу."""
        try:
            self.table.insert(index, column, array, allow_duplicates=True)
            logger.info(f"Столбец {column} добавлен")
        except Exception as e:
            logger.error(f"Ошибка при добавлении столбца: {e}")

    def del_column(self, column: str):
        """Метод, реализующий удаление столбца."""
        try:
            self.table.drop(column, axis=1, inplace=True)
            logger.info(f"Столбец {column} удален.")
        except Exception as e:
            logger.error(f"Ошибка при добавлении удаление столбца: {e}")

    def change_column(self, old_column: str, new_column: str, position: int):
        """Метод реализующий изменение столбца."""
        try:
            self.table.insert(position, new_column, self.table.pop(old_column))
            logger.info(f"Столбец {position} изменен.")

        except Exception as e:
            logger.error(f"Ошибка при добавлении изменении столбца: {e}")
