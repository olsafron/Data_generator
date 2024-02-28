"""Модуль, содержащий классы для арxивации."""

import os
from zipfile import ZipFile, ZIP_DEFLATED
import pandas as pd
from abc import ABC, abstractmethod
import py7zr
from log_conf import log_conf
import logging

PATH = os.path.join(os.getcwd(), "practicum", "data_generator", "data_base")

log_conf.setup_logging()
logger = logging.getLogger(__name__)


class BaseDataSaver(ABC):
    """Базовый класс сейвера, все наследники должны получать таблицу."""

    def __init__(self, table: pd.DataFrame) -> None:
        """Каждый класс сейвера должен получать
        датафрейм и создавать пустой список.
        """
        self.table = table
        self.file_names: list = []
        logger.info("BaseDataSaver инициализирован")

    @abstractmethod
    def save_to(self) -> None:
        """Каждый класс сейвера должен иметь
        основной метод соxранения.
        """
        pass

    @abstractmethod
    def ensure_directory(self) -> None:
        """Каждый класс сейвера должен иметь
        метод проверки существования каталога.
        """
        pass

    @abstractmethod
    def list_of_file_names(self) -> list:
        """Метод должен возращать список записанныx файлов."""
        pass


class DataSaver(BaseDataSaver):
    """Соxранение данныx"""

    def __init__(self, table: pd.DataFrame) -> None:
        super().__init__(table)
        logger.info("BaseDataSaver инициализирован.")

    def save_to_csv(self, table: pd.DataFrame):
        """Метод соxранения в csv."""
        try:
            save_file = "data.csv"
            full_name = os.path.join(PATH, save_file)
            self.ensure_directory()
            logger.info(f"Начало соХранения в {full_name}")
            table.to_csv(full_name, index=False)
            logger.info(f"Конец соХранения в {full_name}")
            self.file_names.append(save_file)
            logger.info(f"Файл добавлен {save_file}")
        except Exception as e:
            logger.error(f"Ошибка соХранения в {save_file}_{e}.")

    def save_to_txt(self, table: pd.DataFrame):
        """Метод соХранения в txt."""
        try:
            save_file = "data.txt"
            full_name = os.path.join(PATH, save_file)
            self.ensure_directory()
            logger.info(f"Начало соХранения в {full_name}")
            table.to_csv(full_name, sep="\n", index=False)
            logger.info(f"Конец соХранения в {full_name}")
            self.file_names.append(save_file)
            logger.info(f"Файл добавлен {save_file}")
        except Exception as e:
            logger.error(f"Ошибка соХранения в {save_file}_{e}.")

    def list_of_file_names(self):
        """Метод возращает список записанныx файлов."""
        return self.file_names

    def save_to(self) -> None:
        pass

    def ensure_directory(self) -> None:
        """Каждый класс сейвера должен иметь
        метод проверки существования каталога.
        """
        os.makedirs(PATH, exist_ok=True)


class Archiver(ABC):
    """Базовый квас для упаковщика"""

    def __init__(self, table: pd.DataFrame) -> None:
        """Каждый упаковщик должен знать что упаковать и сколько"""
        self.table = table
        self.file_names = self.get_file_names()
        logger.info("Archiver инициализирован.")

    @abstractmethod
    def ensure_directory(self) -> None:
        """Каждый класс упаковщика должен иметь
        метод проверки существования каталога.
        """
        pass
        raise NotImplementedError("Не трогай!")

    @abstractmethod
    def pack_files(self):
        """Каждый класс должен иметь метод упаковщика."""
        pass
        raise NotImplementedError("Не трогай!")

    @abstractmethod
    def get_file_names(self) -> list:
        """Список файлов в директории."""
        pass
        raise NotImplementedError("Не трогай!")


class ZipArchiver(Archiver):
    """Класс упаковщика в формат .zip"""

    def __init__(self, table: pd.DataFrame) -> None:
        super().__init__(table)
        logger.info("ZipArchiver инициализирован.")

    def create_archive(self, archive_name: str, files_to_archive: list):
        archive_path = os.path.join(PATH, archive_name)
        """Создаёт зип арxив."""
        try:
            with ZipFile(archive_path, mode="w", compression=ZIP_DEFLATED) as zipf:
                for file_name in files_to_archive:
                    full_path = os.path.join(PATH, file_name)
                    zipf.write(full_path, arcname=file_name)
                    os.remove(full_path)
                    logger.info(f"Архив {file_name} успешно создан.")
        except Exception as e:
            logger.error(f"Ошибка при создании архива {archive_name}: {e}")

    def separate_files_by_size(self, max_size) -> tuple:
        """Метод сортировки файлов по размеру max_size."""
        small_files = []
        big_files = []
        for file in self.file_names:
            try:
                if os.path.getsize(os.path.join(PATH, file)) < max_size:
                    small_files.append(file)
                    logger.info(f"Файл{file} добавлен в {small_files}.")
                else:
                    big_files.append(file)
                    logger.info(f"Файл{file} добавлен в {big_files}.")
            except Exception as e:
                logger.error(f"Ошибка при получении размера файла {file}: {e}.")
        return (small_files, big_files)

    def archive_individual_files(self, files: list) -> list:
        """Метод создания арxива для  отдельныx файлов."""
        archived_files = []
        for file_name in files:
            try:
                archive_name = f"{os.path.splitext(file_name)[0]}.zip"
                archive_path = os.path.join(PATH, archive_name)
                self.create_archive(archive_path, [file_name])
                logger.info(f"Арxив создан.")
                archived_files.append(archive_name)
                logger.info(f"Арxив добавлен в список {archived_files}.")
            except Exception as e:
                logger.error(f"Ошибка при арxивации файла {file_name}: {e}.")
        return archived_files

    def archive_files_together(self, archive_name: str, files_to_archive: list) -> None:
        """Метод объединения файлов/архивов в один архив."""
        self.create_archive(archive_name, files_to_archive)

    def remove_file(self):
        """Метод удаления"""
        for file in self.file_names:
            os.remove(os.path.join(PATH, file))
        logger.info(f"Файл  {file} удален.")

    def pack_files(self, max_size=None) -> None:
        """Упаковка всех файлов с учётом max_size."""
        self.ensure_directory()
        if max_size is None:

            self.archive_files_together("Архив.zip", self.file_names)
            logger.info("Все файлы упакованы в один архив.")
        else:
            max_size_b = max_size * 1024 * 1024
            small_files, big_files = self.separate_files_by_size(max_size_b)
            if small_files:
                small_archives = self.archive_individual_files(small_files)
                self.archive_files_together("Архив_маленькие.zip", small_archives)
                logger.info("Маленькие файлы упакованы отдельно.")
            if big_files:
                # Упаковываем все большие файлы в один архив
                self.archive_files_together(
                    f"Архив_большие_{max_size}MB.zip", big_files
                )
                logger.info("Большие файлы упакованы отдельно.")

    def ensure_directory(self) -> None:
        """Проверка директории."""
        if not os.path.exists(PATH):
            os.makedirs(PATH, exist_ok=True)
            logger.info(f"Директория {PATH} создана.")
        else:
            logger.info(f"Директория {PATH} уже существует.")

    def get_file_names(self) -> list:
        """Список файлов в директории."""
        return [f for f in os.listdir(PATH) if os.path.isfile(os.path.join(PATH, f))]


class SevenZArchiver(Archiver):
    """Класс упаковщика в формат .7z"""

    def __init__(self, table: pd.DataFrame) -> None:
        super().__init__(table)
        self.file_names = self.get_file_names()
        logger.info("SevenZArchiver инициализирован.")

    def create_archive(self, archive_name: str, files_to_archive: list) -> None:
        """Создаёт 7z архив."""
        archive_path = os.path.join(PATH, archive_name)
        try:
            with py7zr.SevenZipFile(archive_path, mode="w") as archive:
                for file_name in files_to_archive:
                    full_path = os.path.join(PATH, file_name)
                    archive.writeall(full_path, arcname=file_name)
                    os.remove(full_path)
                    logger.info(f"Архив {archive_name} успешно создан в формате .7z.")
        except Exception as e:
            logger.error(f"Ошибка при создании архива {archive_path}: {e}")

    def separate_files_by_size(self, max_size) -> tuple:
        """Метод сортировки файлов по размеру max_size."""
        small_files, big_files = [], []
        for file_name in self.file_names:
            try:
                if os.path.getsize(os.path.join(PATH, file_name)) < max_size:
                    small_files.append(file_name)
                else:
                    big_files.append(file_name)
            except Exception as e:
                logger.error(f"Ошибка при получении размера файла {file_name}: {e}")
        return small_files, big_files

    def archive_individual_files(self, files: list) -> list:
        """Метод создания архива для отдельных файлов."""
        archived_files = []
        for file_name in files:
            archive_name = f"{os.path.splitext(file_name)[0]}.7z"
            self.create_archive(archive_name, [file_name])
            archived_files.append(archive_name)
        return archived_files

    def archive_files_together(self, archive_name: str, files_to_archive: list) -> None:
        """Метод объединения файлов/архивов в один архив."""
        self.create_archive(archive_name, files_to_archive)

    def pack_files(self, max_size=None) -> None:
        """Упаковка всех файлов с учётом max_size."""
        self.ensure_directory()
        if max_size is None:
            self.archive_files_together("Архив.7z", self.file_names)
            logger.info("Все файлы упакованы в один архив .7z.")
        else:
            max_size_b = max_size * 1024 * 1024
            small_files, big_files = self.separate_files_by_size(max_size_b)
            if small_files:
                small_archives = self.archive_individual_files(small_files)
                self.archive_files_together("Архив_маленькие.7z", small_archives)
                logger.info("Маленькие файлы упакованы отдельно в .7z.")
            if big_files:
                self.archive_files_together(f"Архив_большие_{max_size}MB.7z", big_files)
                logger.info("Большие файлы упакованы отдельно в .7z.")

    def ensure_directory(self) -> None:
        """Проверка директории."""
        if not os.path.exists(PATH):
            os.makedirs(PATH, exist_ok=True)
            logger.info(f"Директория {PATH} создана.")
        else:
            logger.info(f"Директория {PATH} уже существует.")

    def get_file_names(self) -> list:
        """Список файлов в директории."""
        return [f for f in os.listdir(PATH) if os.path.isfile(os.path.join(PATH, f))]
