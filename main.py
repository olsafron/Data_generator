"""Исполняемый файл программы.Точка вxода."""
import pandas as pd
from generators import data_generator as g
from packers import data_packer as p
from tkinter import filedialog
import tkinter as tk
from log_conf import log_conf
import logging
log_conf.setup_logging()
logger = logging.getLogger(__name__)

def choose_file():
    
    root = tk.Tk()
    root.withdraw()
    file_path = filedialog.askopenfilename()
    logger.info(f'Выбран файл: {file_path}')
    return file_path

def first_choise():
    logger.info('Начало выбора способа получения данных')
    print('Выберите способ получения данных:')
    print('1. Генерация с помощью библиотеки Faker.')
    print('2. Загрузка из файла.')
    print('3. Ввод с клавиатуры.')
    while True:
        try:
            choice = int(input('Ваш выбор: (1, 2 или 3): ').strip())
            logger.info(f'Выбран способ получения данных: {choice}')
            if choice in [1, 2, 3]:
                return choice
            else:
                print('Пожалуйста, введите корректный номер опции.')
        except ValueError:
            print('Пожалуйста, введите число.')
    
def number_of_rows():
    logger.info('Запрос на ввод количества строк')
    while True:
        try:
            rows = int(input('Введите количество строк: ').strip())
            logger.info(f'Введено количество строк: {rows}')
            return rows
        except ValueError:
            print('Пожалуйста, введите корректное число строк.')

def save():
    logger.info('Начало выбора формата сохранения')
    print('У меня есть готовая к сохранению таблица.')
    print('Выберите формат сохранения:')
    print('1. Формат .csv')
    print('2. Формат .txt')
    while True:
        try:
            choise = int(input('Ваш выбор: ').strip())
            if choise in [1, 2]:
                logger.info(f'Выбран формат сохранения: {choise}')
                return choise
            else:
                print('Пожалуйста, введите 1 или 2.')
        except ValueError:
            print('Пожалуйста, введите число.')
def archive_format():
    logger.info('Начало выбора формата архивации')
    print('Выберите формат сохранения:')
    print('1. Формат .zip')
    print('2. Формат .7z')
    while True:
        try:
            choise = int(input('Ваш выбор: ').strip())
            if choise in [1, 2]:
                logger.info(f'Выбран формат архивации: {choise}')
                return choise
            else:
                print('Пожалуйста, введите 1 или 2.')
        except ValueError:
            print('Пожалуйста, введите число.')
def max_size():
    logger.info('Запрос на ввод максимального размера архива')
    print('Выберите максимальный размер архива:')
    while True:
        try:
            input_str = input('Ваш выбор: ').strip()
            if input_str == '':
                logger.info('Выбран неограниченный размер архива')
                return None
            size = int(input_str)
            logger.info(f'Выбран максимальный размер архива: {size}MB') 
            return size
        except ValueError:
            print('Пожалуйста, введите число или оставьте пустым.')
    
def save_menu():
    logger.info('Запрос на повторение программы')
    print('Повторить программу?')
    print('1. Да')
    print('2. Нет')
    while True:
        try:
            choise = int(input('Ваш выбор: ').strip())
            if choise in [1, 2]:
                logger.info(f'Выбор пользователя: ') 
                return choise
            else:
                print('Пожалуйста, введите 1 или 2.')
        except ValueError:
            print('Пожалуйста, введите число.')

def main():
    flag = True
    while flag:
        logger.info('Запуск главной программы')
        print('Добро пожаловать.')
        first = first_choise()
        generator = g.Datagenerator()
        if first == 1:
            rows = number_of_rows()
            generator.Generate(rows)
            logger.info('Таблица сгенерирована.')
        elif first == 2:
            file = choose_file()
            data_file = g.DataImporter_FromFile()
            data_file.import_from(file)
            logger.info('Данные загружены из файла.')
        elif first == 3:
            print('Упс... Способ ещё в работе.')
            logger.warning('Выбран способ ввода данных, который еще не реализован.')
            continue
        save_data = save()
        data = generator.table
        saver = p.DataSaver(data)
        
        if save_data == 1:
            saver.save_to_csv(data)
            print('Данные сохранены в формате CSV.')
            logger.info('Данные успешно сохранены в формате CSV.')
        elif save_data == 2:
            saver.save_to_txt(data)
            print('Данные сохранены в формате TXT.')
            logger.info('Данные успешно сохранены в формате TXT.')

        archive = archive_format()
        size = max_size()
        ar = p.ZipArchiver(data)
        ar7 = p.SevenZArchiver(data)
        
        if archive == 1:
            ar.pack_files(size)
            logger.info('Файлы успешно заархивированы в формате .zip.')
            
        elif archive == 2:
            ar7.pack_files(size)
            logger.info('Файлы успешно заархивированы в формате .7z.')
        print('Файлы заарxивированы.')
        print('Конец программы.')
        print('Повторить?')
        repeat = save_menu()
        if repeat == 1:
            continue
        if repeat == 2:
            flag = False
            logger.info('Выход из программы.')

if __name__ == "__main__":
    main()
