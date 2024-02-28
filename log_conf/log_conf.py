import logging
def setup_logging():
    # Создание и настройка корневого логгера
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)  # Установка уровня логирования

    # Очистка всех существующих обработчиков
    logger.handlers = []

    # Форматирование сообщений лога
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Настройка обработчика вывода в файл с указанием кодировки UTF-8
    file_handler = logging.FileHandler('app.log', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    # Добавление обработчиков к логгеру
    logger.addHandler(file_handler)
   