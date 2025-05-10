import os
import asyncio
from botocore.exceptions import ClientError
import logging
from pathlib import Path
import aiofiles
import aioboto3
from botocore.config import Config


class DocxS3Uploader:
    def __init__(self, aws_access_key_id, aws_secret_access_key, bucket_name,
                 endpoint_url="https://s3.firstvds.ru:443"):
        """
        Инициализация класса для загрузки DOCX файлов на S3-совместимое хранилище

        :param aws_access_key_id: ID ключа доступа S3
        :param aws_secret_access_key: Секретный ключ доступа S3
        :param bucket_name: Имя S3 бакета
        :param endpoint_url: URL endpoint для S3-совместимого хранилища
        """
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.endpoint_url = endpoint_url
        self.bucket_name = bucket_name
        # Создаем конфигурацию без прокси
        self.boto_config = Config(
            proxies={'http': None, 'https': None},
            retries={'max_attempts': 3, 'mode': 'standard'},
            connect_timeout=20,
            read_timeout=60
        )
        self.logger = self._setup_logger()

    def _setup_logger(self):
        """Настройка логгера"""
        logger = logging.getLogger("DocxS3Uploader")
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        # Проверяем, есть ли уже обработчики
        if not logger.handlers:
            logger.addHandler(handler)
        return logger

    def _get_client_config(self):
        """
        Создает базовую конфигурацию для boto3 клиентов

        :return: Словарь с параметрами конфигурации
        """
        return {
            'endpoint_url': self.endpoint_url,
            'config': self.boto_config,
            'verify': False
        }

    async def get_file_url(self, object_key):
        """
        Получает URL для доступа к объекту в S3

        :param object_key: Ключ (путь) объекта в S3
        :return: URL для доступа к объекту
        """
        # Попробуем получить presigned URL, который точно будет работать
        session = aioboto3.Session(
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key
        )

        try:
            client_params = self._get_client_config()
            async with session.client('s3', **client_params) as s3:
                presigned_url = await s3.generate_presigned_url(
                    'get_object',
                    Params={'Bucket': self.bucket_name, 'Key': object_key},
                    ExpiresIn=604800  # URL будет действителен 7 дней
                )
                self.logger.info(f"Сгенерирован presigned URL для {object_key}")
                return presigned_url
        except Exception as e:
            self.logger.warning(f"Ошибка при создании presigned URL: {e}, возвращаем стандартный URL")
            # Если presigned URL не работает, вернем path-style URL как наиболее совместимый
            return f"{self.endpoint_url}/{self.bucket_name}/{object_key}"

    async def upload_file(self, file_path, object_name=None):
        """
        Асинхронная загрузка файла на S3

        :param file_path: Путь к файлу для загрузки
        :param object_name: Имя объекта в S3. Если None, используется имя файла
        :return: Кортеж (успех, URL) где успех - True если файл успешно загружен, иначе False,
                 URL - ссылка на файл если успешно, иначе None
        """
        if object_name is None:
            object_name = os.path.basename(file_path)

        session = aioboto3.Session(
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key
        )

        try:
            # Передаем конфигурацию в клиент
            client_params = self._get_client_config()
            async with session.client('s3', **client_params) as s3:
                self.logger.info(f"Начинаем загрузку файла {file_path}")
                async with aiofiles.open(file_path, 'rb') as file:
                    content = await file.read()
                    await s3.put_object(
                        Bucket=self.bucket_name,
                        Key=object_name,
                        Body=content,
                        ContentType='application/vnd.openxmlformats-officedocument.wordprocessingml.document'
                    )
            self.logger.info(f"Файл {file_path} успешно загружен как {object_name}")

            # Получаем URL файла
            file_url = await self.get_file_url(object_name)
            return True, file_url
        except ClientError as e:
            self.logger.error(f"Ошибка при загрузке файла {file_path}: {e}")
            return False, None
        except Exception as e:
            self.logger.error(f"Неожиданная ошибка при загрузке файла {file_path}: {e}")
            return False, None

    async def check_bucket_exists(self):
        """
        Проверяет существование бакета

        :return: True если бакет существует, иначе False
        """
        session = aioboto3.Session(
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key
        )

        try:
            # Передаем конфигурацию в клиент
            client_params = self._get_client_config()
            async with session.client('s3', **client_params) as s3:
                self.logger.info(f"Проверяем существование бакета {self.bucket_name}")
                # Печатаем для отладки
                self.logger.info(f"Используем endpoint: {self.endpoint_url}")
                await s3.head_bucket(Bucket=self.bucket_name)
                self.logger.info(f"Бакет {self.bucket_name} существует")
                return True
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == '404':
                self.logger.error(f"Бакет {self.bucket_name} не существует")
            elif error_code == '403':
                self.logger.error(f"Нет доступа к бакету {self.bucket_name}")
            else:
                self.logger.error(f"Ошибка при проверке бакета: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Неожиданная ошибка при проверке бакета: {e}")
            self.logger.error(f"Тип ошибки: {type(e)}")
            return False

    async def create_bucket_if_not_exists(self):
        """
        Создает бакет, если он не существует

        :return: True если бакет создан или уже существует, иначе False
        """
        if await self.check_bucket_exists():
            return True

        session = aioboto3.Session(
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key
        )

        try:
            # Передаем конфигурацию в клиент
            client_params = self._get_client_config()
            async with session.client('s3', **client_params) as s3:
                await s3.create_bucket(Bucket=self.bucket_name)
                self.logger.info(f"Бакет {self.bucket_name} успешно создан")
                return True
        except ClientError as e:
            self.logger.error(f"Ошибка при создании бакета {self.bucket_name}: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Неожиданная ошибка при создании бакета: {e}")
            return False

    async def list_buckets(self):
        """
        Получает список всех доступных бакетов

        :return: Список имен бакетов или пустой список в случае ошибки
        """
        session = aioboto3.Session(
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key
        )

        try:
            # Передаем конфигурацию в клиент
            client_params = self._get_client_config()
            async with session.client('s3', **client_params) as s3:
                response = await s3.list_buckets()
                buckets = [bucket['Name'] for bucket in response.get('Buckets', [])]
                self.logger.info(f"Доступные бакеты: {buckets}")
                return buckets
        except Exception as e:
            self.logger.error(f"Ошибка при получении списка бакетов: {e}")
            return []

    async def upload_folder(self, folder_path, prefix=""):
        """
        Асинхронно загружает все DOCX файлы из указанной папки на S3

        :param folder_path: Путь к папке с DOCX файлами
        :param prefix: Префикс для имен объектов в S3
        :return: Словарь с результатами загрузки {имя_файла: (результат, ссылка)}
        """
        # Сначала проверим сервис - запросим список бакетов
        buckets = await self.list_buckets()
        if not buckets:
            self.logger.error("Невозможно подключиться к S3 сервису или получить список бакетов")
            return {}

        if self.bucket_name not in buckets:
            self.logger.info(f"Пробуем создать бакет {self.bucket_name}")
            bucket_created = await self.create_bucket_if_not_exists()
            if not bucket_created:
                self.logger.error("Не удалось создать бакет")
                return {}

        folder = Path(folder_path)
        if not folder.exists() or not folder.is_dir():
            self.logger.error(f"Папка {folder_path} не существует или не является директорией")
            return {}

        docx_files = list(folder.glob("**/*.docx"))
        if not docx_files:
            self.logger.warning(f"В папке {folder_path} не найдено DOCX файлов")
            return {}

        self.logger.info(f"Найдено {len(docx_files)} DOCX файлов для загрузки")

        tasks = []
        file_object_names = {}  # Словарь для хранения соответствия файлов и их ключей в S3

        for file_path in docx_files:
            # Создаем имя объекта в S3 с учетом префикса и относительного пути
            relative_path = file_path.relative_to(folder)
            object_name = f"{prefix}{relative_path}" if prefix else str(relative_path)

            # Заменяем обратные слеши на прямые для совместимости с S3
            object_name = object_name.replace("\\", "/")

            # Сохраняем соответствие файла и его ключа в S3
            file_object_names[str(file_path)] = object_name

            # Добавляем задачу на загрузку файла
            tasks.append(self.upload_file(str(file_path), object_name))

        # Выполняем все задачи конкурентно
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Формируем словарь результатов
        upload_results = {}
        for file_path, result in zip(docx_files, results):
            str_path = str(file_path)
            if isinstance(result, Exception):
                self.logger.error(f"Исключение при загрузке {file_path}: {result}")
                upload_results[str_path] = (False, None)
            else:
                success, url = result
                upload_results[str_path] = (success, url)

        successful = sum(1 for result, _ in upload_results.values() if result)
        self.logger.info(f"Загрузка завершена. Успешно: {successful}/{len(upload_results)}")

        return upload_results


# Добавим функцию для удаления всех возможных прокси-настроек из окружения
def clear_proxy_environment():
    """Удаляет все переменные окружения, связанные с прокси"""
    proxy_vars = [
        'HTTP_PROXY', 'http_proxy',
        'HTTPS_PROXY', 'https_proxy',
        'NO_PROXY', 'no_proxy'
    ]
    for var in proxy_vars:
        if var in os.environ:
            del os.environ[var]
    return "Прокси-настройки удалены из переменных окружения"