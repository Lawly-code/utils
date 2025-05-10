import csv
from datetime import datetime
from os import getenv

from dotenv import load_dotenv

from template_upload.s3_docs import DocxS3Uploader, clear_proxy_environment

load_dotenv()

import asyncio


async def main():
    # Очищаем прокси-настройки из окружения
    print(clear_proxy_environment())

    # Создаем экземпляр загрузчика с вашими данными
    uploader = DocxS3Uploader(
        aws_access_key_id=getenv("S3_ACCESS_KEY_ID"),
        aws_secret_access_key=getenv("S3_SECRET_ACCESS_KEY"),
        bucket_name=getenv("S3_BUCKET_NAME"),
        endpoint_url=getenv("S3_ENDPOINT"),
    )

    # Проверяем соединение, получая список бакетов
    buckets = await uploader.list_buckets()
    if not buckets:
        print("Не удалось подключиться к S3 сервису. Проверьте учетные данные и endpoint.")
        return

    print(f"Доступные бакеты: {buckets}")

    # Загружаем все docx файлы из папки
    results = await uploader.upload_folder(
        folder_path="templates",  # Укажите путь к вашей папке с docx файлами
        prefix=""  # Опциональный префикс для имен файлов в S3
    )

    csv_filename = f"upload_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    # Выводим результаты
    with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['имя_файла', 'ссылка']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=';')
        writer.writeheader()
        for file, (success, url) in results.items():
            file = file.split("/")[-1]
            status = "Успешно" if success else "Ошибка"
            if url:
                print(f"{file}: {status} - Ссылка: {url}")
                writer.writerow({'имя_файла': file, 'ссылка': url})
            else:
                print(f"{file}: не загружен")



if __name__ == "__main__":
    asyncio.run(main())
