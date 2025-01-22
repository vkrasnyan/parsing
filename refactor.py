import os
import re

def replace_in_file(file_path):
    """Заменяет вызовы функций на новую универсальную функцию extract_data."""
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()

    # Замена вызовов safe_get_text
    content = re.sub(
        r'safe_get_text\((.+?),\s*tag=(.+?),\s*class_=(.+?),\s*default=(.+?)\)',
        r'extract_data(\1, tag=\2, class_=\3, default=\4)',
        content
    )

    # Замена вызовов get_text_or_none
    content = re.sub(
        r'get_text_or_none\((.+?)\)',
        r'extract_data(\1)',
        content
    )

    # Записываем изменения обратно в файл
    with open(file_path, 'w', encoding='utf-8') as file:
        file.write(content)

def process_directory(directory_path):
    """Обрабатывает все файлы Python в указанной директории."""
    for root, _, files in os.walk(directory_path):
        for file in files:
            if file.endswith('.py'):  # Только Python файлы
                file_path = os.path.join(root, file)
                print(f"Processing file: {file_path}")
                replace_in_file(file_path)

# Укажите путь к директории с кодом
directory_path = "."
process_directory(directory_path)
