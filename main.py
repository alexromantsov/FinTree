import os
import pandas as pd
from sqlalchemy import create_engine
from config import DB_SETTINGS
from migration import create_table


def load_data(filename):
    filepath = os.path.join('', 'data', filename)
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Ошибка: Файл {filename} не найден в папке data.")

    data = pd.read_csv(filepath, delimiter=';', index_col='код')
    data.fillna(0, inplace=True)
    return data


def calc_sums(data):
    # Получаем список колонок, которые содержат года
    year_columns = [col for col in data.columns if col.isnumeric()]

    # Обработка каждой строки
    def inner_calc_sums(row):
        # Проверка на нетерминальную строку
        if row[year_columns].sum() == 0:
            # Находим все дочерние строки для текущей строки
            child_codes = [index for index in data.index if index.startswith(row.name + ".")]
            # Суммируем данные по годам для всех дочерних строк и сохраняем
            row[year_columns] = data.loc[child_codes, year_columns].sum()
        return row

    return data.apply(inner_calc_sums, axis=1)


def save_to_db(data):
    connection_str = f"postgresql+psycopg2://{DB_SETTINGS['user']}:{DB_SETTINGS['password']}@{DB_SETTINGS['host']}:{DB_SETTINGS['port']}/{DB_SETTINGS['dbname']}"
    engine = create_engine(connection_str)

    data.to_sql('projects', engine, if_exists='replace', index_label='code')


def main():
    try:
        print("Загрузка данных...")
        data = load_data('data.csv')
        print("  > Данные успешно загружены.")
    except FileNotFoundError as e:
        print(e)
        return

    print("Вычисление колонок для годов...")
    year_columns = [col for col in data.columns if col.isnumeric()]
    year_columns_ddl = ', '.join([f"year_{year} FLOAT" for year in year_columns])
    print("  > Колонки для годов вычислены.")

    print("Создание таблицы в базе данных...")
    create_table(year_columns_ddl)
    print("  > Таблица в базе данных создана.")

    print("Вычисление сумм для проектов и подпроектов...")
    data = calc_sums(data)
    print("  > Суммы успешно вычислены.")

    print("Сохранение данных в базе данных...")
    save_to_db(data)
    print("  > Данные успешно сохранены в базе данных.")


if __name__ == '__main__':
    main()
