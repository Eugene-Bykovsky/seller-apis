import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Получить список товаров магазина Ozon.

    Args:
        last_id (str): Последний идентификатор товара.
        client_id (str): Идентификатор клиента.
        seller_token (str): Токен для доступа к API продавца.

    Returns:
        dict: Словарь с результатами запроса.

    Examples:
        Корректное использование:
        >>> get_product_list("3", "12345", "asda23sad")
        {'items': [...], 'total': 100}

        Некорректное использование:
        >>> get_product_list("3", "12345", "invalid_seller_token")
        Traceback (most recent call last):
            ...
        requests.exceptions.HTTPError: 401 Client Error: Unauthorized
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Получить артикулы товаров магазина Ozon.

    Args:
        client_id (str): Идентификатор клиента.
        seller_token (str): Токен для доступа к API продавца.

    Returns:
        list: Список артикулов товаров.

    Examples:
        Корректное использование:
        >>> get_offer_ids("12345", "valid_seller_token")
        ['offer1', 'offer2', ...]

        Некорректное использование:
        >>> get_offer_ids("12345", "invalid_seller_token")
        Traceback (most recent call last):
            ...
        requests.exceptions.HTTPError: 401 Client Error: Unauthorized
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Обновить цены товаров.

    Args:
        prices (list): Список цен, которые нужно обновить.
        client_id (str): Идентификатор клиента.
        seller_token (ster): Токен для доступа к API продавца.

    Returns:
        dict: Результат запроса обновления цен.

    Examples:
        Корректное использование:
        >>> update_price([{'offer_id': '123', 'price': '5990'}], "12345",
                        "valid_seller_token")
        {'status': 'success'}

        Некорректное использование:
        >>> update_price([], "12345", "invalid_seller_token")
        Traceback (most recent call last):
            ...
        requests.exceptions.HTTPError: 401 Client Error: Unauthorized
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Обновить остатки товаров.

    Args:
        stocks (list): Список остатков для обновления.
        client_id: Идентификатор клиента.
        seller_token: Токен для доступа к API продавца.

    Returns:
        dict: Результат запроса обновления остатков.

    Examples:
        Корректное использование:
        >>> update_stocks([{'offer_id': '123', 'stock': 10}], "12345",
                         "valid_seller_token")
        {'status': 'success'}

        Некорректное использование:
        >>> update_stocks([], "12345", "invalid_seller_token")
        Traceback (most recent call last):
            ...
        requests.exceptions.HTTPError: 401 Client Error: Unauthorized
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Скачать файл остатков с сайта Casio и преобразовать в список.

    Returns:
        list: Список остатков товаров.

    Examples:
        Корректное использование:
        >>> download_stock()
        [{'Код': '123', 'Количество': '10', 'Цена': '5990'}, ...]

        Некорректное использование:
        >>> download_stock()
        Traceback (most recent call last):
            ...
        requests.exceptions.HTTPError: 404 Client Error: Not Found
    """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """Создать список остатков на основе загруженных данных.

    Args:
        watch_remnants (list): Список остатков, загруженных из файла.
        offer_ids (list): Список артикулов, загруженных в Ozon.

    Returns:
        list: Список остатков для обновления.

    Examples:
        Корректное использование:
        >>> create_stocks(watch_remnants, ['offer1', 'offer2'])
        [{'offer_id': 'offer1', 'stock': 10}, ...]

        Некорректное использование:
        >>> create_stocks(None, ['offer1', 'offer2'])
        Traceback (most recent call last):
            ...
        AttributeError: 'NoneType' object has no attribute 'get'
    """
    # Уберем то, что не загружено в seller
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Создать список цен на основе загруженных данных и артикулов.

    Args:
        watch_remnants (list): Список остатков, загруженных из файла.
        offer_ids (list): Список артикулов, загруженных в Ozon.

    Returns:
        list: Список цен для обновления

    Examples:
        Корректное использование:
        >>> create_prices(watch_remnants, ['offer1', 'offer2'])
        [{'offer_id': 'offer1', 'price': '5990'}, ...]

        Некорректное использование:
        >>> create_prices(None, ['offer1', 'offer2'])
        Traceback (most recent call last):
            ...
        AttributeError: 'NoneType' object has no attribute 'get'
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


# Преобразует строку с нечисловыми символами в числовую строку. Строка с
# ценой, которая может содержать нечисловые символы, пробелы или разделители
# разрядов.

def price_conversion(price: str) -> str:
    """Преобразует строку с нечисловыми символами в числовую строку.

    Args:
        price (str): Строка с ценой, которая может содержать нечисловые
        символы, пробелы или разделители разрядов.

    Returns:
        str: Строка, содержащая только числовые символы.

    Examples:
        Корректное использование:
        >>> price_conversion("5'990.00 руб.")
        '5990'

        Некорректное использование:
        >>> price_conversion(None)
        Traceback (most recent call last):
            ...
        AttributeError: 'NoneType' object has no attribute 'split'
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разделить список lst на части по n элементов.

    Args:
        lst (list): Исходный список.
        n (int): Размер каждой части.

    Yields:
        list: Часть исходного списка.


    Examples:
        Корректное использование:
        >>> list(divide([1, 2, 3, 4, 5], 2))
        [[1, 2], [3, 4], [5]]

        Некорректное использование:
        >>> list(divide("12345", 2))
        Traceback (most recent call last):
            ...
        TypeError: object of type 'str' has no len()
    """
    for i in range(0, len(lst), n):
        yield lst[i: i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """Загрузить цены товаров в Ozon.

    Args:
        watch_remnants: Список остатков, загруженных из файла.
        client_id: Идентификатор клиента.
        seller_token: Токен для доступа к API продавца.

    Returns:
        list: Список всех обновленных цен.

    Examples:
        Корректное использование:
        >>> await upload_prices(watch_remnants, "12345", "valid_seller_token")
        [{'offer_id': 'offer1', 'price': '5990'}, ...]

        Некорректное использование:
        >>> await upload_prices(None, "12345", "invalid_seller_token")
        Traceback (most recent call last):
            ...
        TypeError: 'NoneType' object has no attribute 'get'
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """Загрузить остатки товаров в Ozon.

    Args:
        watch_remnants: Список остатков, загруженных из файла.
        client_id: Идентификатор клиента.
        seller_token: Токен для доступа к API продавца.

    Returns:
        tuple: Кортеж с двумя списками:
            - Список товаров с ненулевыми остатками.
            - Полный список остатков товаров.

    Examples:
        Корректное использование:
        >>> await upload_stocks(watch_remnants, "12345", "valid_seller_token")
        ([{'offer_id': 'offer1', 'stock': 10}, ...], [{'offer_id': 'offer1', 'stock': 10}, ...])

        Некорректное использование:
        >>> await upload_stocks(None, "12345", "invalid_seller_token")
        Traceback (most recent call last):
            ...
        AttributeError: 'NoneType' object has no attribute 'get'
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    """Основная функция для загрузки остатков и цен товаров в Ozon.

    Examples:
        Корректное использование:
        >>> main()

        Некорректное использование:
        >>> main()  # Если отсутствуют переменная окружения
        Traceback (most recent call last):
            ...
        KeyError: 'CLIENT_ID'
    """
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
