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
    """Получить список товаров магазина озон.

    Аргументы:
        last_id (str): пустая строка для API-запроса, т.к. нужен список всех товаров.
        client_id (str): идентификатор продавца в Озон из окружения.
        seller_token (str): токен для API Озон из окружения.

    Возвращает:
        Декодированный ответ от API со списком товаров магазина Озон.
        В случае некорректного исполнения, Озон вернет статус ответа, отличный от 200,
        и сработает метод .raise_for_status().
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
    """Получить артикулы товаров магазина Озон.

    Аргументы:
        client_id (str): идентификатор продавца в Озон из окружения.
        seller_token (str): токен для API Озон из окружения.

    Возвращает:
        offer_ids (list): список артикулов товаров магазина Озон.
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
    """Обновить цены товаров в Озон.

    Отправляет API-запрос в Озон для обновления цен
    на продаваемые товары.

    Аргументы:
        prices (list): список с ценами товаров, который отправляем.
        client_id (str): идентификатор продавца в Озон из окружения.
        seller_token (str): токен для API Озон из окружения.

    Возвращает:
        Ответ от API в декодированном формате.
        В случае некорректного исполнения, Озон вернет статус ответа, отличный от 200,
        и сработает метод .raise_for_status().
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
    """Обновить остатки товаров в Озон.

    Обновляет информацию о наличии товара в Озон на основании сформированных
    данных об остатках у поставщика.

    Аргументы:
        stocks (list): список с остатками из create_stocks().
        client_id (str): идентификатор продавца в Озон из окружения.
        seller_token (str): токен для API Озон из окружения.

    Возвращает:
        Ответ от API в декодированном формате.
        В случае некорректного исполнения, Озон вернет статус ответа, отличный от 200,
        и сработает метод .raise_for_status().
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
    """Скачать файл ostatki с сайта casio.

    Скачиваем данные о доступных товарах для оптовой закупки у поставщика.
    Данные содержат всю нужную информацию о товаре: код товара, сколько доступно,
    розничная цена.

    Возвращает:
        watch_remnants (list): список часов, доступных для оптовой закупки.
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
    """Сформировать остатки.

    Формирует остатки товаров на основании данных от поставщика.

    Аргументы:
        watch_remnants (list): Список часов, доступных для оптовой закупки.
            Получен из download_stock().
        offer_ids (list): список артикулов товаров магазина Озон.

    Возвращает:
        stocks (list): список с информацией об остатках товаров.
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
    """Создать список с ценами продажи.

    Создает список с ценами продажи часов. В качестве
    цены продажи берется розничная цена товара от поставщика.

    Аргументы:
        watch_remnants (list): Список часов, доступных для оптовой закупки.
            Получен из download_stock().
        offer_ids (list): список артикулов товаров магазина Озон.

    Возвращает:
        prices (list): список с розничными ценами на часы. 
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


def price_conversion(price: str) -> str:
    """Преобразовать цену.

    Аргументы:
        price (str): розничная цена товара из watch_remnants.

    Возвращает:
        Цену в преобразованном формате.

    Пример:
        5'990.00 руб. -> 5990.
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разделить список lst на части по n элементов.

    Аргументы:
        lst (list): список, который нужно разделить.
        n (int): число элементов в одной части от разделенного списка.

    Озон при создании, обновлении товара, обновлении остатков
    позволяет передавать в одном запросе до 100 товаров.
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    """Запуск функций и обработка исключений.

    Запускает необходимые функции для обновления остатков товаров в
    Озон и обновления цен на товары.

    Исключения:
        Обрабатывает 3 типа исключений.
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
