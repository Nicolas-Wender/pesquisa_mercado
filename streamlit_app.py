import streamlit as st
from google.cloud import bigquery
import pandas as pd
import sys, os
from cryptography.fernet import Fernet
from datetime import date, datetime, timedelta
from time import sleep
import requests
from dateutil.relativedelta import relativedelta
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.abspath(os.curdir))


# Bigquery
def send_bigquery(df: pd.DataFrame, table_id: str, schema: list):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
        "./integracao-414415-1335b09dae0f.json"
    )

    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_TRUNCATE",
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    table = client.get_table(table_id)
    print(
        "Loaded {} rows and {} columns to {} in {}".format(
            table.num_rows, len(table.schema), table_id, datetime.now()
        )
    )

    print("Send to biquery !")


def query_bigquery(sql_query: str):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
        "./integracao-414415-1335b09dae0f.json"
    )

    client = bigquery.Client()
    query_job = client.query(sql_query)

    return query_job.result()


# Criptografia
def encrypt_password(password, key):
    fernet = Fernet(key)
    encrypted_password = fernet.encrypt(password.encode())
    return encrypted_password


def decrypt_password(encrypted_password, key):
    fernet = Fernet(key)
    decrypted_password = fernet.decrypt(encrypted_password).decode()
    return decrypted_password


# Requisição Mercado Livre
class api_mercado_livre:
    def __init__(self):
        self.cache = {}
        self._401_count = 0

    def get(self, url, loja: str = ""):
        try:
            titulo_loja = "".join(loja.split()).upper()
            # acess_token = self._access_token(titulo_loja)

            headers = {
                "Accept": "application/json",
                "Authorization": f"Bearer APP_USR-8541254073775405-110608-5dd6f9fb5f8a5851f7d04d87e89e23fb-1023158880",
            }

            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 401:
                if self._401_count >= 2:
                    response.raise_for_status()

                self._401_count += 1
                self.cache.clear()
                return self.get(url, loja)
            elif response.status_code == 429:
                sleep(2)
                return self.get(url, loja)
            else:
                response.raise_for_status()
        except Exception as e:
            print("Ocorreu um erro:", e)
            return "error"

    def _oauth_refresh(self, df_credenciais: pd.DataFrame, loja: str) -> str:
        chave_criptografia = str(os.environ.get(f"CHAVE_CRIPTOGRAFIA")).encode()
        mercado_livre_client_id = os.environ.get(f"CLIENT_ID_MERCADO_LIVRE_{loja}")
        mercado_livre_client_secret = os.environ.get(f"SECRET_KEY_MERCADO_LIVRE_{loja}")

        refresh_token = decrypt_password(
            str(
                df_credenciais["valor"]
                .loc[
                    (df_credenciais["titulo"] == "refresh_token")
                    & (df_credenciais["loja"] == f"MERCADO_LIVRE_{loja}")
                ]
                .values[0]
            ),
            chave_criptografia,
        )

        header = {
            "accept": "application/json",
            "content-type": "application/x-www-form-urlencoded",
        }

        payload = {
            "grant_type": "refresh_token",
            "client_id": mercado_livre_client_id,
            "client_secret": mercado_livre_client_secret,
            "refresh_token": refresh_token,
        }

        api = requests.post(
            "https://api.mercadolibre.com/oauth/token",
            data=payload,
            headers=header,
        )

        situationStatusCode = api.status_code
        api = api.json()

        if situationStatusCode == 400:
            print(f"Request failed. code: {situationStatusCode}")
            print(api)

        df_credenciais.loc[
            (df_credenciais["loja"] == f"MERCADO_LIVRE_{loja}")
            & (df_credenciais["titulo"] == "access_token"),
            "valor",
        ] = encrypt_password(api["access_token"], chave_criptografia)

        df_credenciais.loc[
            (df_credenciais["loja"] == f"MERCADO_LIVRE_{loja}")
            & (df_credenciais["titulo"] == "access_token"),
            "validade",
        ] = str(datetime.now())

        df_credenciais.loc[
            (df_credenciais["loja"] == f"MERCADO_LIVRE_{loja}")
            & (df_credenciais["titulo"] == "refresh_token"),
            "valor",
        ] = encrypt_password(api["refresh_token"], chave_criptografia)

        df_credenciais.loc[
            (df_credenciais["loja"] == f"MERCADO_LIVRE_{loja}")
            & (df_credenciais["titulo"] == "refresh_token"),
            "validade",
        ] = str(datetime.now())

        schema = [
            bigquery.SchemaField("loja", "STRING"),
            bigquery.SchemaField("titulo", "STRING"),
            bigquery.SchemaField("validade", "STRING"),
            bigquery.SchemaField("valor", "STRING"),
        ]
        table_id = f"integracao-414415.data_ptl.credenciais"

        send_bigquery(df_credenciais, table_id, schema)

        return api["access_token"]

    def _validade_access_token(self, df_credenciais: pd.DataFrame, loja: str) -> str:
        data_atualizacao = datetime.strptime(
            df_credenciais["validade"]
            .loc[
                (df_credenciais["titulo"] == "access_token")
                & (df_credenciais["loja"] == f"MERCADO_LIVRE_{loja}")
            ]
            .values[0],
            "%Y-%m-%d %H:%M:%S.%f",
        )

        data_limite = data_atualizacao + timedelta(hours=6)

        if datetime.now() > data_limite or self._401_count >= 2:
            return self._oauth_refresh(df_credenciais, loja)

        return decrypt_password(
            df_credenciais["valor"]
            .loc[
                (df_credenciais["titulo"] == "access_token")
                & (df_credenciais["loja"] == f"MERCADO_LIVRE_{loja}")
            ]
            .values[0],
            str(os.environ.get(f"CHAVE_CRIPTOGRAFIA")).encode(),
        )

    def _access_token(self, loja: str) -> str:
        if loja in self.cache:
            return self.cache[loja]

        results_query_credenciais = query_bigquery(
            "SELECT * FROM `integracao-414415.data_ptl.credenciais`"
        )

        df_credenciais = pd.DataFrame(
            data=[row.values() for row in results_query_credenciais],
            columns=[field.name for field in results_query_credenciais.schema],
        )

        self.cache[loja] = self._validade_access_token(df_credenciais, loja)

        return self.cache[loja]


# Main
def get_last_six_months_intervals():
    intervals = []
    current_month = datetime.now().replace(day=1)

    for i in range(6):
        first_day = current_month - relativedelta(months=1)
        last_day = current_month - relativedelta(days=1)
        date_from = first_day.strftime("%Y-%m-%dT00:00:00.000-00:00")
        date_to = last_day.strftime("%Y-%m-%dT00:00:00.000-00:00")
        intervals.append(f"date_from={date_from}&date_to={date_to}")
        current_month = first_day

    return intervals


def requisitando_visitas_anuncios(id_anuncio, intervals):
    urls = [
        f"https://api.mercadolibre.com/items/visits?ids={id_anuncio}&{interval}"
        for interval in intervals
    ]

    with ThreadPoolExecutor() as executor:
        future_to_month = {
            executor.submit(api_mercado_livre().get, url): idx
            for idx, url in enumerate(urls)
        }
        visitas = {}
        for future in as_completed(future_to_month):
            month = future_to_month[future]
            visitas[f"visita_mes_{month + 1}"] = future.result()[0]["total_visits"]

    return visitas


def requisitando_lista(category, scroll_id: str = ""):
    return api_mercado_livre().get(
        f"https://api.mercadolibre.com/sites/MLB/search?category={category}&sort=sold_quantity_desc{'scroll_id=' + scroll_id if scroll_id else ''}"
    )


def requisitando_qualidades_anuncios(id_anuncio):
    return (
        api_mercado_livre()
        .get(f"https://api.mercadolibre.com/items?ids={id_anuncio}&attributes=health")[
            0
        ]
        .get("body", {})
        .get("health", "")
    )


def requisitando_fornecedor(id_fornecedor):
    return api_mercado_livre().get(
        f"https://api.mercadolibre.com/users/{id_fornecedor}"
    )


# Calcula os intervalos de datas apenas uma vez fora do loop
intervals = get_last_six_months_intervals()

# Pré-carrega os anúncios e fornecedores em paralelo
lista = []
scroll_id = ""
with ThreadPoolExecutor() as executor:
    futures = [
        executor.submit(requisitando_lista, "MLB7073", scroll_id) for _ in range(10)
    ]
    for future in as_completed(futures):
        response = future.result()
        lista.extend(response.get("results", []))
        scroll_id = response.get("scroll_id", "")

# Coleta as informações de cada anúncio em paralelo
relatorio = []
with ThreadPoolExecutor() as executor:
    futures = []
    for anuncio in lista:
        id_anuncio = anuncio.get("id", "")
        id_fornecedor = anuncio.get("seller", {}).get("id", "")

        # Paraleliza as requisições de visitas e fornecedor
        futures.append(
            executor.submit(
                lambda anuncio, id_anuncio, id_fornecedor: {
                    "id": id_anuncio,
                    "preco": anuncio["price"],
                    "ean": str(
                        next(
                            (
                                attribute["source"]
                                for attribute in anuncio["attributes"]
                                if attribute["id"] == "GTIN"
                            ),
                            None,
                        )
                    ),
                    "catalogo": anuncio["catalog_listing"],
                    "qualidade": requisitando_qualidades_anuncios(id_anuncio),
                    "tipo_logistico": anuncio["shipping"]["logistic_type"],
                    "tipo_anuncio": anuncio["listing_type_id"],
                    "id_seller": id_fornecedor,
                    "nome_seller": requisitando_fornecedor(id_fornecedor)["nickname"],
                    "nivel_seller": requisitando_fornecedor(id_fornecedor)[
                        "seller_reputation"
                    ]["power_seller_status"],
                    "reputacao_seller": requisitando_fornecedor(id_fornecedor)[
                        "seller_reputation"
                    ]["level_id"],
                    **requisitando_visitas_anuncios(id_anuncio, intervals),
                },
                anuncio,
                id_anuncio,
                id_fornecedor,
            )
        )

    # Processa o relatório à medida que as requisições terminam
    for future in tqdm(as_completed(futures), total=len(lista)):
        relatorio.append(future.result())
