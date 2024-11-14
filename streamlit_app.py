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
from stqdm import stqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from streamlit_echarts import st_echarts
import numpy as np
from streamlit_extras.metric_cards import style_metric_cards
import statistics

sys.path.insert(0, os.path.abspath(os.curdir))

# st.set_page_config(layout="wide")


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
                "Authorization": f"Bearer APP_USR-8541254073775405-111312-276bf3b05e77acaf6cff6b04daec06f2-1023158880",
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
def get_last_months_intervals():
    intervals = []
    current_month = datetime.now().replace(day=1)

    for i in range(8):
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


def preencher_visitas(df, colunas_visitas):
    """
    Preenche valores nulos representados como 0 nas colunas de visitas mensais
    usando interpolação linear e preenchimento nas extremidades.

    Parâmetros:
    - df: DataFrame contendo as colunas de visitas mensais.
    - colunas_visitas: Lista de colunas de visitas mensais a serem tratadas.

    Retorno:
    - DataFrame com valores preenchidos nas colunas especificadas.
    """
    # Substituir valores 0 por NaN nas colunas de visitas
    df[colunas_visitas] = df[colunas_visitas].replace(0, np.nan)

    # Interpolação linear ao longo das colunas (horizontalmente)
    df[colunas_visitas] = df[colunas_visitas].interpolate(method="linear", axis=1)

    # Preenchimento nas extremidades (backfill e forward fill)
    df[colunas_visitas] = df[colunas_visitas].bfill(axis=1).ffill(axis=1)

    df[colunas_visitas] = df[colunas_visitas].round(0)

    return df


def requisitando_codigo_categoria(query):
    url = f"https://api.mercadolibre.com/sites/MLB/domain_discovery/search?q={query}"
    response = api_mercado_livre().get(url)

    return {item["domain_name"]: item["category_id"] for item in response}


def requisitar_relatorio(categoria):
    # Calcula os intervalos de datas apenas uma vez fora do loop
    intervals = get_last_months_intervals()

    # Pré-carrega os anúncios e fornecedores em paralelo
    lista = []
    scroll_id = ""
    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(requisitando_lista, categoria, scroll_id)
            for _ in range(1)  # colocar as 5 primeiras páginas
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
                        "nome_seller": requisitando_fornecedor(id_fornecedor)[
                            "nickname"
                        ],
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
        for future in stqdm(as_completed(futures), total=len(lista)):
            relatorio.append(future.result())

    return pd.DataFrame(relatorio)


def options_pie(name, data):
    return {
        "tooltip": {"trigger": "item"},
        "series": [
            {
                "name": f"{name}",
                "type": "pie",
                "radius": ["40%", "70%"],
                "avoidLabelOverlap": False,
                "itemStyle": {
                    "borderRadius": 10,
                    "borderColor": "#fff",
                },
                "label": {"show": False, "position": "center"},
                "emphasis": {
                    "label": {"show": True, "fontSize": "10", "fontWeight": "bold"}
                },
                "labelLine": {"show": False},
                "data": data,
            }
        ],
    }


# Título da aplicação
st.title("Pesquisa de Mercado")


if "pesquisar_categoria" not in st.session_state:
    st.session_state.pesquisar_categoria = False

if "pesquisar_metricas" not in st.session_state:
    st.session_state.pesquisar_metricas = False

with st.form("Pesquise a categoria"):
    requisicao = st.text_input("Pesquise a categoria")

    if st.form_submit_button("Pesquisar categoria"):
        if requisicao != "":
            st.session_state.pesquisar_categoria = requisicao
        else:
            st.warning("Por favor, insira o nome da coluna.")

if st.session_state.pesquisar_categoria and "categorias" not in st.session_state:
    try:
        st.session_state.categorias = requisitando_codigo_categoria(
            st.session_state.pesquisar_categoria
        )
        st.session_state.pesquisar_categoria = False
    except Exception as e:
        st.warning("Algo deu errado... entre em contato com o suporte.")

if "categorias" in st.session_state:
    categoria_selecionada = st.selectbox(
        "Qual categoria você deseja?",
        tuple(st.session_state.categorias.keys()),
        placeholder="Selecione uma categoria",
    )

    if st.button("Pesquisar métricas"):
        if categoria_selecionada:
            st.session_state.pesquisar_metricas = True
        else:
            st.warning("Por favor, selecione uma categoria")


if st.session_state.pesquisar_metricas:
    st.session_state.df = requisitar_relatorio(
        st.session_state.categorias[categoria_selecionada]
    )
    st.session_state.pesquisar_metricas = False

if "df" in st.session_state:
    df = st.session_state.df

    colunas_visitas = [
        "visita_mes_1",
        "visita_mes_2",
        "visita_mes_3",
        "visita_mes_4",
        "visita_mes_5",
        "visita_mes_6",
        "visita_mes_7",
        "visita_mes_8",
    ]

    df = preencher_visitas(df, colunas_visitas)

    col1, col2 = st.columns(2)

    col1.metric(label="preço médio", value=f"R$ {round(df["preco"].mean(),2)}")
    col2.metric(label="variação de preço", value=f"R$ {round(df['preco'].std(),2)}")

    style_metric_cards(background_color="#262730")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("Anúncios no catalogo")

        st_echarts(
            options=options_pie(
                "catálogo",
                [
                    {"name": name, "value": count}
                    for name, count in (
                        df["catalogo"].value_counts(normalize=True) * 100
                    )
                    .round()
                    .items()
                ],
            )
        )

    with col2:
        st.markdown("Tipos logísticos")

        nome_tipo_logistico = {
            "xd_drop_off": "Coleta",
            "cross_docking": "Cross docking",
            "drop_off": "Ponto de Coleta",
            "not_specified": "Não especificado",
            "custom": "Customizado",
        }

        df["tipo_logistico"] = df["tipo_logistico"].replace(nome_tipo_logistico)

        st_echarts(
            options=options_pie(
                "tipo logistico",
                [
                    {"name": name, "value": count}
                    for name, count in (
                        df["tipo_logistico"].value_counts(normalize=True) * 100
                    )
                    .round()
                    .items()
                ],
            )
        )

    with col3:
        st.markdown("Tipos de anúncios")

        nome_tipo_anuncio = {
            "gold_special": "classico",
            "gold_pro": "premium",
        }

        df["tipo_anuncio"] = df["tipo_anuncio"].replace(nome_tipo_anuncio)

        st_echarts(
            options=options_pie(
                "tipo anúncio",
                [
                    {"name": name, "value": count}
                    for name, count in (
                        df["tipo_anuncio"].value_counts(normalize=True) * 100
                    )
                    .round()
                    .items()
                ],
            )
        )

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("Niveis dos vendedores")

        nome_nivel_vendedores = {
            "5_green": "5 estrelas",
            "4_light_green": "4 estrelas",
            "3_yellow": "3 estrelas",
            "2_light_green": "novo_nome4",
            "1_green": "novo_nome5",
            "0_green": "novo_nome6",
        }

        df["nivel_seller"] = df["nivel_seller"].replace(nome_nivel_vendedores)

        st_echarts(
            options=options_pie(
                "nivel seller",
                [
                    {"name": name, "value": count}
                    for name, count in (
                        df["nivel_seller"].value_counts(normalize=True) * 100
                    )
                    .round()
                    .items()
                ],
            )
        )

    with col2:
        st.markdown("Reputação dos sellers")
        st_echarts(
            options=options_pie(
                "reputação seller",
                [
                    {"name": name, "value": count}
                    for name, count in (
                        df["reputacao_seller"].value_counts(normalize=True) * 100
                    )
                    .round()
                    .items()
                ],
            )
        )

    st.markdown("### Média das visitas nos ultimos 8 meses")

    options = {
        "title": {"text": ""},
        "tooltip": {
            "trigger": "axis",
            "axisPointer": {
                "type": "cross",
                "label": {"backgroundColor": "#6a7985"},
            },
        },
        "legend": {"data": ["visitas"]},
        "toolbox": {"feature": {"saveAsImage": {}}},
        "grid": {"left": "3%", "right": "4%", "bottom": "3%", "containLabel": True},
        "xAxis": [
            {
                "type": "category",
                "boundaryGap": False,
                "data": [
                    "mes 1",
                    "mes 2",
                    "mes 3",
                    "mes 4",
                    "mes 5",
                    "mes 6",
                    "mes 7",
                    "mes 8",
                ],
            }
        ],
        "yAxis": [{"type": "value"}],
        "series": [
            {
                "name": "Visitas",
                "type": "line",
                "stack": "Visitas",
                "areaStyle": {},
                "emphasis": {"focus": "series"},
                "data": df[
                    [
                        "visita_mes_1",
                        "visita_mes_2",
                        "visita_mes_3",
                        "visita_mes_4",
                        "visita_mes_5",
                        "visita_mes_6",
                        "visita_mes_7",
                        "visita_mes_8",
                    ]
                ]
                .mean(axis=0)
                .round(2)
                .to_list(),
            }
        ],
    }

    st_echarts(options=options)

    media_visitas = round(
        statistics.mean(
            df[
                [
                    "visita_mes_1",
                    "visita_mes_2",
                    "visita_mes_3",
                    "visita_mes_4",
                    "visita_mes_5",
                    "visita_mes_6",
                    "visita_mes_7",
                    "visita_mes_8",
                ]
            ]
            .mean(axis=0)
            .to_list()
        ),
        2,
    )

    st.markdown("## Simule suas vendas")

    custo_unitario = st.number_input(
        "Custo unitário do produto", min_value=0.0, step=1.00, format="%.2f"
    )

    pedido_minimo = st.number_input("Pedido mínimo de compra", min_value=1, step=1)

    porcentagem_custo = st.number_input(
        "Percentual de custo", min_value=0.0, step=1.00, format="%.2f"
    )

    if st.button("Simular", type="primary"):
        col1, col2, col3 = st.columns(3)

        preco_medio = round(df["preco"].mean(), 2)
        rs_estoque = round(custo_unitario * pedido_minimo, 2)
        projecao_vendas_qtd = round((media_visitas * 0.05))
        projecao_vendas_rs = projecao_vendas_qtd * preco_medio
        retorno = (
            preco_medio
            - (preco_medio * 0.12)
            - 6
            - (preco_medio * (porcentagem_custo / 100))
        ) * pedido_minimo

        col1.metric(
            label="R$ em estoque",
            value=f"R$ {rs_estoque}",
        )
        col2.metric(label="Projeção vendas qtd (mês)", value=f"{projecao_vendas_qtd}")
        col3.metric(
            label="Projeção vendas R$ (mês)",
            value=f"R$ {projecao_vendas_rs}",
        )

        col1, col2, col3 = st.columns(3)

        col1.metric(
            label="Projeção retorno R$",
            value=f"R$ {round(retorno - rs_estoque, 2)}",
        )

        col2.metric(
            label="Dias de estoque",
            value=f"{round(rs_estoque / (projecao_vendas_rs/30))}",
        )

        col3.metric(
            label="Projeção rendimento",
            value=f"{round(((projecao_vendas_qtd * (
                preco_medio
                - (preco_medio * 0.12)
                - 6
                - (preco_medio * (porcentagem_custo / 100))
            ) ) / rs_estoque) *100, 2)} %",
        )

    # style_metric_cards(background_color="#262730")


# arredonadar os dados das visitas
# mudar os nomes dos graficos de rosca
# mudar para porcentagem os graficos de rosca
