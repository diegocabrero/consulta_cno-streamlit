import streamlit as st
import pandas as pd
from io import BytesIO
from datetime import date
import zipfile

from google.cloud import bigquery
from google.oauth2 import service_account

# -------------------------------------------------------
# Configuração básica da página
# -------------------------------------------------------
st.set_page_config(
    page_title="Consulta CNO - Base dos Dados",
    layout="wide",
)

# CSS para mudar o fundo das caixas de seleção para #f2f2f2
st.markdown(
    """
    <style>
    /* Aplica o fundo cinza claro nos componentes de select/multiselect */
    div[data-baseweb="select"] > div {
        background-color: #f2f2f2 !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("📊 Consulta CNO (Cadastro Nacional de Obras)")
st.markdown(
    """
Este painel consulta os **microdados do CNO** disponibilizados pela **Base dos Dados**.

Você pode:
- Filtrar por **UF**
- Filtrar por **cidades (municípios)**, com opção de **Selecionar todas**
- Definir **data inicial** e **data final** (campo `data_inicio`)
- Ajustar o **limite de linhas** (ou trazer todos os registros)
- Exportar o resultado para **XLSX, CSV ou ZIP**
"""
)

# -------------------------------------------------------
# Sidebar: configurações de conexão
# -------------------------------------------------------
st.sidebar.header("⚙️ Configurações de conexão")

billing_default = st.secrets.get("BILLING_PROJECT_ID", "")

billing_project_id = st.sidebar.text_input(
    "Billing Project ID (ID do projeto no GCP)",
    value=billing_default,
    help="ID do projeto no Google Cloud usado para faturamento no BigQuery.",
)

st.sidebar.markdown(
    """
💡 Dica: no Streamlit Cloud, salve `BILLING_PROJECT_ID` e `[gcp_service_account]`
em **Secrets** para não precisar digitar aqui.
"""
)

# -------------------------------------------------------
# Criar o cliente BigQuery com as credenciais
# -------------------------------------------------------
def get_bigquery_client(billing_project_id: str) -> bigquery.Client:
    sa_info = st.secrets["gcp_service_account"]  # vem da seção [gcp_service_account]
    creds = service_account.Credentials.from_service_account_info(sa_info)
    return bigquery.Client(
        project=billing_project_id,
        credentials=creds,
    )

# -------------------------------------------------------
# Lista de municípios por UF (sem cache para garantir atualização)
# -------------------------------------------------------
def listar_municipios_por_uf(uf: str, billing_project_id: str):
    """
    Retorna a lista de nomes de municípios para a UF informada.
    """
    if not uf or not billing_project_id:
        return []

    client = get_bigquery_client(billing_project_id)
    sql = f"""
        SELECT DISTINCT nome
        FROM `basedosdados.br_bd_diretorios_brasil.municipio`
        WHERE sigla_uf = '{uf}'
        ORDER BY nome
    """
    df_mun = client.query(sql).to_dataframe()
    return df_mun["nome"].tolist()

# -------------------------------------------------------
# Testar Conexão - Botão “Testar BigQuery (SELECT 1)”
# -------------------------------------------------------
st.sidebar.subheader("🔌 Testar conexão")

if st.sidebar.button("Testar BigQuery (SELECT 1)"):
    try:
        client = get_bigquery_client(billing_project_id)
        test_df = client.query("SELECT 1 AS ok").to_dataframe()
        st.sidebar.success(f"Conexão OK! Resultado: {test_df.iloc[0]['ok']}")
    except Exception as e:
        st.sidebar.error("Erro ao conectar no BigQuery.")
        st.sidebar.write(e)

# -------------------------------------------------------
# Função auxiliar: montar query SQL
# -------------------------------------------------------
def montar_query(
    uf_filtrada=None,
    cidades_nomes=None,
    data_inicio_min=None,
    data_inicio_max=None,
    limite_linhas=None,
):
    filtros = []

    # Filtro de intervalo de datas (data_inicio)
    if data_inicio_min and data_inicio_max:
        filtros.append(
            f"dados.data_inicio BETWEEN DATE '{data_inicio_min}' AND DATE '{data_inicio_max}'"
        )
    elif data_inicio_min:
        filtros.append(f"dados.data_inicio >= DATE '{data_inicio_min}'")
    elif data_inicio_max:
        filtros.append(f"dados.data_inicio <= DATE '{data_inicio_max}'")

    # Filtro de UF
    if uf_filtrada:
        filtros.append(f"dados.sigla_uf = '{uf_filtrada}'")

    # Filtro de cidades (lista de municípios)
    if cidades_nomes:
        nomes_escapados = [c.replace("'", "''") for c in cidades_nomes]
        lista_in = ", ".join(f"'{n}'" for n in nomes_escapados)
        filtros.append(f"diretorio_id_municipio.nome IN ({lista_in})")

    where_clause = ""
    if filtros:
        where_clause = "WHERE " + " AND ".join(filtros)

    # Cláusula LIMIT opcional
    limit_clause = ""
    if limite_linhas is not None and limite_linhas > 0:
        limit_clause = f"LIMIT {limite_linhas}"

    query = f"""
    WITH 
    dicionario_qualificacao_contribuinte AS (
        SELECT
            chave AS chave_qualificacao_contribuinte,
            valor AS descricao_qualificacao_contribuinte
        FROM `basedosdados.br_rf_cno.dicionario`
        WHERE
            nome_coluna = 'qualificacao_contribuinte'
            AND id_tabela = 'vinculos'
    ),
    dicionario_categoria AS (
        SELECT
            chave AS chave_categoria,
            valor AS descricao_categoria
        FROM `basedosdados.br_rf_cno.dicionario`
        WHERE
            nome_coluna = 'categoria'
            AND id_tabela = 'areas'
    ),
    dicionario_destinacao AS (
        SELECT
            chave AS chave_destinacao,
            valor AS descricao_destinacao
        FROM `basedosdados.br_rf_cno.dicionario`
        WHERE
            nome_coluna = 'destinacao'
            AND id_tabela = 'areas'
    ),
    dicionario_tipo_obra AS (
        SELECT
            chave AS chave_tipo_obra,
            valor AS descricao_tipo_obra
        FROM `basedosdados.br_rf_cno.dicionario`
        WHERE
            nome_coluna = 'tipo_obra'
            AND id_tabela = 'areas'
    ),
    dicionario_tipo_area AS (
        SELECT
            chave AS chave_tipo_area,
            valor AS descricao_tipo_area
        FROM `basedosdados.br_rf_cno.dicionario`
        WHERE
            nome_coluna = 'tipo_area'
            AND id_tabela = 'areas'
    ),
    dicionario_tipo_area_complementar AS (
        SELECT
            chave AS chave_tipo_area_complementar,
            valor AS descricao_tipo_area_complementar
        FROM `basedosdados.br_rf_cno.dicionario`
        WHERE
            nome_coluna = 'tipo_area_complementar'
            AND id_tabela = 'areas'
    ),

    -- Enriquecendo vínculos com a descrição da qualificação
    vinculos_enriquecidos AS (
        SELECT
            dados.id_cno,
            STRING_AGG(
                DISTINCT descricao_qualificacao_contribuinte,
                ', '
            ) AS qualificacao_contribuinte
        FROM `basedosdados.br_rf_cno.vinculos` AS dados
        LEFT JOIN dicionario_qualificacao_contribuinte
            ON dados.qualificacao_contribuinte = chave_qualificacao_contribuinte
        GROUP BY dados.id_cno
    ),

    -- Enriquecendo áreas com descrições e agregando por obra
    areas_enriquecidas AS (
        SELECT
            dados.id_cno,
            STRING_AGG(DISTINCT descricao_categoria, ', ') AS categoria,
            STRING_AGG(DISTINCT descricao_destinacao, ', ') AS destinacao,
            STRING_AGG(DISTINCT descricao_tipo_obra, ', ') AS tipo_obra,
            STRING_AGG(DISTINCT descricao_tipo_area, ', ') AS tipo_area,
            STRING_AGG(
                DISTINCT descricao_tipo_area_complementar,
                ', '
            ) AS tipo_area_complementar,
            SUM(dados.metragem) AS metragem_total
        FROM `basedosdados.br_rf_cno.areas` AS dados
        LEFT JOIN dicionario_categoria
            ON dados.categoria = chave_categoria
        LEFT JOIN dicionario_destinacao
            ON dados.destinacao = chave_destinacao
        LEFT JOIN dicionario_tipo_obra
            ON dados.tipo_obra = chave_tipo_obra
        LEFT JOIN dicionario_tipo_area
            ON dados.tipo_area = chave_tipo_area
        LEFT JOIN dicionario_tipo_area_complementar
            ON dados.tipo_area_complementar = chave_tipo_area_complementar
        GROUP BY dados.id_cno
    )

    SELECT
        dados.data_situacao as data_situacao,
        dados.data_inicio as data_inicio,
        dados.sigla_uf AS sigla_uf,
        diretorio_sigla_uf.nome AS sigla_uf_nome,
        dados.id_municipio AS id_municipio,
        diretorio_id_municipio.nome AS id_municipio_nome,
        dados.id_cno AS id_cno,
        dados.nome_empresarial as nome_empresarial,
        dados.area as area,
        dados.unidade_medida as unidade_medida,
        dados.bairro as bairro,
        dados.cep as cep,
        dados.logradouro as logradouro,
        dados.tipo_logradouro as tipo_logradouro,
        dados.numero_logradouro as numero_logradouro,
        dados.complemento as complemento,

        dados.nome_responsavel as nome_responsavel,
        dados.qualificacao_responsavel as qualificacao_responsavel_codigo,

        CASE
            WHEN dados.qualificacao_responsavel = 70  THEN 'Proprietário do imóvel'
            WHEN dados.qualificacao_responsavel = 53  THEN 'Pessoa jurídica construtora'
            WHEN dados.qualificacao_responsavel = 64  THEN 'Incorporador de construção civil'
            WHEN dados.qualificacao_responsavel = 110 THEN 'Construção em nome coletivo'
            WHEN dados.qualificacao_responsavel = 109 THEN 'Consórcio'
            WHEN dados.qualificacao_responsavel = 111 THEN 'Sociedade líder de consórcio'
            WHEN dados.qualificacao_responsavel = 57  THEN 'Dono da obra'
            ELSE NULL
        END AS qualificacao_responsavel,

        -- Novas colunas dos vínculos
        ve.qualificacao_contribuinte,

        -- Novas colunas das áreas
        ae.categoria,
        ae.destinacao,
        ae.tipo_obra,
        ae.tipo_area,
        ae.tipo_area_complementar,
        ae.metragem_total

    FROM `basedosdados.br_rf_cno.microdados` AS dados
    LEFT JOIN (
        SELECT DISTINCT sigla, nome
        FROM `basedosdados.br_bd_diretorios_brasil.uf`
    ) AS diretorio_sigla_uf
        ON dados.sigla_uf = diretorio_sigla_uf.sigla
    LEFT JOIN (
        SELECT DISTINCT id_municipio, nome
        FROM `basedosdados.br_bd_diretorios_brasil.municipio`
    ) AS diretorio_id_municipio
        ON dados.id_municipio = diretorio_id_municipio.id_municipio
    LEFT JOIN vinculos_enriquecidos AS ve
        ON dados.id_cno = ve.id_cno
    LEFT JOIN areas_enriquecidas AS ae
        ON dados.id_cno = ae.id_cno
    {where_clause}
    {limit_clause}
    """
    return query

# -------------------------------------------------------
# Área principal: filtros da consulta
# -------------------------------------------------------
st.subheader("🔎 Filtros da consulta")

with st.form("filtros_cno"):
    # Linha 1: UF, Data inicial, Data final
    col1, col2, col3 = st.columns(3)

    # UF
    with col1:
        uf_opcoes = ["(Todas)"] + [
            "AC", "AL", "AM", "AP", "BA",
            "CE", "DF", "ES", "GO", "MA",
            "MG", "MS", "MT", "PA", "PB",
            "PE", "PI", "PR", "RJ", "RN",
            "RO", "RR", "RS", "SC", "SE",
            "SP", "TO",
        ]
        uf_escolhida = st.selectbox("UF", uf_opcoes, index=uf_opcoes.index("PR"))
        uf_filtrada = None if uf_escolhida == "(Todas)" else uf_escolhida

    # Data inicial
    with col2:
        data_inicial_default = date(2023, 5, 16)
        data_inicio_min = st.date_input(
            "Data inicial (data_inicio)",
            value=data_inicial_default,
            format="YYYY-MM-DD",
        )

    # Data final
    with col3:
        data_final_default = date(2025, 5, 16)
        data_inicio_max = st.date_input(
            "Data final (data_inicio)",
            value=data_final_default,
            format="YYYY-MM-DD",
        )

    # Linha 2: Cidades e Limite (mesma altura)
    col4, col5 = st.columns(2)

    cidades_selecionadas = None
    with col4:
        if uf_filtrada and billing_project_id:
            municipios = listar_municipios_por_uf(uf_filtrada, billing_project_id)
            if municipios:
                cidades_selecionadas = st.multiselect(
                    "Cidades (municípios)",
                    options=municipios,
                    default=[],
                    placeholder="Digite para pesquisar cidades...",
                    help="Selecione uma ou mais cidades. Se nenhuma for selecionada e a opção abaixo estiver marcada, todas serão consideradas.",
                )
            else:
                st.write("Nenhum município encontrado para essa UF.")
                cidades_selecionadas = None
        else:
            st.write(
                "Selecione uma UF (≠ '(Todas)') e informe o Billing Project ID para habilitar o filtro de cidades."
            )
            cidades_selecionadas = None

    with col5:
        trazer_todos = st.checkbox(
            "Trazer todos os registros (sem limite)",
            value=False,
            help="Use com cuidado: pode trazer muitos registros e aumentar o custo no BigQuery.",
        )
        if not trazer_todos:
            limite_linhas = st.number_input(
                "Limite de linhas",
                min_value=1,
                max_value=500_000,
                value=100_000,
                step=10_000,
                help="Quanto maior, mais dados e mais custo de processamento no BigQuery.",
            )
        else:
            limite_linhas = None

    # Checkbox abaixo da linha, não afeta a altura dos campos
    selecionar_todas = st.checkbox(
        "Selecionar todas as cidades",
        value=True,
        help="Quando marcado, **não** será aplicado filtro de cidade (todas as cidades da UF serão consideradas).",
    )

    # Lógica: se selecionar todas → ignorar filtro de cidade
    if selecionar_todas:
        cidades_selecionadas = None
    else:
        if cidades_selecionadas is not None and len(cidades_selecionadas) == 0:
            # Nenhuma cidade marcada = todas
            cidades_selecionadas = None

    # Nome base do arquivo
    nome_arquivo_base = st.text_input(
        "Nome base dos arquivos",
        value="cno_consulta",
        help="Será usado como base para XLSX, CSV e ZIP.",
    )

    executar = st.form_submit_button("▶️ Executar consulta")

# -------------------------------------------------------
# Execução da consulta
# -------------------------------------------------------
if executar:
    if not billing_project_id:
        st.error("❌ Informe o **Billing Project ID** na barra lateral ou em secrets.")
    else:
        data_inicio_min_str = (
            data_inicio_min.strftime("%Y-%m-%d") if data_inicio_min else None
        )
        data_inicio_max_str = (
            data_inicio_max.strftime("%Y-%m-%d") if data_inicio_max else None
        )

        limite_param = int(limite_linhas) if limite_linhas is not None else None

        sql = montar_query(
            uf_filtrada=uf_filtrada,
            cidades_nomes=cidades_selecionadas,
            data_inicio_min=data_inicio_min_str,
            data_inicio_max=data_inicio_max_str,
            limite_linhas=limite_param,
        )

        with st.expander("👀 Ver SQL gerada"):
            st.code(sql, language="sql")

        st.info("Executando consulta no BigQuery (Base dos Dados)...")

        try:
            with st.spinner("Consultando dados..."):
                client = get_bigquery_client(billing_project_id)
                query_job = client.query(sql)
                df = query_job.to_dataframe()

            # Remove duplicidades – 1 linha por id_cno
            if "id_cno" in df.columns:
                df = df.drop_duplicates(subset=["id_cno"])
            else:
                df = df.drop_duplicates()

            st.success(f"Consulta concluída! Obras retornadas (id_cno únicos): {len(df)}")

            if df.empty:
                st.warning("Nenhum dado encontrado para os filtros selecionados.")
            else:
                # Gráfico de barras: obras por mês (data_inicio)
                if "data_inicio" in df.columns:
                    df_graf = df.copy()
                    df_graf["data_inicio"] = pd.to_datetime(
                        df_graf["data_inicio"], errors="coerce"
                    )
                    tmp = df_graf[df_graf["data_inicio"].notna()].copy()
                    tmp["mes"] = tmp["data_inicio"].dt.to_period("M").astype(str)
                    grp = (
                        tmp.groupby("mes")
                        .size()
                        .reset_index(name="quantidade_obras")
                        .sort_values("mes")
                    )

                    if not grp.empty:
                        st.subheader("📈 Quantidade de obras por mês (data_inicio)")
                        st.bar_chart(
                            grp.set_index("mes")["quantidade_obras"]
                        )

                st.subheader("📋 Amostra dos dados")
                st.dataframe(df.head(100))
                st.markdown(f"**Total de obras (id_cno únicos):** {len(df)}")

                # ------------------------------
                # Geração dos arquivos em memória
                # ------------------------------
                # XLSX
                buffer_xlsx = BytesIO()
                df.to_excel(buffer_xlsx, index=False)
                buffer_xlsx.seek(0)

                # CSV (download separado)
                csv_bytes = df.to_csv(
                    index=False, sep=";", encoding="utf-8-sig"
                ).encode("utf-8-sig")

                # ZIP (contendo apenas XLSX)
                buffer_zip = BytesIO()
                with zipfile.ZipFile(buffer_zip, "w", zipfile.ZIP_DEFLATED) as zf:
                    zf.writestr(
                        f"{nome_arquivo_base}.xlsx",
                        buffer_xlsx.getvalue(),
                    )
                buffer_zip.seek(0)

                # ------------------------------
                # Botões de download
                # ------------------------------
                col_a, col_b, col_c = st.columns(3)

                with col_a:
                    st.download_button(
                        label="💾 Baixar XLSX",
                        data=buffer_xlsx,
                        file_name=f"{nome_arquivo_base}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    )

                with col_b:
                    st.download_button(
                        label="📄 Baixar CSV",
                        data=csv_bytes,
                        file_name=f"{nome_arquivo_base}.csv",
                        mime="text/csv",
                    )

                with col_c:
                    st.download_button(
                        label="📦 Baixar ZIP (XLSX)",
                        data=buffer_zip,
                        file_name=f"{nome_arquivo_base}.zip",
                        mime="application/zip",
                    )

        except Exception as e:
            st.error("❌ Ocorreu um erro ao executar a consulta.")
            st.exception(e)
