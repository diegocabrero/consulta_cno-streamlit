import os
import json
import tempfile
from io import BytesIO
from datetime import date

import streamlit as st
import pandas as pd
import basedosdados as bd


# -------------------------------------------------------
# Configuração de credenciais via secrets (Streamlit Cloud)
# -------------------------------------------------------
def configurar_credenciais_gcp():
    """
    Lê o JSON da conta de serviço do Streamlit secrets
    e grava em um arquivo temporário, apontando
    GOOGLE_APPLICATION_CREDENTIALS para ele.
    """
    if "GCP_SERVICE_ACCOUNT_JSON" not in st.secrets:
        st.warning(
            "GCP_SERVICE_ACCOUNT_JSON não encontrado em secrets. "
            "Configure as credenciais no painel de Secrets do Streamlit."
        )
        return

    sa_json_str = st.secrets["GCP_SERVICE_ACCOUNT_JSON"]
    # Se vier como dict, converte para string
    if isinstance(sa_json_str, dict):
        sa_json_str = json.dumps(sa_json_str)

    tmp_file = tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json")
    tmp_file.write(sa_json_str)
    tmp_file.flush()
    tmp_file.close()

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = tmp_file.name


configurar_credenciais_gcp()

# -------------------------------------------------------
# Configuração básica da página
# -------------------------------------------------------
st.set_page_config(
    page_title="Consulta CNO - Base dos Dados",
    layout="wide",
)

st.title("📊 Consulta CNO (Cadastro Nacional de Obras)")
st.markdown(
    """
Este painel consulta os **microdados do CNO** disponibilizados pela **Base dos Dados / BigQuery**.

Você pode:
- Filtrar por **UF**
- Definir um **período de datas** (campo `data_inicio`)
- Ajustar o **limite de linhas**
- Exportar o resultado para **Excel (.xlsx)**
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
💡 Dica: no Streamlit Cloud, salve `BILLING_PROJECT_ID` e `GCP_SERVICE_ACCOUNT_JSON`
em **Secrets** para não precisar digitar aqui.
"""
)


# -------------------------------------------------------
# Função auxiliar: montar query SQL
# -------------------------------------------------------
def montar_query(uf_filtrada=None,
                 data_inicio_min=None,
                 data_inicio_max=None,
                 limite_linhas=10_000):
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

    where_clause = ""
    if filtros:
        where_clause = "WHERE " + " AND ".join(filtros)

    query = f"""
      SELECT
        dados.data_situacao as data_situacao,
        dados.data_inicio as data_inicio,
        dados.sigla_uf AS sigla_uf,
        diretorio_sigla_uf.nome AS sigla_uf_nome,
        dados.id_municipio AS id_municipio,
        diretorio_id_municipio.nome AS id_municipio_nome,
        dados.nome_empresarial as nome_empresarial,
        dados.area as area,
        dados.unidade_medida as unidade_medida,
        dados.bairro as bairro,
        dados.cep as cep,
        dados.logradouro as logradouro,
        dados.tipo_logradouro as tipo_logradouro,
        dados.numero_logradouro as numero_logradouro,
        dados.complemento as complemento,
        dados.caixa_postal as caixa_postal
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
      {where_clause}
      LIMIT {limite_linhas}
    """
    return query


# -------------------------------------------------------
# Área principal: filtros de consulta
# -------------------------------------------------------
st.subheader("🔎 Filtros da consulta")

with st.form("filtros_cno"):
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

    # Período de datas (data_inicio)
    with col2:
        data_inicial_default = date(2023, 5, 16)
        data_final_default = date(2025, 5, 16)
        data_range = st.date_input(
            "Período (data_inicio)",
            value=(data_inicial_default, data_final_default),
            format="YYYY-MM-DD",
        )

        if isinstance(data_range, tuple) and len(data_range) == 2:
            data_inicio_min = data_range[0]
            data_inicio_max = data_range[1]
        else:
            data_inicio_min = None
            data_inicio_max = None

    # Limite de linhas
    with col3:
        limite_linhas = st.number_input(
            "Limite de linhas",
            min_value=1,
            max_value=500_000,
            value=100_000,
            step=10_000,
            help="Quanto maior, mais dados e mais custo de processamento no BigQuery.",
        )

    # Nome do arquivo Excel
    nome_arquivo_excel = st.text_input(
        "Nome do arquivo Excel",
        value="cno_consulta.xlsx",
        help="Nome do arquivo gerado para download.",
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

        sql = montar_query(
            uf_filtrada=uf_filtrada,
            data_inicio_min=data_inicio_min_str,
            data_inicio_max=data_inicio_max_str,
            limite_linhas=int(limite_linhas),
        )

        with st.expander("👀 Ver SQL gerada"):
            st.code(sql, language="sql")

        st.info("Executando consulta no BigQuery (Base dos Dados)...")

        try:
            with st.spinner("Consultando dados..."):
                df = bd.read_sql(
                    query=sql,
                    billing_project_id=billing_project_id,
                    from_file=True,  # <<< usa a service account do JSON
                    reauth=False
                )

            st.success(f"Consulta concluída! Linhas retornadas: {len(df)}")

            if df.empty:
                st.warning("Nenhum dado encontrado para os filtros selecionados.")
            else:
                st.subheader("📋 Amostra dos dados")
                st.dataframe(df.head(100))
                st.markdown(f"**Total de linhas retornadas:** {len(df)}")

                buffer = BytesIO()
                df.to_excel(buffer, index=False)
                buffer.seek(0)

                nome_final = (
                    nome_arquivo_excel
                    if nome_arquivo_excel.lower().endswith(".xlsx")
                    else f"{nome_arquivo_excel}.xlsx"
                )

                st.download_button(
                    label="💾 Baixar Excel",
                    data=buffer,
                    file_name=nome_final,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )

        except Exception as e:
            st.error("❌ Ocorreu um erro ao executar a consulta.")
            st.exception(e)
