import threading
import requests
from tabula import read_pdf
import pandas as pd
import os
from zipfile import ZipFile

# URL do PDF do Anexo I (substitua pelo link correto do PDF)
pdf_url = "https://www.gov.br/ans/pt-br/acesso-a-informacao/participacao-da-sociedade/atualizacao-do-rol-de-procedimentos/Anexo_I_Rol_2021RN_465.2021_RN627L.2024.pdf"
pdf_path = "Anexo_I.pdf"
csv_path = "Teste_Amanda.csv"
zip_path = "Teste_Amanda.zip"

# Função para baixar o PDF automaticamente
def baixar_pdf(url, caminho):
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(caminho, "wb") as file:
            file.write(response.content)
        print(f"PDF baixado com sucesso: {caminho}")
    else:
        print("Erro ao baixar o PDF.")
        exit()

# Função para extrair dados da tabela do PDF e salvar como CSV
def extrair_dados_pdf(caminho_pdf, caminho_csv):
    try:
        tabelas = read_pdf(caminho_pdf, pages='all', multiple_tables=True, lattice=True)

        if not tabelas:
            print("Nenhuma tabela foi encontrada no PDF.")
            exit()

        df_final = pd.concat(tabelas, ignore_index=True)

        # Ajustando o cabeçalho da tabela
        df_final.columns = df_final.iloc[0]
        df_final = df_final[1:].reset_index(drop=True)

        # Substituir abreviações
        substituicoes = {"OD": "Odontologia", "AMB": "Ambulatorial"}
        df_final.replace(substituicoes, inplace=True)

        df_final.to_csv(caminho_csv, index=False, encoding="utf-8")
        print(f"Arquivo CSV salvo: {caminho_csv}")

    except Exception as e:
        print(f"Erro ao processar o PDF: {e}")
        exit()

# Função para compactar os arquivos em um ZIP
def compactar_em_zip(arquivos, caminho_zip):
    with ZipFile(caminho_zip, 'w') as zipf:
        for arquivo in arquivos:
            zipf.write(arquivo, os.path.basename(arquivo))
    print(f"Arquivos compactados em: {caminho_zip}")

# Criar uma thread para baixar o PDF
thread_download = threading.Thread(target=baixar_pdf, args=(pdf_url, pdf_path))
thread_download.start()
thread_download.join()  # Esperar o download terminar antes de continuar

# Processar o PDF e criar a tabela
extrair_dados_pdf(pdf_path, csv_path)

# Compactar os arquivos em ZIP
compactar_em_zip([pdf_path, csv_path], zip_path)