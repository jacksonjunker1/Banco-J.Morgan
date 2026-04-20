import pyodbc as sql
import os
from gerar_conexao_sql import gerar_conexao
import pandas as pd

caminho_agencias = r"C:\Users\jacks\OneDrive\Documentos\Banco J.Morgan\agencias.csv"

try:
    if os.path.exists(caminho_agencias):
        arquivo_agencias = pd.read_csv(caminho_agencias)
        print('Dados Carregador Com Sucesso...')
except Exception as e:
    print(f'Falha ao Carregar Dados {e}')

if __name__ == "__main__":
    conexao = gerar_conexao()   
    dados_agencias = []

for linha in arquivo_agencias.itertuples(index=False):
    dados_agencias.append((
        linha.numero_agencia,
        linha.nome_agencia,
        linha.cidade,
        linha.Estado
    ))


def tabela_Agencias ():
    cursor = conexao.cursor()

    cursor.execute('''
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='Tb_Agencias')
        CREATE TABLE Tb_Agencias(
            ID_Agencia int identity(10,1) primary key,
            Numero_Agencia int not null,
            Nome_Agencia varchar(80) not null,
            Cidade varchar(80) not null,
            Estado char(10) not null
        )
        ''')
    
    query = 'INSERT INTO Tb_Agencias(Numero_Agencia,Nome_Agencia,Cidade, Estado) values(?,?,?,?)'
    cursor.executemany(query,dados_agencias)

if __name__ == "__main__":
    try:
        tabela_Agencias()
        conexao.commit()
        print(f"Tabela Criada e {len(dados_agencias)} Dados Cadastrados Com sucesso!")
    except Exception as e:
        print(f"Erro ao processar: {e}")
        conexao.rollback() 
    finally:
        conexao.close() 
        print("Conexão encerrada.")