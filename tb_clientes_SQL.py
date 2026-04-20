import pyodbc as sql
import os
import pandas as pd
from gerar_conexao_sql import gerar_conexao

caminho_clientes = r"C:\Users\jacks\OneDrive\Documentos\Banco J.Morgan\clientes.csv"

try:
    if os.path.exists(caminho_clientes):
        arquivo_clientes = pd.read_csv(caminho_clientes)
        print("Dados Carregdos Com sucesso...")
except Exception as e:
    print(f"Erro ao Carregar dados {e}")

if __name__ == "__main__":
    conexao = gerar_conexao()
    dados_clientes = []
    
    for linha in arquivo_clientes.itertuples(index=False):
        dados_clientes.append((
            linha.Nome,
            linha.Email,
            linha.CPF,
            linha.Data_Nascimento,
            linha.Endereco
        ))

def tabela_Clientes():
    cursor= conexao.cursor()
    
    cursor.execute('''
        IF NOT EXISTS (SELECT* FROM sys.tables WHERE name='Tb_Clientes')
        CREATE TABLE Tb_Clientes (
            ID_Cliente int identity(1,1) primary key,
            Nome varchar(100) not null,
            Email varchar(120) not null,
            CPF char(50) not null,
            Data_Nascimento char(20) not null,
            Endereco varchar(100) not null
        )

        ''')
    
    query = 'INSERT INTO Tb_Clientes(Nome, Email, CPF, Data_Nascimento, Endereco) values (?, ?, ?, ?, ?)'  
    cursor.executemany(query,dados_clientes)


if __name__ == "__main__":
    try:
        tabela_Clientes()
        conexao.commit()
        print(f"Tabela Criada e {len(dados_clientes)} Dados Cadastrados Com sucesso!")
    except Exception as e:
        print(f"Erro ao processar: {e}")
        conexao.rollback() 
    finally:
        conexao.close() 
        print("Conexão encerrada.")