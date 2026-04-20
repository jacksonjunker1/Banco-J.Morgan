import pyodbc as sql
import pandas as pd
import os
from gerar_conexao_sql import gerar_conexao

caminho_contas = r"C:\Users\jacks\OneDrive\Documentos\Banco J.Morgan\contas_bancarias.csv"

try:
    if os.path.exists(caminho_contas):
        arquivo_contas = pd.read_csv(caminho_contas)
        print("Dados Carregados Com Sucesso...")
except Exception as e:
    print(f'Erro ao carregar arquivo {e}')

if __name__ == "__main__":
    conexao = gerar_conexao()
    dados_contas = []
    for linha in arquivo_contas.itertuples(index=False):
        dados_contas.append((
            linha.ID_Cliente,
            linha.ID_Agencia,
            linha.Numero_Conta,
            linha.Saldo,
            linha.Tipo_Conta
        ))

def tabela_Contas():

    cursor = conexao.cursor()

    cursor.execute('''
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name= 'Tb_Contas')
        CREATE TABLE Tb_Contas (
            ID_Conta int identity(1,1) primary key,
            ID_Cliente int  
                references Tb_Clientes(ID_Cliente) not null,
            ID_Agencia int
                references Tb_Agencias (ID_Agencia) not null,
            Numero_Conta char(50) not null,
            Saldo decimal(10,2) not null,
            Tipo_Conta char(50) not null
        )
    ''')

    query = 'INSERT INTO Tb_Contas (ID_Cliente,ID_Agencia,Numero_Conta,Saldo,Tipo_Conta) values (?, ?, ?, ?, ?)'
    cursor.execute('DELETE FROM Tb_Contas')
    cursor.executemany(query,dados_contas)

if __name__ == "__main__":
    try:
        tabela_Contas()
        conexao.commit()
        print(f"Tabela Criada e {len(dados_contas)} Dados Inseridos com Sucesso...")
    except Exception as e:
        conexao.rollback()
        print(f'Erro ao Cadastrar Dados {e} ')    
    finally:
        conexao.close()
        print('Conexao encerrada')