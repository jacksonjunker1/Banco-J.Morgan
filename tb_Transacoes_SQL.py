import pyodbc as sql
import pandas as pd
import os
from gerar_conexao_sql import gerar_conexao

caminho_transacoes = r"C:\Users\jacks\OneDrive\Documentos\Banco J.Morgan\transacoes.csv"

try:
    if os.path.exists(caminho_transacoes):  
        df_transacoes = pd.read_csv(caminho_transacoes)
        print("Dados lidos com sucesso!")
except  Exception as e:
    print(f"Ocorreu um erro: {e}")

if __name__ == "__main__":
    conexao = gerar_conexao()
    cursor = conexao.cursor()
    dados_Transacoes = []
    for linha in df_transacoes.itertuples(index=False):
        dados_Transacoes.append((
            linha.ID_Conta_Origem,
            linha.ID_Conta_Destino,
            linha.ID_Tipo,
            linha.Valor,
            linha.Data_Hora
        ))

def tabela_Transacoes():

    cursor.execute('''
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='Tb_Transacoes' )
        CREATE TABLE Tb_Transacoes (
                ID_Transacao INT PRIMARY KEY IDENTITY(1,1),
                ID_Conta_Origem INT
                    REFERENCES Tb_Contas(ID_Conta),
                ID_Conta_Destino INT
                    REFERENCES Tb_Contas(ID_Conta),
                ID_Tipo INT
                    REFERENCES Tb_Tipo_Transacao(ID_Tipo_Transacao),
                Valor DECIMAL(10, 2),
                Data_Hora varchar(50)
            )
        ''')
    
    query = 'INSERT INTO Tb_Transacoes (ID_Conta_Origem, ID_Conta_Destino, ID_Tipo, Valor, Data_Hora) VALUES (?, ?, ?, ?, ?)'
    cursor.executemany(query, dados_Transacoes)

if __name__ == "__main__":
    try:
        tabela_Transacoes()
        conexao.commit()
        print("Tabela criada e dados inseridos com sucesso!")
    except Exception as e:
        conexao.rollback()
        print(f"Ocorreu um erro: {e}")
    finally:
        conexao.close()
        print("Conexão Encerrada.")
