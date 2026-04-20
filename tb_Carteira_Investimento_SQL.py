import pyodbc as sql
import os
from gerar_conexao_sql import gerar_conexao
import pandas as pd

caminho_carteira_investimento = r"C:\Users\jacks\OneDrive\Documentos\Banco J.Morgan\carteira_investimentos.csv"

try:
    if os.path.exists(caminho_carteira_investimento):
        arquivo_carteira_investimento = pd.read_csv(caminho_carteira_investimento)
        print('Dados Carregador Com Sucesso...')
except Exception as e:
    print(f'Falha ao Carregar Dados {e}')

if __name__ == "__main__":
    conexao = gerar_conexao()   
    dados_carteira_investimento = []
for linha in arquivo_carteira_investimento.itertuples(index=False):
    dados_carteira_investimento.append((
        linha.ID_Cliente,
        linha.ID_Produto,
        linha.Valor_Investido,
        linha.Data_Aplicacao,
        linha.Status
    ))

def tabela_carteira_investimento():
    cursor = conexao.cursor()
    cursor.execute('''
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='tb_Carteira_Investimento')
        CREATE TABLE tb_Carteira_Investimento (
            ID_Carteira_Investimento INT PRIMARY KEY IDENTITY(1,1),
            ID_Cliente INT
                references Tb_Clientes(ID_Cliente),
            ID_Investimento INT
                references Tb_Investimentos(ID_Investimentos),
            Valor_Investido DECIMAL(18, 2),
            Data_Aplicacao DATE,
            Status VARCHAR(20)
        )
    ''')

    query = '''
        INSERT INTO tb_Carteira_Investimento (ID_Cliente, ID_Investimento, Valor_Investido, Data_Aplicacao, Status)
        VALUES (?, ?, ?, ?, ?)'''
    cursor.executemany(query, dados_carteira_investimento)

if __name__ == "__main__":
    try:
        tabela_carteira_investimento()
        conexao.commit()
        print(f'Tabela Criada e {len(dados_carteira_investimento)} Dados Inseridos Com Sucesso...')
    except Exception as e:
        conexao.rollback()
        print(f'Falha ao Criar Tabela ou Inserir Dados: {e}')
    finally:
        conexao.close()
        print('Conexão Fechada.')