import pyodbc as sql
import pandas as pd
import os
from gerar_conexao_sql import gerar_conexao

caminho_funcionarios = r"C:\Users\jacks\OneDrive\Documentos\Banco J.Morgan\funcionarios.csv"

try:
    if os.path.exists(caminho_funcionarios):
        arquivo_funcionarios = pd.read_csv(caminho_funcionarios)
        print("Dados Carregados Com Sucesso...")
except Exception as e:
    print(f'Erro ao carregar arquivo {e}')

if __name__ == "__main__":
    conexao = gerar_conexao()
    dados_funcionarios = []
    for linha in arquivo_funcionarios.itertuples(index=False):
        dados_funcionarios.append((
            linha.ID_Agencia,
            linha.Nome,
            linha.Cargo,
            linha.Salario,
        ))

def tabela_funcionarios():
    cursor = conexao.cursor()
    cursor.execute('''
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='Tb_Funcionarios')
        CREATE TABLE Tb_Funcionarios (
            ID_Funcionario INT PRIMARY KEY IDENTITY(1,1),
            ID_Agencia INT
                references Tb_Agencias(ID_Agencia) not null,
            Nome VARCHAR(255) not null,
            Cargo VARCHAR(255) not null,
            Salario DECIMAL(18, 2) not null,
        )
    ''')

    query = 'INSERT INTO Tb_Funcionarios(ID_Agencia, Nome, Cargo, Salario) VALUES (?, ?, ?, ?)' 
    cursor.executemany(query, dados_funcionarios)

if __name__ == "__main__":
    try:
        tabela_funcionarios()
        conexao.commit()
        print(f"Tabela Criada e {len(dados_funcionarios)} Dados Cadastrados Com Sucesso!")
    except Exception as e:
        conexao.rollback() 
        print(f"Erro ao processar: {e}")
    finally:
        conexao.close() 
        print("Conexão encerrada.")