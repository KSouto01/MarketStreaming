import duckdb
import os
import pandas as pd

# Garante que estamos olhando para o mesmo lugar que o Ingestor
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(BASE_DIR, 'cma_data.duckdb')

print(f"🕵️ Investigando banco em: {db_path}")

if not os.path.exists(db_path):
    print("❌ ERRO: O arquivo .duckdb NÃO existe nesta pasta!")
else:
    try:
        conn = duckdb.connect(db_path, read_only=True)
        
        # 1. Conta linhas
        count = conn.execute("SELECT count(*) FROM market_snapshot").fetchone()[0]
        print(f"📊 Total de linhas na tabela 'market_snapshot': {count}")
        
        if count > 0:
            # 2. Mostra os grupos e produtos gravados (para conferir se bate com o app)
            print("\n📋 Produtos Encontrados:")
            df = conn.execute("SELECT DISTINCT group_name, product_name FROM market_snapshot").fetchdf()
            print(df)
            
            # 3. Mostra uma amostra de preços
            print("\n💲 Amostra de Dados (Top 3):")
            sample = conn.execute("SELECT symbol, Last, Time FROM market_snapshot LIMIT 3").fetchdf()
            print(sample)
        else:
            print("⚠️ O arquivo existe, mas a tabela está VAZIA. O Ingestor não está gravando nada.")
            
    except Exception as e:
        print(f"❌ Erro ao ler banco: {e}")