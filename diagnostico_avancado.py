import os
import json
import requests
import duckdb
from datetime import datetime, timedelta
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

HOST = os.getenv("CMA_HOST")
USER = os.getenv("CMA_USER")
PASS = os.getenv("CMA_PASS")

def run_investigation():
    print("🕵️ INICIANDO INVESTIGAÇÃO PROFUNDA...\n")
    
    # --- 1. CHECAGEM DO BANCO DE DADOS (O que foi gravado?) ---
    print("📂 [1/3] Auditando Banco de Dados...")
    try:
        db_path = os.path.join(BASE_DIR, 'cma_data.duckdb')
        conn = duckdb.connect(db_path, read_only=True)
        
        # Conta total
        total = conn.execute("SELECT count(*) FROM market_snapshot").fetchone()[0]
        print(f"   Total de linhas: {total}")
        
        # Conta por Grupo
        print("   Distribuição por Grupo:")
        print(conn.execute("SELECT group_name, count(*) FROM market_snapshot GROUP BY group_name").fetchdf())
        
        # Espia CBOT
        print("\n   Amostra CBOT (Se houver):")
        df_cbot = conn.execute("SELECT symbol, Last, Time FROM market_snapshot WHERE group_name = 'CBOT' LIMIT 3").fetchdf()
        if not df_cbot.empty:
            print(df_cbot)
        else:
            print("   ⚠️ NENHUM dado da CBOT encontrado no banco!")
            
    except Exception as e:
        print(f"   ❌ Erro ao ler DB: {e}")

    # --- 2. LOGIN NA API ---
    print("\n🔐 [2/3] Conectando na API...")
    session = requests.Session()
    login_payload = {
        "id": 1, "name": "LoginRequest", "sessionId": "",
        "user": USER, "pass": PASS, "type": "s", "service": "m", 
        "transport": "Polling", "version": 1, "sync": True,
        "oms": {"ip": "0.0.0.0", "channel": "API", "language": "PT"}
    }
    
    try:
        resp = session.post(f"{HOST}/execute", data={'JSONRequest': json.dumps(login_payload)})
        sid = resp.json().get("sessionId")
        print(f"   ✅ Sessão: {sid[:8]}...")
    except:
        print("   ❌ Falha fatal no login.")
        return

    # --- 3. AUDITORIA DE DADOS BRUTOS (CBOT + HISTÓRICO) ---
    print("\n📡 [3/3] Dissecando Dados Brutos...")
    
    # A) Teste de Cotação CBOT (Vamos tentar Março/26 que costuma ter liquidez: ZSH26)
    cbot_symbol = "ZSH26" 
    print(f"\n   👉 BUSCANDO COTAÇÃO: {cbot_symbol} (CBOT)...")
    quote_payload = {
        "id": 2, "name": "QuotesRequest", "sessionId": sid, "type": "q", "sync": True, 
        "symbols": [{"symbol": cbot_symbol, "sourceId": "30"}], # Fonte 30 = CBOT
        "fields": [] # Traz TUDO
    }
    try:
        r = session.post(f"{HOST}/execute", data={'JSONRequest': json.dumps(quote_payload)})
        print("   RESPOSTA CBOT:")
        print(json.dumps(r.json(), indent=2))
    except Exception as e: print(f"Erro: {e}")

    # B) Teste de Histórico Dólar (Para o gráfico)
    # Vamos pegar o contrato atual DOLF26
    hist_symbol = "DOLF26"
    print(f"\n   👉 BUSCANDO HISTÓRICO: {hist_symbol} (B3)...")
    
    # Datas para o payload
    dt_to = datetime.now()
    dt_from = dt_to - timedelta(days=5) # Só 5 dias pra não poluir a tela
    
    hist_payload = {
        "id": 3, "name": "DailyGraphRequest", "sessionId": sid,
        "type": "c", "sync": True, 
        "symbolId": {"sourceId": "57", "symbol": hist_symbol},
        "dateFrom": dt_from.strftime("%Y-%m-%d"),
        "dateTo": dt_to.strftime("%Y-%m-%d"),
        "period": 1
    }
    try:
        r = session.post(f"{HOST}/execute", data={'JSONRequest': json.dumps(hist_payload)})
        data_h = r.json()
        print("   RESPOSTA HISTÓRICO (Primeira barra):")
        if 'bars' in data_h and len(data_h['bars']) > 0:
            print(json.dumps(data_h['bars'][0], indent=2)) # Mostra só o primeiro dia para vermos as chaves
        else:
            print(json.dumps(data_h, indent=2))
            
    except Exception as e: print(f"Erro: {e}")

if __name__ == "__main__":
    run_investigation()