import os
import json
import requests
from dotenv import load_dotenv

# Carrega credenciais
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

HOST = os.getenv("CMA_HOST")
USER = os.getenv("CMA_USER")
PASS = os.getenv("CMA_PASS")

def scan_fields():
    print("🕵️ INICIANDO SCANNER DE CAMPOS CMA (MODO TEXTO)...\n")
    
    # 1. Login
    session = requests.Session()
    login_payload = {
        "id": 1, "name": "LoginRequest", "sessionId": "",
        "user": USER, "pass": PASS, "type": "s", "service": "m", 
        "transport": "Polling", "version": 1, "sync": True,
        "oms": {"ip": "0.0.0.0", "channel": "API", "language": "PT"}
    }
    
    try:
        resp = session.post(f"{HOST}/execute", data={'JSONRequest': json.dumps(login_payload)})
        data_login = resp.json()
        if not data_login.get("success"):
            print(f"❌ Falha no Login: {data_login}")
            return
            
        sid = data_login.get("sessionId")
        print(f"✅ Login OK! Sessão: {sid[:8]}...")
    except Exception as e:
        print(f"❌ Erro Crítico Login: {e}")
        return

    # 2. Scanner - Pede TODOS os campos
    print("\n📡 Scaneando DOLF26 (B3)...")
    
    payload = {
        "id": 2, "name": "QuotesRequest", "sessionId": sid,
        "type": "q", "sync": True, 
        "symbols": [{"symbol": "DOLF26", "sourceId": "57"}],
        "fields": [] # VAZIO = Retorna TUDO
    }
    
    try:
        resp = session.post(f"{HOST}/execute", data={'JSONRequest': json.dumps(payload)})
        data = resp.json()
        
        if 'arrQuotes' in data and len(data['arrQuotes']) > 0:
            quote = data['arrQuotes'][0]
            values = quote.get('arrValues', [])
            
            print("\n📋 MAPA DE IDs REAL (HEXADECIMAL/DECIMAL):")
            print(f"{'ID (Chave)':<10} | {'VALOR RETORNADO':<20}")
            print("-" * 40)
            
            # Lista para ordenar alfabeticamente
            all_fields = []
            
            for item in values:
                # item é um dicionário { 'CHAVE': 'VALOR' }
                for k, v in item.items():
                    all_fields.append((str(k), str(v)))
            
            # Ordena pelo ID (texto)
            all_fields.sort(key=lambda x: x[0])
            
            for k, v in all_fields:
                print(f"{k:<10} | {v:<20}")
                
            print("-" * 40)
            print("👉 O QUE PROCURAR AGORA:")
            print("1. Hora: Procure algo como '14:50' ou similar.")
            print("2. Último Preço: Procure o valor '5385.500' (ou atual).")
            print("3. Variação: Procure algo como '0.50' ou '+0.27'.")
        else:
            print("⚠️ Nenhuma cotação retornada ou lista vazia.")
            print(f"Resposta completa: {data}")
            
    except Exception as e:
        print(f"❌ Erro Scanner: {e}")

if __name__ == "__main__":
    scan_fields()