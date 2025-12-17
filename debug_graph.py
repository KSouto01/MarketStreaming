import os
import requests
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv

# 1. Carrega Credenciais
load_dotenv()
HOST = os.getenv("CMA_HOST")
USER = os.getenv("CMA_USER")
PASS = os.getenv("CMA_PASS")

if not HOST:
    print("❌ Erro: .env não carregado")
    exit()

url = HOST + "/execute"

# 2. Faz Login (Para pegar Sessão)
print("🔑 Fazendo Login...")
s = requests.Session()
login_payload = {
    "id": 1, "name": "LoginRequest", "sessionId": "", 
    "user": USER, "pass": PASS, "type": "s", "service": "m", 
    "transport": "Polling", "version": 1, "sync": True, 
    "oms": {"ip": "127.0.0.1", "channel": "API", "language": "PT"}
}
resp = s.post(url, data={'JSONRequest': json.dumps(login_payload)})
sess_id = resp.json().get('sessionId')

if not sess_id:
    print("❌ Falha no Login")
    print(resp.text)
    exit()

print(f"✅ Sessão: {sess_id[:10]}...")

# 3. Tenta Baixar o Gráfico (DOLF26) - Onde está dando erro
print("\n📉 Solicitando Gráfico DOLF26...")

graph_payload = {
    "id": 2,
    "name": "DailyGraphRequest",
    "sessionId": sess_id,
    "type": "c", 
    "sync": True,
    "symbolId": {"sourceId": "57", "symbol": "DOLF26"}, # B3 Dolar Futuro
    "dateFrom": (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"), # 30 dias atrás
    "dateTo": datetime.now().strftime("%Y-%m-%d"),
    "period": 1
}

# Envia e Imprime TUDO
r = s.post(url, data={'JSONRequest': json.dumps(graph_payload)})

print("\n" + "="*40)
print(f"📡 STATUS CODE: {r.status_code}")
print("📜 RESPOSTA RAW (JSON):")
try:
    print(json.dumps(r.json(), indent=4))
except:
    print(r.text)
print("="*40 + "\n")