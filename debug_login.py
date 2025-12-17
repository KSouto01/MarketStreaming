import os
import requests
import json
from dotenv import load_dotenv

# 1. Carrega variáveis
print("📂 Carregando .env...")
load_dotenv()

HOST = os.getenv("CMA_HOST")
USER = os.getenv("CMA_USER")
PASS = os.getenv("CMA_PASS")

if not HOST or not USER or not PASS:
    print("❌ ERRO: Verifique seu arquivo .env")
    exit()

# 2. Monta a URL e Payload
url = HOST + "/execute"
print(f"🚀 Conectando em: {url}")
print(f"👤 Usuário: {USER}")

payload = {
    "id": 1,
    "name": "LoginRequest",
    "sessionId": "",  # <--- ADICIONE ESTA LINHA MÁGICA (Vazia)
    "user": USER,
    "pass": PASS,
    "type": "s",
    "service": "m",
    "transport": "Polling",
    "version": 1,
    "sync": True,
    "oms": {"ip": "127.0.0.1", "channel": "API", "language": "PT"}
}

try:
    # 3. Dispara a requisição
    resp = requests.post(url, data={'JSONRequest': json.dumps(payload)}, timeout=10)
    
    print("\n" + "="*40)
    print(f"📡 STATUS HTTP: {resp.status_code}")
    
    try:
        # Tenta formatar o JSON bonitinho
        dados_resposta = resp.json()
        print("📜 RESPOSTA DO SERVIDOR (COMPLETA):")
        print(json.dumps(dados_resposta, indent=4, ensure_ascii=False))
    except:
        # Se não for JSON, mostra texto puro
        print("📜 RESPOSTA (TEXTO):")
        print(resp.text)
        
    print("="*40 + "\n")

except Exception as e:
    print(f"❌ ERRO CRÍTICO: {e}")