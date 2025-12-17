import os
import json
import requests
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

HOST = os.getenv("CMA_HOST")
USER = os.getenv("CMA_USER")
PASS = os.getenv("CMA_PASS")

def bruteforce_cbot():
    print("🕵️ INICIANDO OPERAÇÃO 'CAÇA À CBOT'...\n")
    
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
        sid = resp.json().get("sessionId")
        print(f"✅ Login OK! Sessão: {sid[:8]}...")
    except Exception as e:
        print(f"❌ Erro Login: {e}")
        return

    # 2. Lista de Tentativas
    # Vamos testar Soja (ZS) e Milho (ZC) para Março/26 (H26) e Março/25 (H25 - mais perto, mais garantido)
    variations = [
        # TENTATIVA 1: Padrão Globex (que estávamos usando)
        {"symbol": "ZSH26", "sourceId": "30", "desc": "Padrão Globex (ZS + H26)"},
        
        # TENTATIVA 2: Ano com 1 dígito (Comum em sistemas antigos)
        {"symbol": "ZSH6", "sourceId": "30", "desc": "Ano 1 Dígito (ZS + H6)"},
        
        # TENTATIVA 3: Raiz Antiga (Pit Traded)
        {"symbol": "SH26", "sourceId": "30", "desc": "Raiz Pit (S + H26)"},
        
        # TENTATIVA 4: Contrato Curto (Vencimento mais próximo H25)
        {"symbol": "ZSH25", "sourceId": "30", "desc": "Vencimento Curto (2025)"},
        
        # TENTATIVA 5: Sem SourceID (Sua hipótese)
        {"symbol": "ZSH26", "sourceId": "", "desc": "Sem Source ID"},
        
        # TENTATIVA 6: Mini Contrato (Às vezes o Full não é liberado)
        {"symbol": "XKH26", "sourceId": "30", "desc": "Mini Soja (XK)"}
    ]

    print(f"\n🔫 Disparando {len(variations)} formatos diferentes para ver qual retorna...")
    
    for attempt in variations:
        # Monta o objeto do símbolo (com ou sem sourceId)
        if attempt["sourceId"]:
            sym_obj = {"symbol": attempt["symbol"], "sourceId": attempt["sourceId"]}
        else:
            sym_obj = {"symbol": attempt["symbol"]} # Teste sem source

        payload = {
            "id": 2, "name": "QuotesRequest", "sessionId": sid,
            "type": "q", "sync": True, 
            "symbols": [sym_obj],
            "fields": [] # Traz tudo
        }
        
        try:
            r = session.post(f"{HOST}/execute", data={'JSONRequest': json.dumps(payload)})
            resp = r.json()
            
            # Análise do Resultado
            if 'arrQuotes' in resp and len(resp['arrQuotes']) > 0:
                quote = resp['arrQuotes'][0]
                is_valid = quote.get('isValid', False)
                error_msg = quote.get('error', 'Sem erro')
                
                status_icon = "✅ SUCESSO" if is_valid else "❌ FALHA"
                print(f"\n[{status_icon}] Teste: {attempt['desc']}")
                print(f"   Enviado: {attempt['symbol']}")
                print(f"   Retorno: {error_msg}")
                
                if is_valid:
                    print("   🎉 ENCONTRAMOS! Este é o formato correto.")
                    print(f"   Valores: {quote.get('arrValues')}")
            else:
                print(f"\n[⚠️ VAZIO] Teste: {attempt['desc']} - Sem resposta da API.")
                
        except Exception as e:
            print(f"Erro no teste {attempt['symbol']}: {e}")

if __name__ == "__main__":
    bruteforce_cbot()