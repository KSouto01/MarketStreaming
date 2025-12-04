import os
import json
import time
import requests
import duckdb
from datetime import datetime, timedelta
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))
DB_HISTORY = os.path.join(BASE_DIR, 'cma_history.duckdb')
CHART_FILE = os.path.join(BASE_DIR, 'active_chart.json')

# Padrão de Segurança
DEFAULT_TARGET = {"symbol": "DOLF26", "sourceId": "57"}

class HistoryIngestor:
    def __init__(self):
        self.base_url = os.getenv("CMA_HOST") + "/execute"
        self.user = os.getenv("CMA_USER")
        self.password = os.getenv("CMA_PASS")
        self.session = requests.Session()
        self.session_id = None
        self.msg_id = 0
        self.last_symbol = None
        self.error_count = 0 # Contador de falhas
        self._init_db()

    def _init_db(self):
        try:
            with duckdb.connect(DB_HISTORY) as conn:
                conn.execute("CREATE TABLE IF NOT EXISTS market_history (symbol VARCHAR, date_ref DATE, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume DOUBLE)")
        except: pass

    def _post(self, name, payload):
        data = {"id": self.msg_id+1, "name": name, "sessionId": self.session_id or "", **payload}
        self.msg_id += 1
        try:
            # Timeout de 10s para não ficar eternamente esperando
            resp = self.session.post(self.base_url, data={'JSONRequest': json.dumps(data)}, timeout=10)
            return resp.json() if resp.status_code == 200 else None
        except: return None

    def login(self):
        print("📚 [History] Autenticando...")
        resp = self._post("LoginRequest", {"user": self.user, "pass": self.password, "type": "s", "service": "m", "transport": "Polling", "version": 1, "sync": True, "oms": {"ip": "0.0.0.0", "channel": "API", "language": "PT"}})
        if resp and resp.get("success"):
            self.session_id = resp.get("sessionId")
            return True
        return False

    def get_target(self):
        try:
            if os.path.exists(CHART_FILE):
                with open(CHART_FILE, 'r') as f:
                    data = json.load(f)
                    if data.get('symbol'): return data
        except: pass
        return DEFAULT_TARGET

    def force_reset_target(self):
        """Reseta o arquivo JSON para o Dólar se o ativo atual estiver quebrado"""
        print(f"♻️ RESET DE EMERGÊNCIA: Voltando para {DEFAULT_TARGET['symbol']}...")
        try:
            with open(CHART_FILE, 'w') as f:
                json.dump(DEFAULT_TARGET, f)
            self.last_symbol = None # Força re-download
            self.error_count = 0
        except Exception as e:
            print(f"❌ Erro ao resetar JSON: {e}")

    def run(self):
        while True:
            if not self.session_id:
                if not self.login(): time.sleep(5); continue
            
            try:
                target = self.get_target()
                
                # Se mudou o ativo, tenta baixar
                if target['symbol'] != self.last_symbol:
                    
                    # CIRCUIT BREAKER: Se falhar 3x no mesmo ativo, desiste dele
                    if self.error_count >= 3:
                        print(f"🚫 Ativo {target['symbol']} falhou 3x. Bloqueando e resetando...")
                        self.force_reset_target()
                        continue

                    print(f"📉 Baixando Gráfico: {target['symbol']}...")
                    
                    payload = {
                        "type": "c", "sync": True, 
                        "symbolId": {"sourceId": target['sourceId'], "symbol": target['symbol']},
                        "dateFrom": (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d"),
                        "dateTo": datetime.now().strftime("%Y-%m-%d"), "period": 1
                    }
                    resp = self._post("DailyGraphRequest", payload)
                    
                    # Falha de Rede/Timeout
                    if not resp:
                        print(f"⚠️ Timeout/Rede para {target['symbol']}.")
                        self.error_count += 1
                        time.sleep(2)
                        continue

                    # Processamento
                    rows = []
                    bars = resp.get('graphicalBars') or resp.get('bars')
                    
                    if bars:
                        for b in bars:
                            try:
                                rows.append((
                                    target['symbol'], str(b.get('date')),
                                    float(b.get('open') or 0), float(b.get('max') or b.get('high') or 0),
                                    float(b.get('min') or b.get('low') or 0), float(b.get('close') or 0),
                                    float(b.get('volume') or 0)
                                ))
                            except: continue
                    
                    if rows:
                        try:
                            with duckdb.connect(DB_HISTORY) as conn:
                                conn.execute("DELETE FROM market_history")
                                conn.executemany("INSERT INTO market_history VALUES (?,?,?,?,?,?,?)", rows)
                            print(f"✅ Histórico Salvo: {len(rows)} candles.")
                            self.last_symbol = target['symbol']
                            self.error_count = 0 # Sucesso! Zera contador.
                        except: pass
                    else:
                        print(f"⚠️ Ativo {target['symbol']} não tem histórico na API.")
                        # Se a API diz que não tem dados, não adianta insistir. Reseta.
                        self.error_count += 1 
                        time.sleep(1)
                
                time.sleep(1)
            
            except Exception as e:
                print(f"❌ Erro Crítico History: {e}")
                time.sleep(5)

if __name__ == "__main__":
    print("🚀 Ingestor History v3.0 (Com Auto-Reset)")
    HistoryIngestor().run()