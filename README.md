# Market Streaming

Painel em tempo real para monitoramento de contratos futuros da **B3 (Brasil)** e **CBOT (Chicago)**.

## 🚀 Funcionalidades
- **Streaming de Dados:** Conexão direta com API da CMA via Socket/Polling.
- **Visualização:** Tabelas interativas (AgGrid) com atualização a cada 2 segundos.
- **Histórico:** Gráfico de Candlestick para análise técnica do Dólar e Commodities.
- **Modo Simulação:** Inclui gerador de dados fictícios (`mock_ingestor.py`) para testes fora do horário de pregão.

## 🛠️ Tecnologias
- Python 3.x
- Dash & Plotly (Frontend)
- DuckDB (Banco de dados em memória/arquivo de alta performance)
- Requests (Integração API)

## 📦 Instalação

1. Clone o repositório.
2. Crie um ambiente virtual:
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   venv\Scripts\activate     # Windows