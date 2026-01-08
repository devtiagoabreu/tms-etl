import sys

print("=== Teste do Ambiente Virtual ===")
print(f"Python: {sys.version}")
print(f"Executando de: {sys.executable}")

try:
    import flet
    import requests
    import mariadb
    import pandas as pd
    
    print("✅ Todas as dependências instaladas com sucesso!")
    print(f"Flet: {flet.__version__}")
    print(f"Requests: {requests.__version__}")
    print(f"Pandas: {pd.__version__}")
    
except ImportError as e:
    print(f"❌ Erro: {e}")
    print("Execute: pip install -r requirements.txt")