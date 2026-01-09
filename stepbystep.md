📋 Pré-requisitos
Python instalado (versão 3.7 ou superior)

Conexão com internet (para instalar pacotes)

🚀 Passo 1: Verifique a instalação do Python
Abra o terminal/CMD/PowerShell e verifique:

bash
python --version
# ou
python3 --version
Se não tiver Python instalado, baixe em python.org

🛠️ Passo 2: Instale o virtualenv (se necessário)
bash
pip install virtualenv

📁 Passo 3: Crie uma pasta para o projeto
bash
# No Windows
mkdir C:\Projetos\ETL_Toyota_TMS
cd C:\Projetos\ETL_Toyota_TMS

# No Linux/Mac
mkdir ~/Projetos/ETL_Toyota_TMS
cd ~/Projetos/ETL_Toyota_TMS

🌟 Passo 4: Crie o ambiente virtual
Método 1: Usando venv (Recomendado para Python 3.3+)
bash
# Windows
python -m venv venv

# Linux/Mac
python3 -m venv venv

.\venv\Scripts\Activate.ps1

📦 Passo 6: Instale as dependências do projeto
Crie um arquivo requirements.txt com:

txt
flet==0.19.0
requests==2.31.0
mariadb==1.1.8
pandas==2.1.4
openpyxl==3.1.2
python-dotenv==1.0.0
Depois instale:

bash
pip install -r requirements.txt


Instalação individual (se preferir):
bash
pip install flet==0.19.0
pip install requests==2.31.0
pip install mariadb==1.1.8
pip install pandas==2.1.4
pip install openpyxl==3.1.2
pip install python-dotenv==1.0.0

ETL_Toyota_TMS/
│
├── venv_etl/                    # Ambiente virtual (não versionar)
├── src/                         # Código fonte
│   ├── __init__.py
│   ├── main.py                  # Aplicação Flet principal
│   ├── tms_automation.py       # Lógica do ETL
│   ├── database.py             # Conexão com banco
│   └── utils.py                # Funções auxiliares
│
├── config/
│   ├── __init__.py
│   └── settings.py             # Configurações
│
├── logs/                        # Logs da aplicação
├── data/                        # Dados temporários
│   ├── csv/
│   └── backup/
│
├── requirements.txt            # Dependências
├── .env                        # Variáveis de ambiente (não versionar)
├── .gitignore                  # Arquivos para ignorar no Git
└── README.md                   # Documentação


🎯 Passo 11: Crie o script principal
Crie src/main.py com o código da aplicação Flet (coloque todo o código que forneci anteriormente aqui).

⚙️ Passo 12: Configure atalhos úteis
Windows - Criar atalho de ativação:
Crie um arquivo ativar.bat:

⚙️ Passo 12: Configure atalhos úteis
Windows - Criar atalho de ativação:
Crie um arquivo ativar.bat:

batch
@echo off
echo Ativando ambiente virtual...
call venv_etl\Scripts\activate
echo Ambiente virtual ativado!
cmd /k


Linux/Mac - Criar alias:
Adicione ao ~/.bashrc ou ~/.zshrc:

bash
alias ativar_etl="cd ~/Projetos/ETL_Toyota_TMS && source venv_etl/bin/activate"
🏃 Passo 13: Execute a aplicação
bash
# No terminal com ambiente virtual ativado
cd src
python main.py
📊 Passo 14: Verificação final
Comandos úteis para verificar:
bash
# Verificar pacotes instalados
pip list

# Verificar caminho do Python
which python   # Linux/Mac
where python   # Windows

# Verificar se está no ambiente virtual
python -c "import sys; print(sys.prefix)"
🔄 Passo 15: Gerenciamento do ambiente virtual
Atualizar dependências:
bash
pip freeze > requirements.txt
Desativar ambiente:
bash
deactivate
Remover ambiente:
bash
# Windows
rmdir /s venv_etl

# Linux/Mac
rm -rf venv_etl
Recriar ambiente:
bash
deactivate
rm -rf venv_etl
python -m venv venv_etl
# Ative e reinstale as dependências






📦 Como Executar:
# Instale as dependências
pip install flet requests mariadb

# Execute a aplicação
python tms_etl_gui.py

🎯 Benefícios da Interface Gráfica:
Facilidade de Uso: Interface intuitiva para operadores não técnicos

Visualização em Tempo Real: Ver o progresso das operações

Controle Granular: Escolher exatamente qual operação executar

Diagnóstico Fácil: Logs organizados e fáceis de ler

Configuração Centralizada: Todas as configurações em um só lugar

Processamento Específico: Solução fácil para teares desligados

🚀 Operação Especial para Teares Desligados:
A interface facilita especialmente o processamento de teares que estavam desligados:

Clique em "Tear Específico"

Informe o ID do tear (ex: "00042")

Defina o período (dias) para buscar dados

O sistema processa apenas os dados desse tear, evitando duplicações

Esta solução combina toda a funcionalidade do ETL com uma interface moderna e fácil de usar, perfeita para operação diária na fábrica!