import flet as ft
import mariadb
import os
import time
import logging
import threading
import requests
from datetime import datetime, timedelta
from pathlib import Path
from bs4 import BeautifulSoup
import csv
import re
from typing import Dict, List, Optional, Tuple, Any

# ============================================================================
# CONFIGURAÇÃO DE LOGGING
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('tms_etl_gui.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# CONSTANTES E CONFIGURAÇÕES
# ============================================================================
CSV_BASE_DIR = Path("C:\\TMSDATA")
DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'abreu',
    'password': 'dqgh3ffrdg',
    'database': 'dbintegrafabric'
}

# ============================================================================
# CLASSE PRINCIPAL DO SISTEMA TMS
# ============================================================================
class TMSSystem:
    """Classe principal para interação com o sistema TMS Perl"""
    
    def __init__(self, base_url: str = "http://127.0.0.1/tms"):
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        })
        self.log_callback = None
    
    def set_log_callback(self, callback):
        """Define callback para logs"""
        self.log_callback = callback
    
    def log(self, message: str, log_type: str = "info"):
        """Adiciona log"""
        if self.log_callback:
            self.log_callback(message, log_type)
        else:
            print(f"[{log_type.upper()}] {message}")
    
    def select_all_looms_and_collect(self) -> bool:
        """Seleciona todos os teares e inicia coleta"""
        try:
            self.log("🔧 Selecionando todos os teares...", "info")
            
            # 1. Acessa tela de seleção de teares
            url = f"{self.base_url}/loom/getdata.cgi"
            response = self.session.get(url, timeout=30)
            response.encoding = 'utf-8'
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 2. Encontra o formulário
            form = soup.find('form')
            if not form:
                self.log("❌ Formulário não encontrado", "error")
                return False
            
            # 3. Prepara dados para selecionar TODOS os teares
            post_data = []
            
            # Encontra todos os options do select
            select = soup.find('select', {'name': 'loom'})
            if select:
                options = select.find_all('option')
                for option in options:
                    value = option.get('value', '')
                    if value:
                        post_data.append(('loom', value))
            
            if not post_data:
                self.log("❌ Nenhum tear encontrado", "error")
                return False
            
            self.log(f"✅ {len(post_data)} teares encontrados", "success")
            
            # 4. Envia seleção de todos os teares
            action_url = f"{self.base_url}/{form.get('action', 'getdata2.cgi').lstrip('/')}"
            
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
                'Referer': url
            }
            
            response = self.session.post(action_url, data=post_data, headers=headers, timeout=60)
            response.encoding = 'utf-8'
            
            # 5. Inicia a coleta de dados
            soup2 = BeautifulSoup(response.text, 'html.parser')
            form2 = soup2.find('form')
            
            if form2 and 'action' in form2.attrs:
                collect_url = f"{self.base_url}/{form2.get('action').lstrip('/')}"
                
                # Prepara dados para coleta
                collect_data = {}
                inputs = form2.find_all('input')
                for inp in inputs:
                    name = inp.get('name')
                    value = inp.get('value', '')
                    if name:
                        collect_data[name] = value
                
                # Adiciona botão de iniciar coleta
                collect_data['start'] = 'Iniciar Coleta de Dados'
                
                self.log("🚀 Iniciando coleta de dados...", "info")
                self.log("⏳ Aguarde, pode demorar alguns minutos...", "warning")
                
                response = self.session.post(collect_url, data=collect_data, headers=headers, timeout=300)
                response.encoding = 'utf-8'
                
                # Aguarda coleta processar
                time.sleep(10)
                
                if "Completado Normalmente" in response.text:
                    self.log("✅ Coleta concluída com sucesso", "success")
                    return True
                else:
                    self.log("⚠️ Coleta pode não ter sido concluída normalmente", "warning")
                    return True  # Continua mesmo assim
            
            return True
            
        except Exception as e:
            self.log(f"❌ Erro na coleta: {str(e)}", "error")
            return False
    
    def get_available_months(self) -> Dict[str, List[str]]:
        """Obtém meses disponíveis para exportação"""
        try:
            self.log("📅 Obtendo meses disponíveis...", "info")
            url = f"{self.base_url}/edit/exportcsv.cgi"
            response = self.session.get(url, timeout=30)
            response.encoding = 'utf-8'
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            months_data = {
                'shift': [],    # Dados do Turno
                'operator': [], # Dados do Operador
                'history': []   # Histórico de Parada
            }
            
            # Encontra todos os selects
            selects = soup.find_all('select')
            
            for i, select in enumerate(selects):
                if i == 0:  # Primeiro select = shift
                    for option in select.find_all('option'):
                        value = option.get('value', '')
                        if value:
                            months_data['shift'].append(value)
                elif i == 1:  # Segundo select = operator
                    for option in select.find_all('option'):
                        value = option.get('value', '')
                        if value:
                            months_data['operator'].append(value)
                elif i == 2:  # Terceiro select = history
                    for option in select.find_all('option'):
                        value = option.get('value', '')
                        if value:
                            months_data['history'].append(value)
            
            self.log(f"✅ Meses disponíveis: Turno={len(months_data['shift'])}, "
                   f"Operador={len(months_data['operator'])}, "
                   f"Histórico={len(months_data['history'])}", "success")
            
            if months_data['shift']:
                self.log(f"📅 Últimos 2 meses turno: {months_data['shift'][:2]}", "info")
            
            return months_data
            
        except Exception as e:
            self.log(f"❌ Erro ao obter meses: {str(e)}", "error")
            return {'shift': [], 'operator': [], 'history': []}
    
    def export_months_with_forecast(self, shift_months: List[str] = None) -> Dict[str, Any]:
        """
        Exporta meses específicos com inventário de fio
        """
        result = {
            'success': False,
            'message': '',
            'months': [],
            'elapsed_time': 0
        }
        
        start_time = time.time()
        
        try:
            # Se não especificar meses, pega os 2 últimos disponíveis
            if not shift_months:
                months_data = self.get_available_months()
                shift_months = months_data['shift'][:2] if months_data['shift'] else []
            
            if not shift_months:
                result['message'] = "Nenhum mês disponível para exportação"
                self.log(result['message'], "error")
                return result
            
            result['months'] = shift_months
            
            self.log(f"💾 Exportando meses: {shift_months}", "info")
            self.log("📊 Com inventário de fio e previsão", "info")
            
            # Prepara dados POST
            post_data = []
            
            # Adiciona meses do turno (shift)
            for month in shift_months:
                post_data.append(('shift[]', month))
            
            # Adiciona forecast (INVENTÁRIO DE FIO)
            post_data.append(('forecast', 'on'))
            
            # Botão submit
            post_data.append(('submit', 'Exportar Dados'))
            
            self.log(f"📤 Enviando dados: {len(post_data)} campos", "debug")
            
            # Faz a requisição
            export_url = f"{self.base_url}/edit/exportcsv2.cgi"
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
                'Referer': f"{self.base_url}/edit/exportcsv.cgi"
            }
            
            self.log(f"🚀 Enviando para: {export_url}", "info")
            self.log("⏳ Exportação em andamento...", "warning")
            
            response = self.session.post(export_url, data=post_data, headers=headers, timeout=300)
            response.encoding = 'utf-8'
            
            elapsed_time = time.time() - start_time
            result['elapsed_time'] = elapsed_time
            
            # Salva resposta para debug
            with open('export_response.html', 'w', encoding='utf-8') as f:
                f.write(response.text)
            
            # Verifica sucesso
            response_text = response.text.upper()
            
            if 'EXPORTACAO CONCLUIDA' in response_text or 'EXPORTAÇÃO CONCLUÍDA' in response_text:
                result['success'] = True
                result['message'] = f"Exportação concluída em {elapsed_time:.1f}s"
                self.log(f"✅ {result['message']}", "success")
                
                # Aguarda criação dos arquivos
                time.sleep(3)
                self.verify_exported_files(shift_months)
                
            elif 'EXPORT DONE' in response_text:
                result['success'] = True
                result['message'] = f"Exportação concluída em {elapsed_time:.1f}s"
                self.log(f"✅ {result['message']}", "success")
                
            else:
                result['message'] = "Exportação não confirmada"
                self.log(f"⚠️ {result['message']}", "warning")
                self.log(f"📄 Resposta salva em export_response.html", "debug")
            
        except requests.exceptions.Timeout:
            result['message'] = "Timeout na exportação"
            self.log(f"⏰ {result['message']}", "error")
        except Exception as e:
            result['message'] = f"Erro: {str(e)}"
            self.log(f"❌ {result['message']}", "error")
        
        return result
    
    def verify_exported_files(self, months: List[str]):
        """Verifica se os arquivos foram criados"""
        try:
            self.log("🔍 Verificando arquivos exportados...", "info")
            
            if not CSV_BASE_DIR.exists():
                self.log(f"❌ Diretório {CSV_BASE_DIR} não existe", "warning")
                return
            
            for month in months:
                # Converte formato 2026.01 para 2026-01
                folder_name = month.replace('.', '-')
                month_dir = CSV_BASE_DIR / folder_name
                
                if month_dir.exists():
                    daily_dir = month_dir / "daily"
                    if daily_dir.exists():
                        csv_files = list(daily_dir.glob("*.csv"))
                        self.log(f"✅ {folder_name}: {len(csv_files)} arquivos daily", "success")
                        
                        # Mostra alguns arquivos
                        for csv_file in csv_files[:3]:
                            size_kb = csv_file.stat().st_size / 1024
                            self.log(f"   📄 {csv_file.name} ({size_kb:.1f} KB)", "debug")
                        
                        if len(csv_files) > 3:
                            self.log(f"   ... (+{len(csv_files)-3} mais)", "debug")
                    else:
                        self.log(f"⚠️ {folder_name}: pasta daily não existe", "warning")
                else:
                    self.log(f"⚠️ {folder_name}: pasta do mês não existe", "warning")
            
            # Verifica forecast.csv
            forecast_file = CSV_BASE_DIR / "forecast.csv"
            if forecast_file.exists():
                size_kb = forecast_file.stat().st_size / 1024
                self.log(f"📊 forecast.csv: {size_kb:.1f} KB", "info")
            
        except Exception as e:
            self.log(f"❌ Erro ao verificar arquivos: {str(e)}", "error")
    
    def run_complete_process(self) -> Dict[str, Any]:
        """
        Executa processo completo: Coleta + Exportação
        """
        result = {
            'success': False,
            'message': '',
            'months': [],
            'elapsed_time': 0
        }
        
        start_time = time.time()
        
        try:
            self.log("="*60, "info")
            self.log("🚀 INICIANDO PROCESSO COMPLETO TMS", "info")
            self.log("="*60, "info")
            
            # PASSO 1: Coleta de dados
            self.log("📡 PASSO 1: Coleta de dados dos teares", "info")
            collection_success = self.select_all_looms_and_collect()
            
            if not collection_success:
                self.log("⚠️ Possível problema na coleta, continuando...", "warning")
            
            # Aguarda entre passos
            time.sleep(5)
            
            # PASSO 2: Exportação
            self.log("💾 PASSO 2: Exportação de dados", "info")
            
            # Obtém meses disponíveis
            months_data = self.get_available_months()
            shift_months = months_data['shift'][:2] if months_data['shift'] else []
            
            if not shift_months:
                result['message'] = "Nenhum mês disponível para exportação"
                return result
            
            # Exporta os 2 últimos meses
            export_result = self.export_months_with_forecast(shift_months)
            
            result.update(export_result)
            
            elapsed_time = time.time() - start_time
            result['elapsed_time'] = elapsed_time
            
            if export_result['success']:
                result['message'] = f"Processo completo concluído em {elapsed_time:.1f}s"
                self.log(f"✅ {result['message']}", "success")
            else:
                result['message'] = f"Processo com possível erro: {export_result['message']}"
            
        except Exception as e:
            result['message'] = f"Erro no processo: {str(e)}"
            self.log(f"❌ {result['message']}", "error")
        
        return result

# ============================================================================
# CLASSE DO BANCO DE DADOS (mantida igual)
# ============================================================================
class DatabaseManager:
    """Gerencia operações com o banco de dados MariaDB"""
    
    def __init__(self, config: Dict = None):
        self.config = config or DB_CONFIG.copy()
        self.connection = None
        
    def connect(self) -> bool:
        """Estabelece conexão com o banco"""
        try:
            self.connection = mariadb.connect(**self.config)
            logger.info("Conexão com banco estabelecida")
            return True
        except mariadb.Error as e:
            logger.error(f"Erro ao conectar ao banco: {e}")
            return False
        except Exception as e:
            logger.error(f"Erro inesperado na conexão: {e}")
            return False
    
    def disconnect(self):
        """Fecha conexão com o banco"""
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def execute_procedure(self, proc_name: str, params: Dict) -> bool:
        """Executa uma stored procedure"""
        try:
            if not self.connection:
                self.connect()
            
            cursor = self.connection.cursor()
            
            # Constrói chamada da procedure
            placeholders = ', '.join(['%s'] * len(params))
            sql = f"CALL {proc_name}({placeholders})"
            
            cursor.execute(sql, list(params.values()))
            self.connection.commit()
            
            cursor.close()
            return True
            
        except mariadb.Error as e:
            logger.error(f"Erro ao executar procedure {proc_name}: {e}")
            if self.connection:
                self.connection.rollback()
            return False
        except Exception as e:
            logger.error(f"Erro inesperado: {e}")
            return False
    
    def check_duplicate(self, data_turno: str, tear: str) -> bool:
        """Verifica se já existe registro com a mesma chave primária"""
        try:
            if not self.connection:
                self.connect()
            
            cursor = self.connection.cursor()
            cursor.execute(
                "SELECT COUNT(*) FROM tblDadosTeares WHERE dataTurno = %s AND tear = %s",
                (data_turno, tear)
            )
            count = cursor.fetchone()[0]
            cursor.close()
            
            return count > 0
            
        except mariadb.Error as e:
            logger.error(f"Erro ao verificar duplicata: {e}")
            return False
    
    def upsert_data(self, data: Dict) -> bool:
        """Faz UPSERT (UPDATE se existir, INSERT se não)"""
        try:
            exists = self.check_duplicate(data['DataTurno'], data['Tear'])
            
            if exists:
                # UPDATE
                return self.update_data(data)
            else:
                # INSERT usando procedure
                return self.execute_procedure("uspDadosTearesInserir", data)
                
        except Exception as e:
            logger.error(f"Erro no UPSERT: {e}")
            return False
    
    def update_data(self, data: Dict) -> bool:
        """Atualiza dados existentes"""
        try:
            if not self.connection:
                self.connect()
            
            cursor = self.connection.cursor()
            
            # Constrói SQL dinâmico para UPDATE
            set_clause = []
            params = []
            
            for key, value in data.items():
                if key not in ['DataTurno', 'Tear']:  # Chave primária
                    set_clause.append(f"{key} = %s")
                    params.append(value)
            
            # Adiciona condições WHERE
            params.append(data['DataTurno'])
            params.append(data['Tear'])
            
            sql = f"UPDATE tblDadosTeares SET {', '.join(set_clause)} WHERE dataTurno = %s AND tear = %s"
            
            cursor.execute(sql, params)
            self.connection.commit()
            cursor.close()
            
            logger.debug(f"Atualizado registro: {data['DataTurno']} - {data['Tear']}")
            return True
            
        except mariadb.Error as e:
            logger.error(f"Erro ao atualizar dados: {e}")
            if self.connection:
                self.connection.rollback()
            return False

# ============================================================================
# CLASSE DO PROCESSADOR CSV
# ============================================================================
class CSVProcessor:
    """Processa arquivos CSV no formato TMS"""
    
    def __init__(self, csv_dir: Path = None):
        self.csv_dir = csv_dir or CSV_BASE_DIR
        self.db_manager = None
        
    def set_db_manager(self, db_manager: DatabaseManager):
        """Define o gerenciador de banco de dados"""
        self.db_manager = db_manager
    
    def find_daily_files(self) -> List[Path]:
        """Encontra arquivos CSV daily"""
        csv_files = []
        
        if self.csv_dir.exists():
            # Procura por pastas YYYY-MM
            for month_dir in self.csv_dir.iterdir():
                if month_dir.is_dir() and re.match(r'\d{4}-\d{2}', month_dir.name):
                    daily_dir = month_dir / "daily"
                    if daily_dir.exists():
                        for csv_file in daily_dir.glob("*.csv"):
                            csv_files.append(csv_file)
        
        csv_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
        return csv_files
    
    def parse_csv_row(self, row: List[str]) -> Dict:
        """Converte linha CSV para dicionário"""
        column_mapping = {
            0: 'DataTurno', 1: 'Tear', 2: 'Artigo', 3: 'None', 4: 'ArtigoGen',
            5: 'Rpm', 6: 'Eficiencia', 7: 'Funcionando', 8: 'Parado', 9: 'Pontos',
            10: 'Metros', 11: 'Jardas', 12: 'MedidaGen', 13: 'QtdGen', 14: 'MinGen',
            15: 'QtdParadasUrdume', 16: 'MinParadasUrdume', 17: 'QtdParadasOurelaFalsa',
            18: 'MinParadasOurelaFalsa', 19: 'QtdParadasLenoDireita',
            20: 'MinParadasLenoDireita', 21: 'QtdParadasLenoEsquerda',
            22: 'MinParadasLenoEsquerda', 23: 'QtdParadasTrama', 24: 'MinParadasTrama',
            25: 'QtdTrocaDeRolo', 26: 'MinTrocaDeRolo', 27: 'QtdCorteTecido',
            28: 'MinCorteTecido', 29: 'QtdParadaManual', 30: 'MinParadaManual',
            31: 'QtdEnergiaDesligada', 32: 'MinEnergiaDesligada', 33: 'QtdParadasOutras',
            34: 'MinParadasOutras', 35: 'Wf11', 36: 'Wf12', 37: 'Wf21', 38: 'Wf22',
            39: 'QtdGen1', 40: 'MinGen1', 41: 'QtdGen2', 42: 'MinGen2', 43: 'QtdGen3',
            44: 'MinGen3', 45: 'QtdGen4', 46: 'MinGen4', 47: 'QtdGen5', 48: 'MinGen5',
            49: 'QtdGen6', 50: 'MinGen6', 51: 'QtdGen7', 52: 'MinGen7', 53: 'QtdGen8',
            54: 'MinGen8', 55: 'QtdGen9', 56: 'MinGen9', 57: 'QtdGen10', 58: 'MinGen10',
            59: 'QtdGen11', 60: 'MinGen11', 61: 'QtdGen12', 62: 'MinGen12',
            63: 'QtdGen13', 64: 'MinGen13', 65: 'QtdGen14', 66: 'MinGen14',
            67: 'QtdGen15', 68: 'MinGen15', 69: 'QtdGen16', 70: 'MinGen16'
        }
        
        data = {}
        for idx, value in enumerate(row):
            if idx in column_mapping:
                column_name = column_mapping[idx]
                data[column_name] = value.strip()
        
        return data
    
    def process_csv_file(self, file_path: Path, callback=None) -> Tuple[int, int]:
        """Processa arquivo CSV"""
        success_count = 0
        error_count = 0
        
        try:
            # Detecta encoding
            encodings = ['utf-8', 'latin-1', 'cp1252']
            content = None
            
            for encoding in encodings:
                try:
                    with open(file_path, 'r', encoding=encoding) as f:
                        content = f.read()
                    break
                except UnicodeDecodeError:
                    continue
            
            if content is None:
                logger.error(f"Não foi possível ler {file_path}")
                return 0, 0
            
            lines = content.strip().split('\n')
            
            for line_num, line in enumerate(lines, 1):
                try:
                    if line.startswith('\ufeff'):
                        line = line[1:]
                    
                    row = [cell.strip() for cell in line.split(',')]
                    
                    if len(row) < 3:
                        continue
                    
                    data = self.parse_csv_row(row)
                    
                    # Verifica tear desligado
                    is_tear_desligado = self.is_tear_desligado(data)
                    
                    if not is_tear_desligado or self.should_process_tear_desligado(data):
                        if self.db_manager and self.db_manager.upsert_data(data):
                            success_count += 1
                            if callback:
                                callback(f"✓ {data.get('DataTurno', '')} - Tear {data.get('Tear', '')}")
                        else:
                            error_count += 1
                            if callback:
                                callback(f"✗ {data.get('DataTurno', '')} - Tear {data.get('Tear', '')}")
                    
                except Exception as e:
                    error_count += 1
                    logger.error(f"Erro linha {line_num}: {e}")
            
            logger.info(f"{file_path.name}: {success_count} OK, {error_count} erros")
            
        except Exception as e:
            logger.error(f"Erro ao processar {file_path}: {e}")
        
        return success_count, error_count
    
    def is_tear_desligado(self, data: Dict) -> bool:
        """Verifica se tear estava desligado"""
        try:
            data_turno = data.get('DataTurno', '')
            
            if data_turno.endswith('.C'):
                funcionando = float(data.get('Funcionando', 0) or 0)
                parado = float(data.get('Parado', 0) or 0)
                
                if funcionando == 0 and parado >= 400:
                    return True
            
            return False
        except:
            return False
    
    def should_process_tear_desligado(self, data: Dict) -> bool:
        """Decide se processa tear desligado"""
        try:
            data_turno = data.get('DataTurno', '')
            tear = data.get('Tear', '')
            
            if self.db_manager:
                exists = self.db_manager.check_duplicate(data_turno, tear)
                return not exists
            
            return True
        except:
            return False

# ============================================================================
# INTERFACE PRINCIPAL
# ============================================================================
class TMSETLGUI:
    """Interface principal"""
    
    def __init__(self, page: ft.Page):
        self.page = page
        self.setup_page()
        
        self.tms_system = TMSSystem()
        self.db_manager = DatabaseManager()
        self.csv_processor = CSVProcessor()
        self.csv_processor.set_db_manager(self.db_manager)
        
        # Inicializar variáveis de estado
        self.is_running = False
        self.current_operation = None
        
        # Conecta o sistema TMS aos logs da GUI
        self.tms_system.set_log_callback(self.add_log)
    
    def setup_page(self):
        """Configura página"""
        self.page.title = "ETL Toyota TMS Automation"
        self.page.theme_mode = ft.ThemeMode.LIGHT
        self.page.window_width = 1200
        self.page.window_height = 800
        self.page.window_resizable = True
        
        self.build_ui()
    
    def build_ui(self):
        """Constrói interface"""
        # Cores
        primary_color = ft.colors.BLUE_700
        green_color = ft.colors.GREEN_700
        blue_color = ft.colors.BLUE_500
        purple_color = ft.colors.PURPLE_700
        
        # TÍTULO
        title = ft.Container(
            content=ft.Column([
                ft.Row([
                    ft.Icon(name=ft.icons.FACTORY, size=30, color=primary_color),
                    ft.Text("ETL Toyota TMS Automation", 
                           size=22, 
                           weight=ft.FontWeight.BOLD,
                           color=primary_color)
                ]),
                ft.Text("Coleta, Exportação e Importação automática de dados",
                       size=12,
                       color=ft.colors.GREY_600)
            ]),
            padding=15,
            bgcolor=ft.colors.BLUE_50
        )
        
        # BOTÕES PRINCIPAIS
        collect_btn = ft.ElevatedButton(
            "📡 Coletar Dados",
            icon=ft.icons.COLLECTIONS_BOOKMARK,
            on_click=self.run_collection,
            bgcolor=blue_color,
            color=ft.colors.WHITE,
            width=250,
            height=50
        )
        
        export_btn = ft.ElevatedButton(
            "💾 Exportar (2 meses)",
            icon=ft.icons.FILE_DOWNLOAD,
            on_click=self.run_export,
            bgcolor=green_color,
            color=ft.colors.WHITE,
            width=250,
            height=50
        )
        
        complete_btn = ft.ElevatedButton(
            "🚀 Processo Completo",
            icon=ft.icons.PLAY_ARROW,
            on_click=self.run_complete_process,
            bgcolor=primary_color,
            color=ft.colors.WHITE,
            width=250,
            height=50
        )
        
        import_btn = ft.ElevatedButton(
            "📤 Importar CSV",
            icon=ft.icons.UPLOAD,
            on_click=self.run_import,
            bgcolor=purple_color,
            color=ft.colors.WHITE,
            width=250,
            height=50
        )
        
        check_files_btn = ft.ElevatedButton(
            "📁 Verificar Arquivos",
            icon=ft.icons.FOLDER,
            on_click=self.check_files,
            bgcolor=ft.colors.ORANGE,
            color=ft.colors.WHITE,
            width=250,
            height=50
        )
        
        # Layout dos botões
        button_grid = ft.Column([
            ft.Row([collect_btn, export_btn], spacing=20),
            ft.Row([complete_btn, import_btn], spacing=20),
            ft.Row([check_files_btn], spacing=20)
        ], spacing=20)
        
        actions_section = ft.Container(
            content=ft.Column([
                ft.Text("Ações Principais", size=18, weight=ft.FontWeight.BOLD),
                ft.Divider(),
                button_grid,
                ft.Divider(),
                ft.Text("Fluxo: 1. Coletar → 2. Exportar (cria CSV) → 3. Importar (banco)",
                       size=12, color=ft.colors.GREY_600)
            ]),
            padding=20,
            bgcolor=ft.colors.WHITE,
            border_radius=10,
            border=ft.border.all(1, ft.colors.GREY_300),
            width=400
        )
        
        # ÁREA DE LOGS
        self.log_display = ft.Column(
            spacing=5,
            scroll=ft.ScrollMode.ALWAYS,
            height=400
        )
        
        log_controls = ft.Row([
            ft.IconButton(
                icon=ft.icons.CLEAR_ALL,
                icon_size=20,
                tooltip="Limpar logs",
                on_click=lambda e: self.clear_logs()
            ),
            ft.IconButton(
                icon=ft.icons.REFRESH,
                icon_size=20,
                tooltip="Atualizar status",
                on_click=lambda e: self.update_status()
            )
        ], spacing=5)
        
        log_section = ft.Container(
            content=ft.Column([
                ft.Row([
                    ft.Text("Log de Execução", size=18, weight=ft.FontWeight.BOLD),
                    log_controls
                ]),
                ft.Divider(),
                ft.Container(
                    content=self.log_display,
                    border=ft.border.all(1, ft.colors.GREY_300),
                    border_radius=5,
                    padding=10,
                    bgcolor=ft.colors.BLACK,
                    expand=True
                )
            ], expand=True),
            padding=20,
            bgcolor=ft.colors.WHITE,
            border_radius=10,
            border=ft.border.all(1, ft.colors.GREY_300),
            expand=True
        )
        
        # LAYOUT PRINCIPAL
        main_content = ft.Column([
            title,
            ft.Divider(),
            ft.Row([
                actions_section,
                ft.VerticalDivider(width=20),
                log_section
            ], expand=True)
        ], expand=True)
        
        self.page.add(main_content)
    
    def add_log(self, message: str, log_type: str = "info"):
        """Adiciona mensagem ao log"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        
        colors = {
            "info": ft.colors.WHITE,
            "success": ft.colors.GREEN,
            "error": ft.colors.RED,
            "warning": ft.colors.ORANGE,
            "debug": ft.colors.BLUE
        }
        
        icons = {
            "info": "ℹ️",
            "success": "✅",
            "error": "❌",
            "warning": "⚠️",
            "debug": "🔍"
        }
        
        color = colors.get(log_type, ft.colors.WHITE)
        icon = icons.get(log_type, "ℹ️")
        
        log_entry = ft.Row([
            ft.Text(f"[{timestamp}]", size=10, color=ft.colors.GREY_400, width=70),
            ft.Text(icon, size=12, width=30),
            ft.Text(message, color=color, size=12, selectable=True, expand=True)
        ], spacing=5)
        
        self.log_display.controls.append(log_entry)
        
        # Rola para o final
        if len(self.log_display.controls) > 0:
            self.page.update()
            try:
                self.log_display.scroll_to(offset=-1, duration=100)
            except:
                pass
        
        # Mantém últimos 200 logs
        if len(self.log_display.controls) > 200:
            self.log_display.controls = self.log_display.controls[-200:]
        
        logger.info(f"{log_type.upper()}: {message}")
    
    def clear_logs(self, e=None):
        """Limpa logs"""
        self.log_display.controls.clear()
        self.page.update()
        self.add_log("Logs limpos", "info")
    
    def update_status(self, e=None):
        """Atualiza status"""
        self.add_log("🔄 Atualizando status...", "info")
        self.check_files(None)
    
    def run_collection(self, e):
        """Executa coleta de dados"""
        if self.is_running:
            self.add_log("⚠️ Operação já em andamento", "warning")
            return
        
        self.is_running = True
        
        def execute():
            try:
                self.add_log("📡 Iniciando coleta de dados...", "info")
                success = self.tms_system.select_all_looms_and_collect()
                
                if success:
                    self.add_log("✅ Coleta concluída com sucesso", "success")
                else:
                    self.add_log("⚠️ Coleta com possível problema", "warning")
                
            except Exception as e:
                self.add_log(f"❌ Erro na coleta: {str(e)}", "error")
            
            finally:
                self.is_running = False
        
        threading.Thread(target=execute, daemon=True).start()
    
    def run_export(self, e):
        """Executa exportação dos últimos 2 meses"""
        if self.is_running:
            self.add_log("⚠️ Operação já em andamento", "warning")
            return
        
        self.is_running = True
        
        def execute():
            try:
                self.add_log("💾 Iniciando exportação dos últimos 2 meses...", "info")
                result = self.tms_system.export_months_with_forecast()
                
                if result['success']:
                    self.add_log(f"✅ {result['message']}", "success")
                    self.add_log(f"📅 Meses exportados: {result['months']}", "info")
                    self.add_log("📂 Arquivos criados em C:\\TMSDATA", "info")
                else:
                    self.add_log(f"❌ {result['message']}", "error")
                
            except Exception as e:
                self.add_log(f"❌ Erro na exportação: {str(e)}", "error")
            
            finally:
                self.is_running = False
        
        threading.Thread(target=execute, daemon=True).start()
    
    def run_complete_process(self, e):
        """Executa processo completo"""
        if self.is_running:
            self.add_log("⚠️ Operação já em andamento", "warning")
            return
        
        self.is_running = True
        
        def execute():
            try:
                self.add_log("🚀 Iniciando processo completo...", "info")
                result = self.tms_system.run_complete_process()
                
                if result['success']:
                    self.add_log(f"✅ {result['message']}", "success")
                    self.add_log("📂 Arquivos prontos para importação", "info")
                else:
                    self.add_log(f"⚠️ {result['message']}", "warning")
                
            except Exception as e:
                self.add_log(f"❌ Erro no processo: {str(e)}", "error")
            
            finally:
                self.is_running = False
        
        threading.Thread(target=execute, daemon=True).start()
    
    def run_import(self, e):
        """Importa arquivos CSV"""
        if self.is_running:
            self.add_log("⚠️ Operação já em andamento", "warning")
            return
        
        self.is_running = True
        
        def execute():
            try:
                self.add_log("📤 Procurando arquivos CSV...", "info")
                
                files = self.csv_processor.find_daily_files()
                
                if not files:
                    self.add_log("❌ Nenhum arquivo CSV encontrado", "error")
                    self.add_log("Execute primeiro a exportação", "warning")
                    return
                
                self.add_log(f"📁 Encontrados {len(files)} arquivos", "info")
                
                total_success = 0
                total_errors = 0
                
                for i, file in enumerate(files):
                    self.add_log(f"📄 Processando {file.name}...", "debug")
                    
                    success, errors = self.csv_processor.process_csv_file(
                        file,
                        callback=lambda msg: self.add_log(msg, "debug")
                    )
                    
                    total_success += success
                    total_errors += errors
                    
                    self.add_log(f"✓ {file.name}: {success} linhas OK, {errors} erros", 
                               "success" if errors == 0 else "warning")
                
                self.add_log(f"✅ Importação concluída: {total_success} OK, {total_errors} erros", 
                           "success" if total_errors == 0 else "warning")
                
            except Exception as e:
                self.add_log(f"❌ Erro na importação: {str(e)}", "error")
            
            finally:
                self.is_running = False
        
        threading.Thread(target=execute, daemon=True).start()
    
    def check_files(self, e):
        """Verifica arquivos existentes"""
        def execute():
            try:
                self.add_log("📁 Verificando arquivos em C:\\TMSDATA...", "info")
                
                if not CSV_BASE_DIR.exists():
                    self.add_log("❌ Diretório não existe", "error")
                    return
                
                # Lista pastas de meses
                month_dirs = [d for d in CSV_BASE_DIR.iterdir() 
                             if d.is_dir() and re.match(r'\d{4}-\d{2}', d.name)]
                
                if month_dirs:
                    self.add_log(f"✅ {len(month_dirs)} pastas de meses encontradas:", "success")
                    
                    for month_dir in month_dirs:
                        daily_dir = month_dir / "daily"
                        if daily_dir.exists():
                            csv_files = list(daily_dir.glob("*.csv"))
                            self.add_log(f"📁 {month_dir.name}: {len(csv_files)} arquivos daily", "info")
                else:
                    self.add_log("⚠️ Nenhuma pasta de mês encontrada", "warning")
                
                # Verifica forecast
                forecast_file = CSV_BASE_DIR / "forecast.csv"
                if forecast_file.exists():
                    size_kb = forecast_file.stat().st_size / 1024
                    self.add_log(f"📊 forecast.csv: {size_kb:.1f} KB", "info")
                
            except Exception as e:
                self.add_log(f"❌ Erro: {str(e)}", "error")
        
        threading.Thread(target=execute, daemon=True).start()

def main(page: ft.Page):
    """Função principal"""
    page.title = "ETL Toyota TMS Automation"
    page.theme_mode = ft.ThemeMode.LIGHT
    page.window_width = 1200
    page.window_height = 800
    page.window_resizable = True
    
    try:
        app = TMSETLGUI(page)
    except Exception as e:
        page.add(ft.Text(f"Erro: {str(e)}", color=ft.colors.RED))

if __name__ == "__main__":
    ft.app(target=main, view=ft.AppView.FLET_APP)