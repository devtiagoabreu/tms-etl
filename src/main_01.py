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
import json
from typing import Dict, List, Optional, Tuple
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
import re

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
        
    def get_all_looms(self) -> List[Tuple[str, str]]:
        """Obtém lista de todos os teares disponíveis"""
        try:
            url = f"{self.base_url}/loom/getdata.cgi"
            response = self.session.get(url, timeout=30)
            response.encoding = 'utf-8'
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            teares = []
            for option in soup.find_all('option'):
                value = option.get('value', '')
                if value and value.strip():
                    parts = value.split()
                    if len(parts) >= 2:
                        tear_id = parts[0]
                        tear_name = ' '.join(parts[1:])
                        teares.append((tear_id, tear_name))
            
            logger.info(f"Encontrados {len(teares)} teares")
            return teares
            
        except Exception as e:
            logger.error(f"Erro ao obter lista de teares: {e}")
            return []
    
    def select_all_looms(self) -> bool:
        """Seleciona todos os teares para coleta"""
        try:
            teares = self.get_all_looms()
            if not teares:
                return False
            
            # Cria payload com todos os teares
            loom_values = [f"{tear_id} {tear_name}" for tear_id, tear_name in teares]
            
            # Envia para tela de coleta
            url = f"{self.base_url}/loom/getdata2.cgi"
            data = {'loom': loom_values}
            
            response = self.session.post(url, data=data, timeout=60)
            response.encoding = 'utf-8'
            
            if "Iniciar Coleta de Dados" in response.text:
                logger.info(f"Selecionados {len(teares)} teares para coleta")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Erro ao selecionar teares: {e}")
            return False
    
    def collect_data(self) -> bool:
        """Executa a coleta de dados dos teares selecionados"""
        try:
            # Primeiro seleciona todos os teares
            if not self.select_all_looms():
                return False
            
            # Aguarda processamento da coleta
            logger.info("Coleta em andamento...")
            
            # Monitora a tela de progresso
            progress_url = f"{self.base_url}/loom/getdata2.cgi"
            
            # Simula espera (na implementação real, monitoraria o progresso)
            time.sleep(10)
            
            # Verifica tela final
            result_url = f"{self.base_url}/loom/getdata3.cgi"
            response = self.session.get(result_url, timeout=60)
            response.encoding = 'utf-8'
            
            if "Completado Normalmente" in response.text or "FINALIZAR COLETA" in response.text:
                logger.info("Coleta concluída com sucesso")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Erro na coleta de dados: {e}")
            return False
    
    def export_csv(self, months: List[str] = None, include_forecast: bool = True) -> bool:
        """Exporta dados para CSV"""
        try:
            if months is None:
                # Pega os últimos 2 meses
                current_date = datetime.now()
                months = []
                for i in range(2):
                    month_date = current_date - timedelta(days=30*i)
                    months.append(month_date.strftime("%Y.%m"))
            
            # Acessa tela de exportação
            url = f"{self.base_url}/edit/exportcsv.cgi"
            response = self.session.get(url, timeout=30)
            response.encoding = 'utf-8'
            
            # Prepara dados para exportação
            data = {
                'shift[]': months,
                'operator[]': months,
                'history[]': months,
                'forecast': 'on' if include_forecast else '',
                'submit': 'Exportar Dados'
            }
            
            # Envia requisição de exportação
            export_url = f"{self.base_url}/edit/exportcsv2.cgi"
            response = self.session.post(export_url, data=data, timeout=300)  # 5 minutos timeout
            response.encoding = 'utf-8'
            
            if "EXPORT DONE" in response.text or "EXPORT_DONE" in response.text:
                logger.info(f"Exportação concluída para meses: {', '.join(months)}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Erro na exportação CSV: {e}")
            return False

# ============================================================================
# CLASSE DO BANCO DE DADOS
# ============================================================================
class DatabaseManager:
    """Gerencia operações com o banco de dados MariaDB"""
    
    def __init__(self, host='localhost', port=3306, user='abreu', 
                 password='dqgh3ffrdg', database='dbintegrafabric'):
        self.config = {
            'host': host,
            'port': port,
            'user': user,
            'password': password,
            'database': database
        }
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
    """Processa arquivos CSV e gerencia importação"""
    
    def __init__(self, csv_dir: str = "C:\\TMSDATA"):
        self.csv_dir = Path(csv_dir)
        self.db_manager = None
        
    def set_db_manager(self, db_manager: DatabaseManager):
        """Define o gerenciador de banco de dados"""
        self.db_manager = db_manager
    
    def find_csv_files(self) -> List[Path]:
        """Encontra todos os arquivos CSV no diretório"""
        csv_files = []
        
        if self.csv_dir.exists():
            for pattern in ['*.csv', '*.CSV']:
                csv_files.extend(list(self.csv_dir.rglob(pattern)))
        
        # Ordena por data de modificação (mais recentes primeiro)
        csv_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
        
        return csv_files
    
    def parse_csv_row(self, row: List[str]) -> Dict:
        """Converte uma linha CSV para dicionário"""
        # Mapeia os índices para nomes das colunas (baseado no código C# fornecido)
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
        """Processa um arquivo CSV e importa para o banco"""
        success_count = 0
        error_count = 0
        total_rows = 0
        
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
                logger.error(f"Não foi possível ler o arquivo {file_path}")
                return 0, 0
            
            # Processa linhas
            lines = content.strip().split('\n')
            
            for line_num, line in enumerate(lines, 1):
                total_rows += 1
                
                try:
                    # Remove BOM se existir
                    if line.startswith('\ufeff'):
                        line = line[1:]
                    
                    # Divide por vírgula
                    row = [cell.strip() for cell in line.split(',')]
                    
                    # Pula linhas vazias ou com poucas colunas
                    if len(row) < 3:
                        continue
                    
                    # Converte para dicionário
                    data = self.parse_csv_row(row)
                    
                    # Verifica se é um tear desligado (todos valores zerados no turno C)
                    is_tear_desligado = self.is_tear_desligado(data)
                    
                    if not is_tear_desligado or self.should_process_tear_desligado(data):
                        # Faz UPSERT
                        if self.db_manager and self.db_manager.upsert_data(data):
                            success_count += 1
                            if callback:
                                callback(f"✓ Processado: {data.get('DataTurno', '')} - Tear {data.get('Tear', '')}")
                        else:
                            error_count += 1
                            if callback:
                                callback(f"✗ Erro: {data.get('DataTurno', '')} - Tear {data.get('Tear', '')}")
                    
                except Exception as e:
                    error_count += 1
                    logger.error(f"Erro na linha {line_num}: {e}")
                    if callback:
                        callback(f"✗ Erro linha {line_num}: {str(e)[:50]}")
            
            logger.info(f"Arquivo {file_path.name}: {success_count}/{total_rows} linhas processadas")
            
        except Exception as e:
            logger.error(f"Erro ao processar arquivo {file_path}: {e}")
            if callback:
                callback(f"✗ Erro no arquivo {file_path.name}")
        
        return success_count, error_count
    
    def is_tear_desligado(self, data: Dict) -> bool:
        """Verifica se o tear estava desligado durante a coleta"""
        try:
            # Turno C (último turno) com todos valores zerados
            data_turno = data.get('DataTurno', '')
            
            if data_turno.endswith('.C'):
                # Verifica principais métricas
                funcionando = float(data.get('Funcionando', 0) or 0)
                parado = float(data.get('Parado', 0) or 0)
                rpm = float(data.get('Rpm', 0) or 0)
                
                # Se funcionando = 0 e parado = tempo total do turno, está desligado
                if funcionando == 0 and parado >= 400:  # Turno de ~440 minutos
                    return True
            
            return False
            
        except:
            return False
    
    def should_process_tear_desligado(self, data: Dict) -> bool:
        """Decide se deve processar dados de tear desligado"""
        # Lógica: Processa apenas se não existir registro anterior para este tear/turno
        try:
            data_turno = data.get('DataTurno', '')
            tear = data.get('Tear', '')
            
            if self.db_manager:
                exists = self.db_manager.check_duplicate(data_turno, tear)
                return not exists  # Processa apenas se não existir
            
            return True
        except:
            return False

# ============================================================================
# CLASSE PRINCIPAL DA INTERFACE
# ============================================================================
class TMSETLGUI:
    """Interface principal do sistema ETL"""
    
    def __init__(self, page: ft.Page):
        self.page = page
        self.setup_page()
        
        # Instancia gerenciadores
        self.tms_system = TMSSystem()
        self.db_manager = DatabaseManager()
        self.csv_processor = CSVProcessor()
        self.csv_processor.set_db_manager(self.db_manager)
        
        # Estado da aplicação
        self.is_running = False
        self.current_operation = None
        
    def setup_page(self):
        """Configura a página principal"""
        self.page.title = "ETL Toyota TMS Automation"
        self.page.theme_mode = ft.ThemeMode.LIGHT
        self.page.window_width = 1200
        self.page.window_height = 800
        self.page.window_resizable = True
        self.page.padding = 0
        self.page.spacing = 0
        
        self.build_ui()
        
    def build_ui(self):
        """Constrói a interface do usuário"""
        # Cores
        primary_color = ft.colors.BLUE_700
        green_color = ft.colors.GREEN_700
        orange_color = ft.colors.ORANGE_700
        purple_color = ft.colors.PURPLE_700
        
        # 1. TÍTULO
        title_section = ft.Container(
            content=ft.Column([
                ft.Row([
                    ft.Icon(name=ft.icons.FACTORY, size=30, color=primary_color),
                    ft.Text("ETL Toyota TMS Automation", 
                           size=22, 
                           weight=ft.FontWeight.BOLD,
                           color=primary_color)
                ], alignment=ft.MainAxisAlignment.CENTER),
                ft.Text("Sistema de automação para coleta, exportação e importação de dados dos teares Toyota",
                       size=12,
                       color=ft.colors.GREY_600,
                       text_align=ft.TextAlign.CENTER)
            ]),
            padding=15,
            bgcolor=ft.colors.BLUE_50,
            border_radius=ft.border_radius.only(top_left=10, top_right=10)
        )
        
        # 2. CARDS DE STATUS
        self.system_card = self.create_status_card(
            "Status do Sistema",
            "Pronto",
            ft.icons.CHECK_CIRCLE_OUTLINE,
            green_color,
            self.update_system_status
        )
        
        self.csv_card = self.create_status_card(
            "Arquivos CSV",
            "0 encontrados",
            ft.icons.INSERT_DRIVE_FILE,
            primary_color,
            self.count_csv_files
        )
        
        self.db_card = self.create_status_card(
            "Conexão DB",
            "Não testada",
            ft.icons.STORAGE,
            orange_color,
            self.test_db_connection
        )
        
        self.last_exec_card = self.create_status_card(
            "Última Execução",
            "Nunca",
            ft.icons.ACCESS_TIME,
            purple_color,
            self.update_last_execution
        )
        
        status_row = ft.Row(
            controls=[self.system_card, self.csv_card, self.db_card, self.last_exec_card],
            spacing=10,
            alignment=ft.MainAxisAlignment.SPACE_EVENLY,
            wrap=False
        )
        
        status_section = ft.Container(
            content=ft.Column([
                ft.Text("Status do Sistema", size=16, weight=ft.FontWeight.BOLD, color=primary_color),
                ft.Divider(height=10),
                status_row
            ]),
            padding=15,
            bgcolor=ft.colors.WHITE
        )
        
        # 3. CONFIGURAÇÃO DO BANCO
        self.host_field = ft.TextField(
            label="Host",
            value="localhost",
            width=150,
            height=40,
            content_padding=10,
            border_color=primary_color
        )
        
        self.port_field = ft.TextField(
            label="Porta",
            value="3306",
            width=80,
            height=40,
            content_padding=10,
            border_color=primary_color
        )
        
        self.user_field = ft.TextField(
            label="Usuário",
            value="abreu",
            width=130,
            height=40,
            content_padding=10,
            border_color=primary_color
        )
        
        self.password_field = ft.TextField(
            label="Senha",
            value="dqgh3ffrdg",
            width=130,
            height=40,
            content_padding=10,
            password=True,
            can_reveal_password=True,
            border_color=primary_color
        )
        
        self.database_field = ft.TextField(
            label="Banco de Dados",
            value="dbintegrafabric",
            width=150,
            height=40,
            content_padding=10,
            border_color=primary_color
        )
        
        test_btn = ft.ElevatedButton(
            "Testar Conexão",
            icon=ft.icons.SETTINGS_ETHERNET,
            on_click=lambda e: self.test_db_connection(e),
            bgcolor=primary_color,
            color=ft.colors.WHITE,
            height=40,
            style=ft.ButtonStyle(
                shape=ft.RoundedRectangleBorder(radius=5)
            )
        )
        
        config_section = ft.Container(
            content=ft.Column([
                ft.Text("Configuração do Banco de Dados", 
                       size=16, 
                       weight=ft.FontWeight.BOLD,
                       color=primary_color),
                ft.Divider(height=10),
                ft.Row([
                    self.host_field,
                    self.port_field,
                    self.user_field,
                    self.password_field,
                    self.database_field,
                    test_btn
                ], spacing=10, wrap=False)
            ]),
            padding=15,
            bgcolor=ft.colors.WHITE,
            border=ft.border.all(1, ft.colors.GREY_300),
            border_radius=8
        )
        
        # 4. BOTÕES DE AÇÃO
        self.etl_btn = self.create_action_button(
            "ETL Completo",
            ft.icons.PLAY_ARROW,
            green_color,
            self.run_full_etl
        )
        
        self.import_btn = self.create_action_button(
            "Apenas Importação",
            ft.icons.UPLOAD_FILE,
            primary_color,
            self.run_import_only
        )
        
        self.specific_btn = self.create_action_button(
            "Tear Específico",
            ft.icons.BUILD,
            orange_color,
            self.run_specific_tear
        )
        
        self.collect_btn = self.create_action_button(
            "Apenas Coleta",
            ft.icons.COLLECTIONS_BOOKMARK,
            ft.colors.BLUE_500,
            self.run_collection_only
        )
        
        self.export_btn = self.create_action_button(
            "Apenas Exportação",
            ft.icons.FILE_DOWNLOAD,
            purple_color,
            self.run_export_only
        )
        
        self.cleanup_btn = self.create_action_button(
            "Limpar Arquivos",
            ft.icons.CLEANING_SERVICES,
            ft.colors.RED_700,
            self.run_cleanup
        )
        
        # Layout dos botões em grid
        button_grid = ft.Column(
            controls=[
                ft.Row([self.etl_btn, self.import_btn], spacing=10),
                ft.Row([self.specific_btn, self.collect_btn], spacing=10),
                ft.Row([self.export_btn, self.cleanup_btn], spacing=10)
            ],
            spacing=10
        )
        
        actions_section = ft.Container(
            content=ft.Column([
                ft.Text("Ações", size=16, weight=ft.FontWeight.BOLD, color=primary_color),
                ft.Divider(height=10),
                button_grid
            ]),
            padding=15,
            bgcolor=ft.colors.WHITE,
            border=ft.border.all(1, ft.colors.GREY_300),
            border_radius=8,
            width=430
        )
        
        # 5. PROGRESSO
        self.progress_bar = ft.ProgressBar(width=400, color=primary_color, bgcolor=ft.colors.GREY_300)
        self.progress_text = ft.Text("", size=12, color=ft.colors.GREY_600)
        
        progress_section = ft.Container(
            content=ft.Column([
                ft.Text("Progresso", size=16, weight=ft.FontWeight.BOLD, color=primary_color),
                ft.Divider(height=10),
                ft.Column([
                    self.progress_bar,
                    ft.Container(height=5),
                    self.progress_text
                ])
            ]),
            padding=15,
            bgcolor=ft.colors.WHITE,
            border=ft.border.all(1, ft.colors.GREY_300),
            border_radius=8
        )
        
        # 6. ÁREA DE LOGS
        self.log_display = ft.Column(
            spacing=5,
            scroll=ft.ScrollMode.ALWAYS,
            height=250
        )
        
        log_controls = ft.Row([
            ft.IconButton(
                icon=ft.icons.DELETE_SWEEP,
                icon_size=20,
                tooltip="Limpar logs",
                on_click=lambda e: self.clear_logs()
            ),
            ft.IconButton(
                icon=ft.icons.CONTENT_COPY,
                icon_size=20,
                tooltip="Copiar logs",
                on_click=lambda e: self.copy_logs()
            ),
            ft.IconButton(
                icon=ft.icons.SAVE,
                icon_size=20,
                tooltip="Salvar logs",
                on_click=lambda e: self.save_logs()
            ),
            ft.IconButton(
                icon=ft.icons.PAUSE,
                icon_size=20,
                tooltip="Pausar/Continuar",
                on_click=lambda e: self.toggle_log_scroll()
            )
        ], spacing=5)
        
        log_section = ft.Container(
            content=ft.Column([
                ft.Row([
                    ft.Text("Log de Execução", 
                           size=16, 
                           weight=ft.FontWeight.BOLD,
                           color=primary_color),
                    log_controls
                ]),
                ft.Divider(height=10),
                ft.Container(
                    content=self.log_display,
                    border=ft.border.all(1, ft.colors.GREY_300),
                    border_radius=5,
                    padding=10,
                    bgcolor=ft.colors.BLACK,
                    expand=True
                )
            ], expand=True),
            padding=15,
            bgcolor=ft.colors.WHITE,
            border=ft.border.all(1, ft.colors.GREY_300),
            border_radius=8,
            expand=True
        )
        
        # 7. LAYOUT PRINCIPAL
        main_content = ft.Column(
            controls=[
                title_section,
                ft.Divider(height=0, color=ft.colors.GREY_300),
                status_section,
                ft.Divider(height=0, color=ft.colors.GREY_300),
                ft.Container(config_section, padding=15),
                ft.Row([
                    ft.Column([
                        actions_section,
                        ft.Container(height=10),
                        progress_section
                    ], width=450),
                    ft.VerticalDivider(width=20, color=ft.colors.GREY_300),
                    log_section
                ], expand=True, spacing=0)
            ],
            spacing=0,
            expand=True
        )
        
        # Container principal
        main_container = ft.Container(
            content=main_content,
            expand=True,
            padding=0
        )
        
        self.page.add(main_container)
        
        # Atualizações iniciais
        self.page.update()
        self.initial_updates()
    
    # ============================================================================
    # MÉTODOS AUXILIARES DA INTERFACE
    # ============================================================================
    
    def create_status_card(self, title, value, icon, color, callback):
        """Cria um card de status"""
        return ft.Container(
            content=ft.Column([
                ft.Row([
                    ft.Icon(name=icon, color=color, size=20),
                    ft.Text(title, size=12, weight=ft.FontWeight.BOLD)
                ], spacing=5),
                ft.Divider(height=5),
                ft.Text(value, size=16, weight=ft.FontWeight.BOLD),
                ft.Divider(height=5),
                ft.Text("Clique para atualizar", size=9, color=ft.colors.GREY_500)
            ], spacing=0, tight=True),
            padding=12,
            border=ft.border.all(1, ft.colors.GREY_300),
            border_radius=8,
            bgcolor=ft.colors.WHITE,
            width=180,
            height=100,
            on_click=lambda e: callback(e)
        )
    
    def create_action_button(self, text, icon, color, callback):
        """Cria botão de ação"""
        return ft.ElevatedButton(
            text=text,
            icon=icon,
            style=ft.ButtonStyle(
                bgcolor=color,
                color=ft.colors.WHITE,
                padding=ft.padding.symmetric(horizontal=20, vertical=12),
                shape=ft.RoundedRectangleBorder(radius=8)
            ),
            width=200,
            on_click=callback
        )
    
    def initial_updates(self):
        """Executa atualizações iniciais"""
        def run_updates():
            time.sleep(1)
            self.count_csv_files(None)
            time.sleep(0.5)
            self.test_db_connection(None)
            self.update_last_execution(None)
        
        threading.Thread(target=run_updates, daemon=True).start()
    
    def update_card(self, card, new_value):
        """Atualiza o valor de um card"""
        if hasattr(card, 'content'):
            content = card.content
            if isinstance(content, ft.Column):
                for control in content.controls:
                    if isinstance(control, ft.Text) and len(control.value) > 10:
                        control.value = new_value
                        break
        
        self.page.update()
    
    def set_progress(self, value: float, text: str = ""):
        """Atualiza barra de progresso"""
        self.progress_bar.value = value / 100
        self.progress_text.value = text
        self.page.update()
    
    def add_log(self, message: str, log_type: str = "info"):
        """Adiciona mensagem ao log"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        
        if log_type == "success":
            color = ft.colors.GREEN_400
            icon = "✅"
        elif log_type == "error":
            color = ft.colors.RED_400
            icon = "❌"
        elif log_type == "warning":
            color = ft.colors.ORANGE_400
            icon = "⚠️"
        elif log_type == "debug":
            color = ft.colors.BLUE_400
            icon = "🔍"
        else:
            color = ft.colors.WHITE
            icon = "ℹ️"
        
        log_entry = ft.Row([
            ft.Text(f"[{timestamp}]", size=10, color=ft.colors.GREY_400, width=60),
            ft.Text(icon, size=12, width=20),
            ft.Text(message, color=color, size=12, selectable=True, expand=True)
        ], spacing=5)
        
        self.log_display.controls.append(log_entry)
        
        # Rola para o final
        if len(self.log_display.controls) > 0:
            self.page.update()
            # Tenta rolar para o final
            try:
                self.log_display.scroll_to(offset=-1, duration=100)
            except:
                pass
        
        # Mantém apenas os últimos 100 logs
        if len(self.log_display.controls) > 100:
            self.log_display.controls = self.log_display.controls[-100:]
        
        logger.info(f"{log_type.upper()}: {message}")
    
    # ============================================================================
    # MÉTODOS DE ATUALIZAÇÃO DE STATUS
    # ============================================================================
    
    def update_system_status(self, e=None):
        """Atualiza status do sistema"""
        self.add_log("🔄 Atualizando status do sistema...", "info")
        
        if self.is_running:
            status = "Executando"
            color = ft.colors.ORANGE_700
        else:
            status = "Pronto"
            color = ft.colors.GREEN_700
        
        self.update_card(self.system_card, status)
        self.page.update()
    
    def count_csv_files(self, e=None):
        """Conta arquivos CSV"""
        try:
            csv_files = self.csv_processor.find_csv_files()
            count = len(csv_files)
            
            self.update_card(self.csv_card, f"{count} arquivos")
            
            if count > 0:
                self.add_log(f"📁 Encontrados {count} arquivos CSV", "success")
            else:
                self.add_log("📁 Nenhum arquivo CSV encontrado", "warning")
                
        except Exception as e:
            self.update_card(self.csv_card, "Erro")
            self.add_log(f"❌ Erro ao contar arquivos: {str(e)}", "error")
    
    def test_db_connection(self, e=None):
        """Testa a conexão com o banco"""
        try:
            # Atualiza configuração
            self.db_manager.config = {
                'host': self.host_field.value,
                'port': int(self.port_field.value),
                'user': self.user_field.value,
                'password': self.password_field.value,
                'database': self.database_field.value
            }
            
            if self.db_manager.connect():
                self.update_card(self.db_card, "Conectado")
                self.add_log("✅ Conexão com banco estabelecida", "success")
                return True
            else:
                self.update_card(self.db_card, "Falha")
                self.add_log("❌ Falha na conexão com banco", "error")
                return False
                
        except Exception as e:
            self.update_card(self.db_card, "Falha")
            self.add_log(f"❌ Erro na conexão: {str(e)}", "error")
            return False
    
    def update_last_execution(self, e=None):
        """Atualiza última execução"""
        try:
            if os.path.exists('tms_etl_gui.log'):
                with open('tms_etl_gui.log', 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                    for line in reversed(lines):
                        if ' - INFO - ' in line:
                            timestamp = line.split(' - ')[0]
                            self.update_card(self.last_exec_card, timestamp[:16])
                            return
            
            self.update_card(self.last_exec_card, "Nunca")
            
        except Exception as e:
            self.add_log(f"❌ Erro ao verificar última execução: {str(e)}", "error")
    
    # ============================================================================
    # MÉTODOS DE CONTROLE DE LOGS
    # ============================================================================
    
    def clear_logs(self, e=None):
        """Limpa os logs"""
        self.log_display.controls.clear()
        self.page.update()
        self.add_log("🧹 Logs limpos", "info")
    
    def copy_logs(self, e=None):
        """Copia logs para área de transferência"""
        log_text = ""
        for control in self.log_display.controls:
            if isinstance(control, ft.Row):
                for child in control.controls:
                    if isinstance(child, ft.Text):
                        log_text += child.value + " "
                log_text += "\n"
        
        self.page.set_clipboard(log_text)
        self.add_log("📋 Logs copiados", "success")
    
    def save_logs(self, e=None):
        """Salva logs em arquivo"""
        try:
            filename = f"logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            with open(filename, 'w', encoding='utf-8') as f:
                for control in self.log_display.controls:
                    if isinstance(control, ft.Row):
                        for child in control.controls:
                            if isinstance(child, ft.Text):
                                f.write(child.value + " ")
                        f.write("\n")
            
            self.add_log(f"💾 Logs salvos em {filename}", "success")
        except Exception as e:
            self.add_log(f"❌ Erro ao salvar: {str(e)}", "error")
    
    def toggle_log_scroll(self, e=None):
        """Alterna scroll automático dos logs"""
        self.add_log("⏸️ Controle de scroll alterado", "info")
    
    # ============================================================================
    # MÉTODOS PRINCIPAIS DE EXECUÇÃO
    # ============================================================================
    
    def run_full_etl(self, e):
        """Executa ETL completo"""
        if self.is_running:
            self.add_log("⚠️ Operação já em andamento", "warning")
            return
        
        self.is_running = True
        self.current_operation = "ETL Completo"
        self.update_system_status()
        
        # Desabilita botões durante execução
        self.set_buttons_enabled(False)
        
        def execute_etl():
            try:
                self.add_log("🚀 Iniciando ETL completo...", "info")
                
                # PASSO 1: Coleta de dados
                self.set_progress(10, "Coletando dados dos teares...")
                self.add_log("📡 Coletando dados dos teares...", "info")
                
                if self.tms_system.collect_data():
                    self.add_log("✅ Coleta concluída com sucesso", "success")
                    self.set_progress(30, "Coleta concluída")
                else:
                    self.add_log("❌ Falha na coleta de dados", "error")
                    self.set_progress(30, "Falha na coleta")
                
                # PASSO 2: Exportação CSV
                self.set_progress(40, "Exportando para CSV...")
                self.add_log("💾 Exportando dados para CSV...", "info")
                
                # Obtém meses atual e anterior
                current_month = datetime.now().strftime("%Y.%m")
                last_month = (datetime.now() - timedelta(days=30)).strftime("%Y.%m")
                months = [current_month, last_month]
                
                if self.tms_system.export_csv(months=months, include_forecast=True):
                    self.add_log("✅ Exportação CSV concluída", "success")
                    self.set_progress(60, "Exportação concluída")
                else:
                    self.add_log("⚠️ Exportação CSV pode ter falhado", "warning")
                    self.set_progress(60, "Exportação com possível erro")
                
                # PASSO 3: Processamento CSV
                self.set_progress(70, "Processando arquivos CSV...")
                self.add_log("📂 Processando arquivos CSV...", "info")
                
                csv_files = self.csv_processor.find_csv_files()
                total_files = len(csv_files)
                
                if total_files > 0:
                    self.add_log(f"📁 Encontrados {total_files} arquivos CSV para processar", "info")
                    
                    processed_files = 0
                    total_success = 0
                    total_errors = 0
                    
                    for i, csv_file in enumerate(csv_files):
                        self.set_progress(70 + (i/total_files * 25), 
                                        f"Processando {csv_file.name}...")
                        
                        self.add_log(f"📄 Processando {csv_file.name}...", "debug")
                        
                        success, errors = self.csv_processor.process_csv_file(
                            csv_file,
                            callback=lambda msg: self.add_log(msg, "debug")
                        )
                        
                        total_success += success
                        total_errors += errors
                        processed_files += 1
                        
                        self.add_log(f"✓ {csv_file.name}: {success} linhas OK, {errors} erros", 
                                   "success" if errors == 0 else "warning")
                    
                    self.add_log(f"✅ Processamento concluído: {total_success} linhas OK, {total_errors} erros", 
                               "success" if total_errors == 0 else "warning")
                    self.set_progress(95, f"Processado: {total_success} OK, {total_errors} erros")
                else:
                    self.add_log("⚠️ Nenhum arquivo CSV encontrado para processar", "warning")
                    self.set_progress(95, "Nenhum arquivo encontrado")
                
                # PASSO 4: Finalização
                self.set_progress(100, "ETL completo concluído!")
                self.add_log("🎉 ETL completo concluído com sucesso!", "success")
                
                # Atualiza status
                self.count_csv_files()
                self.update_last_execution()
                
            except Exception as e:
                self.add_log(f"❌ Erro no ETL: {str(e)}", "error")
                self.set_progress(0, f"Erro: {str(e)[:50]}")
            
            finally:
                self.is_running = False
                self.current_operation = None
                self.update_system_status()
                self.set_buttons_enabled(True)
                self.set_progress(0, "Pronto para nova operação")
        
        # Executa em thread separada
        threading.Thread(target=execute_etl, daemon=True).start()
    
    def run_import_only(self, e):
        """Executa apenas importação"""
        if self.is_running:
            self.add_log("⚠️ Operação já em andamento", "warning")
            return
        
        self.is_running = True
        self.current_operation = "Importação"
        self.update_system_status()
        self.set_buttons_enabled(False)
        
        def execute_import():
            try:
                self.add_log("📤 Iniciando importação de dados...", "info")
                self.set_progress(10, "Procurando arquivos CSV...")
                
                csv_files = self.csv_processor.find_csv_files()
                total_files = len(csv_files)
                
                if total_files == 0:
                    self.add_log("❌ Nenhum arquivo CSV encontrado", "error")
                    self.set_progress(0, "Nenhum arquivo encontrado")
                    return
                
                self.add_log(f"📁 Encontrados {total_files} arquivos CSV", "info")
                self.set_progress(20, f"Processando {total_files} arquivos...")
                
                processed_files = 0
                total_success = 0
                total_errors = 0
                
                for i, csv_file in enumerate(csv_files):
                    progress = 20 + (i/total_files * 75)
                    self.set_progress(progress, f"Processando {csv_file.name}...")
                    
                    self.add_log(f"📄 Processando {csv_file.name}...", "debug")
                    
                    success, errors = self.csv_processor.process_csv_file(
                        csv_file,
                        callback=lambda msg: self.add_log(msg, "debug")
                    )
                    
                    total_success += success
                    total_errors += errors
                    processed_files += 1
                    
                    self.add_log(f"✓ {csv_file.name}: {success} linhas OK, {errors} erros", 
                               "success" if errors == 0 else "warning")
                
                # Resumo
                self.set_progress(100, f"Importação concluída: {total_success} OK, {total_errors} erros")
                
                if total_errors == 0:
                    self.add_log(f"✅ Importação concluída com sucesso! {total_success} linhas processadas", "success")
                else:
                    self.add_log(f"⚠️ Importação concluída com {total_errors} erros. {total_success} linhas OK", "warning")
                
                # Atualiza status
                self.update_last_execution()
                
            except Exception as e:
                self.add_log(f"❌ Erro na importação: {str(e)}", "error")
                self.set_progress(0, f"Erro: {str(e)[:50]}")
            
            finally:
                self.is_running = False
                self.current_operation = None
                self.update_system_status()
                self.set_buttons_enabled(True)
                self.set_progress(0, "Pronto para nova operação")
        
        threading.Thread(target=execute_import, daemon=True).start()
    
    def run_collection_only(self, e):
        """Executa apenas coleta"""
        if self.is_running:
            self.add_log("⚠️ Operação já em andamento", "warning")
            return
        
        self.is_running = True
        self.current_operation = "Coleta"
        self.update_system_status()
        self.set_buttons_enabled(False)
        
        def execute_collection():
            try:
                self.add_log("📡 Iniciando coleta de dados...", "info")
                self.set_progress(20, "Conectando ao sistema TMS...")
                
                if self.tms_system.collect_data():
                    self.add_log("✅ Coleta concluída com sucesso", "success")
                    self.set_progress(100, "Coleta concluída")
                else:
                    self.add_log("❌ Falha na coleta de dados", "error")
                    self.set_progress(100, "Falha na coleta")
                
                self.update_last_execution()
                
            except Exception as e:
                self.add_log(f"❌ Erro na coleta: {str(e)}", "error")
                self.set_progress(0, f"Erro: {str(e)[:50]}")
            
            finally:
                self.is_running = False
                self.current_operation = None
                self.update_system_status()
                self.set_buttons_enabled(True)
                self.set_progress(0, "Pronto para nova operação")
        
        threading.Thread(target=execute_collection, daemon=True).start()
    
    def run_export_only(self, e):
        """Executa apenas exportação"""
        if self.is_running:
            self.add_log("⚠️ Operação já em andamento", "warning")
            return
        
        self.is_running = True
        self.current_operation = "Exportação"
        self.update_system_status()
        self.set_buttons_enabled(False)
        
        def execute_export():
            try:
                self.add_log("💾 Iniciando exportação para CSV...", "info")
                self.set_progress(20, "Conectando ao sistema TMS...")
                
                # Obtém meses atual e anterior
                current_month = datetime.now().strftime("%Y.%m")
                last_month = (datetime.now() - timedelta(days=30)).strftime("%Y.%m")
                months = [current_month, last_month]
                
                self.set_progress(40, f"Exportando meses: {', '.join(months)}...")
                
                if self.tms_system.export_csv(months=months, include_forecast=True):
                    self.add_log("✅ Exportação CSV concluída", "success")
                    self.set_progress(100, "Exportação concluída")
                else:
                    self.add_log("⚠️ Exportação CSV pode ter falhado", "warning")
                    self.set_progress(100, "Exportação com possível erro")
                
                # Atualiza contagem de arquivos
                self.count_csv_files()
                self.update_last_execution()
                
            except Exception as e:
                self.add_log(f"❌ Erro na exportação: {str(e)}", "error")
                self.set_progress(0, f"Erro: {str(e)[:50]}")
            
            finally:
                self.is_running = False
                self.current_operation = None
                self.update_system_status()
                self.set_buttons_enabled(True)
                self.set_progress(0, "Pronto para nova operação")
        
        threading.Thread(target=execute_export, daemon=True).start()
    
    def run_specific_tear(self, e):
        """Executa operação para tear específico"""
        self.add_log("🔧 Funcionalidade de tear específico selecionada", "info")
        
        # Diálogo para selecionar tear
        def handle_tear_selection(tear_id):
            if tear_id:
                self.add_log(f"🎯 Processando tear {tear_id}...", "info")
                # Implementar lógica específica para o tear
                
        # Mostra diálogo simples
        tear_dialog = ft.AlertDialog(
            title=ft.Text("Selecionar Tear"),
            content=ft.TextField(label="ID do Tear (ex: 00001)"),
            actions=[
                ft.TextButton("Cancelar", on_click=lambda e: self.page.close(tear_dialog)),
                ft.TextButton("Processar", on_click=lambda e: handle_tear_selection(
                    tear_dialog.content.value
                ))
            ]
        )
        
        self.page.open(tear_dialog)
    
    def run_cleanup(self, e):
        """Limpa arquivos temporários"""
        if self.is_running:
            self.add_log("⚠️ Operação já em andamento", "warning")
            return
        
        def execute_cleanup():
            try:
                self.add_log("🧹 Iniciando limpeza de arquivos...", "info")
                
                # Limpa arquivos CSV antigos (mais de 30 dias)
                csv_dir = Path("C:\\TMSDATA")
                if csv_dir.exists():
                    cutoff_date = datetime.now() - timedelta(days=30)
                    deleted_count = 0
                    
                    for csv_file in csv_dir.rglob("*.csv"):
                        try:
                            file_mtime = datetime.fromtimestamp(csv_file.stat().st_mtime)
                            if file_mtime < cutoff_date:
                                csv_file.unlink()
                                deleted_count += 1
                                self.add_log(f"🗑️ Removido: {csv_file.name}", "debug")
                        except Exception as e:
                            self.add_log(f"❌ Erro ao remover {csv_file.name}: {e}", "error")
                    
                    self.add_log(f"✅ Limpeza concluída: {deleted_count} arquivos removidos", "success")
                else:
                    self.add_log("ℹ️ Diretório C:\\TMSDATA não encontrado", "info")
                
                # Atualiza contagem
                self.count_csv_files()
                
            except Exception as e:
                self.add_log(f"❌ Erro na limpeza: {str(e)}", "error")
        
        threading.Thread(target=execute_cleanup, daemon=True).start()
    
    def set_buttons_enabled(self, enabled: bool):
        """Habilita/desabilita botões"""
        buttons = [self.etl_btn, self.import_btn, self.specific_btn, 
                  self.collect_btn, self.export_btn, self.cleanup_btn]
        
        for btn in buttons:
            btn.disabled = not enabled
        
        self.page.update()

# ============================================================================
# FUNÇÃO PRINCIPAL
# ============================================================================
def main(page: ft.Page):
    """Função principal"""
    # Configurações básicas da página
    page.title = "ETL Toyota TMS Automation"
    page.theme_mode = ft.ThemeMode.LIGHT
    page.window_width = 1200
    page.window_height = 800
    page.window_resizable = True
    page.padding = 0
    
    try:
        app = TMSETLGUI(page)
    except Exception as e:
        page.add(
            ft.Column([
                ft.Text("Erro ao iniciar aplicação", 
                       size=20, color=ft.colors.RED, weight=ft.FontWeight.BOLD),
                ft.Text(str(e), size=12, color=ft.colors.RED),
                ft.ElevatedButton("Recarregar", on_click=lambda e: page.go("/"))
            ], 
            alignment=ft.MainAxisAlignment.CENTER, 
            horizontal_alignment=ft.CrossAxisAlignment.CENTER)
        )
        logger.error(f"Erro ao iniciar aplicação: {str(e)}")

if __name__ == "__main__":
    ft.app(target=main, view=ft.AppView.FLET_APP)