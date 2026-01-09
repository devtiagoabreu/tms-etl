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
from typing import Dict, List, Optional, Tuple, Set, Any
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
# CLASSE PRINCIPAL DO SISTEMA TMS - CORRIGIDA
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
    
    def get_available_months(self) -> Dict[str, List[str]]:
        """
        Obtém os meses disponíveis para cada tipo de dados
        
        Returns:
            Dicionário com listas de meses para cada tipo
        """
        try:
            url = f"{self.base_url}/edit/exportcsv.cgi"
            response = self.session.get(url, timeout=30)
            response.encoding = 'utf-8'
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            months_data = {
                'shift': [],    # Dados do Turno (daily)
                'operator': [], # Dados do Operador
                'history': []   # Histórico de Parada
            }
            
            # Encontra todas as tags select
            selects = soup.find_all('select')
            
            for i, select in enumerate(selects):
                if i == 0:  # Primeiro select = shift (Dados do Turno)
                    for option in select.find_all('option'):
                        value = option.get('value', '')
                        if value:
                            months_data['shift'].append(value)
                elif i == 1:  # Segundo select = operator (Dados do Operador)
                    for option in select.find_all('option'):
                        value = option.get('value', '')
                        if value:
                            months_data['operator'].append(value)
                elif i == 2:  # Terceiro select = history (Histórico de Parada)
                    for option in select.find_all('option'):
                        value = option.get('value', '')
                        if value:
                            months_data['history'].append(value)
            
            logger.info(f"Meses disponíveis - Shift: {len(months_data['shift'])}, "
                       f"Operator: {len(months_data['operator'])}, "
                       f"History: {len(months_data['history'])}")
            
            return months_data
            
        except Exception as e:
            logger.error(f"Erro ao obter meses disponíveis: {e}")
            return {'shift': [], 'operator': [], 'history': []}
    
    def export_csv_tms_format(self, shift_months: List[str] = None, 
                            operator_months: List[str] = None,
                            history_months: List[str] = None,
                            include_forecast: bool = True) -> Dict[str, Any]:
        """
        Exporta dados no formato EXATO do TMS Perl
        
        Args:
            shift_months: Meses para Dados do Turno (formato: YYYY.MM)
            operator_months: Meses para Dados do Operador
            history_months: Meses para Histórico de Parada
            include_forecast: Incluir Inventário de Fio e Previsão
            
        Returns:
            Dicionário com resultado da exportação
        """
        result = {
            'success': False,
            'message': '',
            'exported_types': [],
            'months': [],
            'elapsed_time': 0
        }
        
        start_time = time.time()
        
        try:
            # Obtém meses disponíveis
            available_months = self.get_available_months()
            
            # Se não especificou meses, pega os 2 últimos meses disponíveis
            if not shift_months:
                shift_months = available_months['shift'][:2] if available_months['shift'] else []
            
            if not operator_months:
                operator_months = available_months['operator'][:2] if available_months['operator'] else []
            
            if not history_months:
                history_months = available_months['history'][:2] if available_months['history'] else []
            
            # Verifica se há pelo menos algum mês selecionado
            if not shift_months and not operator_months and not history_months and not include_forecast:
                result['message'] = "Nenhum dado selecionado para exportação"
                logger.error(result['message'])
                return result
            
            # Constrói a lista de meses exportados
            all_months = list(set(shift_months + operator_months + history_months))
            result['months'] = all_months
            
            # Constrói tipos exportados
            exported_types = []
            if shift_months:
                exported_types.append('shift')
            if operator_months:
                exported_types.append('operator')
            if history_months:
                exported_types.append('history')
            if include_forecast:
                exported_types.append('forecast')
            
            result['exported_types'] = exported_types
            
            # Acessa tela de exportação
            url = f"{self.base_url}/edit/exportcsv.cgi"
            response = self.session.get(url, timeout=30)
            response.encoding = 'utf-8'
            
            if response.status_code != 200:
                result['message'] = f"Falha ao acessar tela de exportação: HTTP {response.status_code}"
                logger.error(result['message'])
                return result
            
            # Prepara dados no formato EXATO do TMS Perl
            # Formato: shift=YYYY.MM&shift=YYYY.MM&operator=YYYY.MM&history=YYYY.MM&forecast=on&submit=Exportar Dados
            data = {}
            
            # Adiciona meses para cada tipo (pode ter múltiplos valores com mesma chave)
            for month in shift_months:
                if 'shift' not in data:
                    data['shift'] = []
                data['shift'].append(month)
            
            for month in operator_months:
                if 'operator' not in data:
                    data['operator'] = []
                data['operator'].append(month)
            
            for month in history_months:
                if 'history' not in data:
                    data['history'] = []
                data['history'].append(month)
            
            # Adiciona forecast (checkbox)
            if include_forecast:
                data['forecast'] = 'on'
            
            # Botão de submit
            data['submit'] = 'Exportar Dados'
            
            logger.info(f"Exportando dados no formato TMS:")
            if shift_months:
                logger.info(f"  • Dados do Turno: {', '.join(shift_months)}")
            if operator_months:
                logger.info(f"  • Dados do Operador: {', '.join(operator_months)}")
            if history_months:
                logger.info(f"  • Histórico de Parada: {', '.join(history_months)}")
            if include_forecast:
                logger.info(f"  • Inventário de Fio e Previsão: SIM")
            
            # Envia requisição de exportação - FORMATO CORRETO
            export_url = f"{self.base_url}/edit/exportcsv2.cgi"
            
            # IMPORTANTE: O TMS Perl espera dados no formato application/x-www-form-urlencoded
            # com múltiplos valores para a mesma chave
            post_data = []
            
            # Adiciona shift (pode ter múltiplos valores)
            if 'shift' in data:
                for month in data['shift']:
                    post_data.append(('shift', month))
            
            # Adiciona operator (pode ter múltiplos valores)
            if 'operator' in data:
                for month in data['operator']:
                    post_data.append(('operator', month))
            
            # Adiciona history (pode ter múltiplos valores)
            if 'history' in data:
                for month in data['history']:
                    post_data.append(('history', month))
            
            # Adiciona forecast (apenas se for 'on')
            if 'forecast' in data and data['forecast'] == 'on':
                post_data.append(('forecast', 'on'))
            
            # Adiciona submit
            post_data.append(('submit', 'Exportar Dados'))
            
            # Faz a requisição POST com o formato correto
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
                'Referer': f"{self.base_url}/edit/exportcsv.cgi"
            }
            
            response = self.session.post(export_url, data=post_data, headers=headers, timeout=300)
            response.encoding = 'utf-8'
            
            elapsed_time = time.time() - start_time
            result['elapsed_time'] = elapsed_time
            
            # Verifica resultado - O TMS retorna "EXPORT DONE" quando sucesso
            response_text = response.text
            response_text_upper = response_text.upper()
            
            if "EXPORT DONE" in response_text_upper or "EXPORT_DONE" in response_text_upper:
                result['success'] = True
                result['message'] = f"Exportação concluída em {elapsed_time:.1f}s"
                logger.info(f"✅ {result['message']}")
                
                # Aguarda um pouco e verifica se os arquivos foram criados
                time.sleep(3)
                self.verify_tms_export_files(shift_months, operator_months, history_months, include_forecast)
                
            elif "EXPORTACAO CONCLUIDA" in response_text_upper or "EXPORTAÇÃO CONCLUÍDA" in response_text_upper:
                result['success'] = True
                result['message'] = f"Exportação concluída em {elapsed_time:.1f}s"
                logger.info(f"✅ {result['message']}")
                
            else:
                result['message'] = "Exportação não retornou confirmação de sucesso"
                logger.warning(f"⚠️ {result['message']}")
                
                # Salva resposta para debug
                with open('tms_export_debug.html', 'w', encoding='utf-8') as f:
                    f.write(response_text)
                logger.warning(f"Resposta salva em tms_export_debug.html para análise")
                
                # Tenta verificar se há algum erro específico
                if "ERROR" in response_text_upper:
                    error_match = re.search(r'error[^<]*', response_text_upper, re.IGNORECASE)
                    if error_match:
                        result['message'] += f": {error_match.group(0)[:100]}"
            
        except requests.exceptions.Timeout:
            result['message'] = "Timeout na exportação (tempo excedido)"
            logger.error(f"⏰ {result['message']}")
        except Exception as e:
            result['message'] = f"Erro na exportação: {str(e)}"
            logger.error(f"❌ {result['message']}")
        
        finally:
            # Garante que o tempo é sempre calculado
            if result['elapsed_time'] == 0:
                result['elapsed_time'] = time.time() - start_time
        
        return result
    
    def verify_tms_export_files(self, shift_months: List[str], operator_months: List[str], 
                               history_months: List[str], include_forecast: bool):
        """Verifica se os arquivos foram criados no formato TMS"""
        try:
            # Para cada mês de shift, verifica se criou a pasta YYYY-MM e dentro dela daily/ e machine/
            for month in shift_months:
                # Converte formato YYYY.MM para YYYY-MM
                year_month = month.replace('.', '-')
                month_dir = CSV_BASE_DIR / year_month
                
                if month_dir.exists():
                    daily_dir = month_dir / "daily"
                    machine_dir = month_dir / "machine"
                    
                    if daily_dir.exists():
                        daily_files = list(daily_dir.glob("*.csv"))
                        logger.info(f"✓ Shift {month}: {len(daily_files)} arquivos daily em {daily_dir}")
                    
                    if machine_dir.exists():
                        machine_files = list(machine_dir.glob("*.csv"))
                        logger.info(f"✓ Shift {month}: {len(machine_files)} arquivos machine em {machine_dir}")
            
            # Para cada mês de operator, verifica se criou a pasta YYYY-MM e dentro dela operator/
            for month in operator_months:
                year_month = month.replace('.', '-')
                month_dir = CSV_BASE_DIR / year_month
                
                if month_dir.exists():
                    operator_dir = month_dir / "operator"
                    
                    if operator_dir.exists():
                        operator_files = list(operator_dir.glob("*.csv"))
                        logger.info(f"✓ Operator {month}: {len(operator_files)} arquivos operator em {operator_dir}")
            
            # Para cada mês de history, verifica se criou a pasta stop_history
            if history_months:
                history_dir = CSV_BASE_DIR / "stop_history"
                if history_dir.exists():
                    history_files = list(history_dir.glob("*.csv"))
                    logger.info(f"✓ History: {len(history_files)} arquivos em stop_history")
            
            # Verifica forecast.csv na raiz
            if include_forecast:
                forecast_file = CSV_BASE_DIR / "forecast.csv"
                if forecast_file.exists():
                    logger.info(f"✓ Forecast: forecast.csv criado")
            
        except Exception as e:
            logger.error(f"Erro ao verificar arquivos exportados: {e}")
    
    def export_last_two_months_all(self) -> Dict[str, Any]:
        """
        Exporta os últimos 2 meses para todos os tipos de dados
        (Funciona EXATAMENTE como a interface web manual)
        """
        try:
            # Obtém meses disponíveis
            available_months = self.get_available_months()
            
            # Pega os últimos 2 meses de cada tipo que estão disponíveis
            shift_months = available_months['shift'][:2] if available_months['shift'] else []
            operator_months = available_months['operator'][:2] if available_months['operator'] else []
            history_months = available_months['history'][:2] if available_months['history'] else []
            
            if not shift_months and not operator_months and not history_months:
                return {
                    'success': False,
                    'message': "Nenhum mês disponível para exportação",
                    'exported_types': [],
                    'months': [],
                    'elapsed_time': 0
                }
            
            logger.info(f"Exportando últimos 2 meses:")
            logger.info(f"  Shift: {shift_months}")
            logger.info(f"  Operator: {operator_months}")
            logger.info(f"  History: {history_months}")
            logger.info(f"  Forecast: SIM")
            
            return self.export_csv_tms_format(
                shift_months=shift_months,
                operator_months=operator_months,
                history_months=history_months,
                include_forecast=True
            )
            
        except Exception as e:
            logger.error(f"Erro ao exportar últimos 2 meses: {e}")
            return {
                'success': False,
                'message': f"Erro: {str(e)}",
                'exported_types': [],
                'months': [],
                'elapsed_time': 0
            }
    
    def export_daily_only_last_two_months(self) -> Dict[str, Any]:
        """Exporta apenas dados daily (shift) dos últimos 2 meses"""
        try:
            available_months = self.get_available_months()
            shift_months = available_months['shift'][:2] if available_months['shift'] else []
            
            if not shift_months:
                return {
                    'success': False,
                    'message': "Nenhum mês disponível para dados do turno",
                    'exported_types': [],
                    'months': [],
                    'elapsed_time': 0
                }
            
            logger.info(f"Exportando apenas daily (shift): {shift_months}")
            
            return self.export_csv_tms_format(
                shift_months=shift_months,
                operator_months=[],
                history_months=[],
                include_forecast=False
            )
            
        except Exception as e:
            logger.error(f"Erro ao exportar daily: {e}")
            return {
                'success': False,
                'message': f"Erro: {str(e)}",
                'exported_types': [],
                'months': [],
                'elapsed_time': 0
            }
    
    def export_operator_only_last_two_months(self) -> Dict[str, Any]:
        """Exporta apenas dados operator dos últimos 2 meses"""
        try:
            available_months = self.get_available_months()
            operator_months = available_months['operator'][:2] if available_months['operator'] else []
            
            if not operator_months:
                return {
                    'success': False,
                    'message': "Nenhum mês disponível para dados do operador",
                    'exported_types': [],
                    'months': [],
                    'elapsed_time': 0
                }
            
            logger.info(f"Exportando apenas operator: {operator_months}")
            
            return self.export_csv_tms_format(
                shift_months=[],
                operator_months=operator_months,
                history_months=[],
                include_forecast=False
            )
            
        except Exception as e:
            logger.error(f"Erro ao exportar operator: {e}")
            return {
                'success': False,
                'message': f"Erro: {str(e)}",
                'exported_types': [],
                'months': [],
                'elapsed_time': 0
            }
    
    def export_history_only_last_two_months(self) -> Dict[str, Any]:
        """Exporta apenas dados history dos últimos 2 meses"""
        try:
            available_months = self.get_available_months()
            history_months = available_months['history'][:2] if available_months['history'] else []
            
            if not history_months:
                return {
                    'success': False,
                    'message': "Nenhum mês disponível para histórico de parada",
                    'exported_types': [],
                    'months': [],
                    'elapsed_time': 0
                }
            
            logger.info(f"Exportando apenas history: {history_months}")
            
            return self.export_csv_tms_format(
                shift_months=[],
                operator_months=[],
                history_months=history_months,
                include_forecast=False
            )
            
        except Exception as e:
            logger.error(f"Erro ao exportar history: {e}")
            return {
                'success': False,
                'message': f"Erro: {str(e)}",
                'exported_types': [],
                'months': [],
                'elapsed_time': 0
            }
    
    def get_export_status(self) -> Dict[str, Any]:
        """
        Verifica o status da exportação (quais arquivos foram criados)
        
        Returns:
            Status detalhado dos arquivos exportados
        """
        result = {
            'total_files': 0,
            'files_by_type': {},
            'last_export_time': None,
            'directory_structure': {}
        }
        
        try:
            # Verifica estrutura de diretórios
            if CSV_BASE_DIR.exists():
                # Lista todas as pastas YYYY-MM (criadas pelo TMS)
                month_dirs = [d for d in CSV_BASE_DIR.iterdir() if d.is_dir() and re.match(r'\d{4}-\d{2}', d.name)]
                
                for month_dir in month_dirs:
                    month_structure = {
                        'daily': 0,
                        'machine': 0,
                        'operator': 0
                    }
                    
                    # Verifica subpastas
                    daily_dir = month_dir / "daily"
                    if daily_dir.exists():
                        daily_files = list(daily_dir.glob("*.csv"))
                        month_structure['daily'] = len(daily_files)
                        result['total_files'] += len(daily_files)
                    
                    machine_dir = month_dir / "machine"
                    if machine_dir.exists():
                        machine_files = list(machine_dir.glob("*.csv"))
                        month_structure['machine'] = len(machine_files)
                        result['total_files'] += len(machine_files)
                    
                    operator_dir = month_dir / "operator"
                    if operator_dir.exists():
                        operator_files = list(operator_dir.glob("*.csv"))
                        month_structure['operator'] = len(operator_files)
                        result['total_files'] += len(operator_files)
                    
                    result['directory_structure'][month_dir.name] = month_structure
                
                # Verifica stop_history
                history_dir = CSV_BASE_DIR / "stop_history"
                if history_dir.exists():
                    history_files = list(history_dir.glob("*.csv"))
                    result['files_by_type']['history'] = len(history_files)
                    result['total_files'] += len(history_files)
                
                # Verifica forecast.csv
                forecast_file = CSV_BASE_DIR / "forecast.csv"
                if forecast_file.exists():
                    result['files_by_type']['forecast'] = 1
                    result['total_files'] += 1
                    result['last_export_time'] = datetime.fromtimestamp(forecast_file.stat().st_mtime)
            
            logger.info(f"Status exportação: {result['total_files']} arquivos em {len(result['directory_structure'])} meses")
            
        except Exception as e:
            logger.error(f"Erro ao verificar status: {e}")
        
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
# CLASSE DO PROCESSADOR CSV (ATUALIZADA para novo formato)
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
        """
        Encontra arquivos CSV daily no formato TMS
        
        O TMS cria: C:\TMSDATA\2026-01\daily\2026-01-01.csv
        """
        csv_files = []
        
        if self.csv_dir.exists():
            # Procura por pastas YYYY-MM
            for month_dir in self.csv_dir.iterdir():
                if month_dir.is_dir() and re.match(r'\d{4}-\d{2}', month_dir.name):
                    daily_dir = month_dir / "daily"
                    if daily_dir.exists():
                        for csv_file in daily_dir.glob("*.csv"):
                            csv_files.append(csv_file)
        
        # Ordena por data de modificação (mais recentes primeiro)
        csv_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
        
        return csv_files
    
    def get_csv_summary(self) -> Dict[str, Any]:
        """
        Obtém resumo dos arquivos CSV no formato TMS
        
        Returns:
            Resumo por tipo e mês
        """
        summary = {
            'total_files': 0,
            'months': {},
            'recent_files': 0,
            'total_size': 0
        }
        
        try:
            if self.csv_dir.exists():
                # Procura por pastas YYYY-MM
                for month_dir in self.csv_dir.iterdir():
                    if month_dir.is_dir() and re.match(r'\d{4}-\d{2}', month_dir.name):
                        month_summary = {
                            'daily': 0,
                            'machine': 0,
                            'operator': 0,
                            'files': []
                        }
                        
                        # Verifica subpastas
                        daily_dir = month_dir / "daily"
                        if daily_dir.exists():
                            daily_files = list(daily_dir.glob("*.csv"))
                            month_summary['daily'] = len(daily_files)
                            summary['total_files'] += len(daily_files)
                        
                        machine_dir = month_dir / "machine"
                        if machine_dir.exists():
                            machine_files = list(machine_dir.glob("*.csv"))
                            month_summary['machine'] = len(machine_files)
                            summary['total_files'] += len(machine_files)
                        
                        operator_dir = month_dir / "operator"
                        if operator_dir.exists():
                            operator_files = list(operator_dir.glob("*.csv"))
                            month_summary['operator'] = len(operator_files)
                            summary['total_files'] += len(operator_files)
                        
                        summary['months'][month_dir.name] = month_summary
                
                # Verifica stop_history
                history_dir = self.csv_dir / "stop_history"
                if history_dir.exists():
                    history_files = list(history_dir.glob("*.csv"))
                    summary['months']['stop_history'] = {'files': len(history_files)}
                    summary['total_files'] += len(history_files)
                
                # Verifica forecast.csv
                forecast_file = self.csv_dir / "forecast.csv"
                if forecast_file.exists():
                    summary['months']['forecast'] = {'files': 1}
                    summary['total_files'] += 1
                
                # Calcula tamanho total
                for file_path in self.csv_dir.rglob("*.csv"):
                    summary['total_size'] += file_path.stat().st_size
                
                # Conta arquivos recentes (últimas 24 horas)
                cutoff_time = datetime.now() - timedelta(hours=24)
                for file_path in self.csv_dir.rglob("*.csv"):
                    if datetime.fromtimestamp(file_path.stat().st_mtime) > cutoff_time:
                        summary['recent_files'] += 1
            
            logger.info(f"Resumo TMS CSV: {summary['total_files']} arquivos, "
                       f"{summary['recent_files']} recentes, "
                       f"{summary['total_size'] / 1024 / 1024:.2f} MB")
            
        except Exception as e:
            logger.error(f"Erro ao obter resumo CSV: {e}")
        
        return summary
    
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
# CLASSE PRINCIPAL DA INTERFACE (ATUALIZADA)
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
        red_color = ft.colors.RED_700
        
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
            self.show_csv_summary
        )
        
        self.db_card = self.create_status_card(
            "Conexão DB",
            "Não testada",
            ft.icons.STORAGE,
            orange_color,
            self.test_db_connection
        )
        
        self.months_card = self.create_status_card(
            "Meses Disp.",
            "Verificando...",
            ft.icons.CALENDAR_MONTH,
            purple_color,
            self.show_available_months
        )
        
        self.last_exec_card = self.create_status_card(
            "Última Execução",
            "Nunca",
            ft.icons.ACCESS_TIME,
            red_color,
            self.update_last_execution
        )
        
        status_row = ft.Row(
            controls=[self.system_card, self.csv_card, self.db_card, 
                     self.months_card, self.last_exec_card],
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
        
        # 4. BOTÕES DE AÇÃO - SIMPLIFICADOS E FUNCIONAIS
        self.etl_btn = self.create_action_button(
            "ETL Completo",
            ft.icons.PLAY_ARROW,
            green_color,
            self.run_full_etl
        )
        
        self.import_btn = self.create_action_button(
            "Importar Daily",
            ft.icons.UPLOAD_FILE,
            primary_color,
            self.run_import_only
        )
        
        self.collect_btn = self.create_action_button(
            "Coletar Dados",
            ft.icons.COLLECTIONS_BOOKMARK,
            ft.colors.BLUE_500,
            self.run_collection_only
        )
        
        self.export_all_btn = self.create_action_button(
            "Exportar Tudo",
            ft.icons.FILE_DOWNLOAD,
            purple_color,
            self.run_export_all
        )
        
        # Botões de exportação específica - CORRIGIDOS
        self.export_daily_btn = self.create_action_button(
            "Daily (Turno)",
            ft.icons.TODAY,
            ft.colors.GREEN_600,
            lambda e: self.run_export_daily()
        )
        
        self.export_operator_btn = self.create_action_button(
            "Operador",
            ft.icons.PERSON,
            ft.colors.ORANGE_600,
            lambda e: self.run_export_operator()
        )
        
        self.export_history_btn = self.create_action_button(
            "Histórico",
            ft.icons.HISTORY,
            ft.colors.PURPLE_600,
            lambda e: self.run_export_history()
        )
        
        self.export_forecast_btn = self.create_action_button(
            "Previsão",
            ft.icons.TRENDING_UP,
            ft.colors.BLUE_600,
            lambda e: self.run_export_forecast()
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
                ft.Row([self.collect_btn, self.export_all_btn], spacing=10),
                ft.Divider(height=5),
                ft.Text("Exportação Específica:", size=12, weight=ft.FontWeight.BOLD),
                ft.Row([self.export_daily_btn, self.export_operator_btn], spacing=10),
                ft.Row([self.export_history_btn, self.export_forecast_btn], spacing=10),
                ft.Divider(height=5),
                ft.Row([self.cleanup_btn], spacing=10)
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
            width=450
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
                icon=ft.icons.REFRESH,
                icon_size=20,
                tooltip="Atualizar status",
                on_click=lambda e: self.initial_updates()
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
            self.add_log("🔄 Atualizando status inicial...", "info")
            
            # Atualiza status do sistema
            self.update_system_status(None)
            
            # Atualiza contagem de CSV
            self.show_csv_summary(None)
            
            # Testa conexão com banco
            self.test_db_connection(None)
            
            # Verifica meses disponíveis
            self.show_available_months(None)
            
            # Atualiza última execução
            self.update_last_execution(None)
            
            self.add_log("✅ Status inicial atualizado", "success")
        
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
        if self.is_running:
            status = "Executando"
            color = ft.colors.ORANGE_700
        else:
            status = "Pronto"
            color = ft.colors.GREEN_700
        
        self.update_card(self.system_card, status)
    
    def show_csv_summary(self, e=None):
        """Mostra resumo dos arquivos CSV"""
        try:
            summary = self.csv_processor.get_csv_summary()
            total_files = summary['total_files']
            recent_files = summary['recent_files']
            
            if total_files > 0:
                display_text = f"{total_files} ({recent_files} recentes)"
                self.add_log(f"📁 {total_files} arquivos CSV encontrados ({recent_files} recentes)", "info")
            else:
                display_text = "0 arquivos"
                self.add_log("📁 Nenhum arquivo CSV encontrado", "warning")
            
            self.update_card(self.csv_card, display_text)
            
            # Mostra detalhes se clicou
            if e:
                self.show_csv_details_dialog(summary)
                
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
    
    def show_available_months(self, e=None):
        """Mostra meses disponíveis para exportação"""
        try:
            self.add_log("📅 Verificando meses disponíveis...", "info")
            months_data = self.tms_system.get_available_months()
            
            shift_count = len(months_data.get('shift', []))
            operator_count = len(months_data.get('operator', []))
            history_count = len(months_data.get('history', []))
            
            if shift_count > 0 or operator_count > 0 or history_count > 0:
                display_text = f"{shift_count}/{operator_count}/{history_count}"
                self.update_card(self.months_card, display_text)
                
                # Mostra os últimos 2 meses de cada tipo
                if months_data['shift']:
                    last_two_shift = months_data['shift'][:2]
                    self.add_log(f"📊 Turno: {', '.join(last_two_shift)}", "info")
                if months_data['operator']:
                    last_two_operator = months_data['operator'][:2]
                    self.add_log(f"👤 Operador: {', '.join(last_two_operator)}", "info")
                if months_data['history']:
                    last_two_history = months_data['history'][:2]
                    self.add_log(f"📈 Histórico: {', '.join(last_two_history)}", "info")
                
                self.add_log("✅ Meses disponíveis verificados", "success")
            else:
                self.update_card(self.months_card, "Nenhum")
                self.add_log("⚠️ Nenhum mês disponível para exportação", "warning")
                
            # Mostra detalhes se clicou
            if e:
                self.show_months_dialog(months_data)
                
        except Exception as e:
            self.update_card(self.months_card, "Erro")
            self.add_log(f"❌ Erro ao verificar meses: {str(e)}", "error")
    
    def update_last_execution(self, e=None):
        """Atualiza última execução"""
        try:
            if os.path.exists('tms_etl_gui.log'):
                with open('tms_etl_gui.log', 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                    for line in reversed(lines):
                        if ' - INFO - ' in line or ' - SUCCESS - ' in line:
                            timestamp = line.split(' - ')[0]
                            self.update_card(self.last_exec_card, timestamp[:16])
                            return
            
            self.update_card(self.last_exec_card, "Nunca")
            
        except Exception as e:
            self.add_log(f"❌ Erro ao verificar última execução: {str(e)}", "error")
    
    # ============================================================================
    # MÉTODOS DE DIÁLOGO
    # ============================================================================
    
    def show_csv_details_dialog(self, summary: Dict):
        """Mostra diálogo com detalhes dos arquivos CSV"""
        content_rows = []
        
        for month, month_info in summary.get('months', {}).items():
            if month == 'stop_history':
                files_count = month_info.get('files', 0)
                content_rows.append(
                    ft.ListTile(
                        title=ft.Text("STOP_HISTORY", weight=ft.FontWeight.BOLD),
                        subtitle=ft.Text(f"{files_count} arquivos"),
                        leading=ft.Icon(ft.icons.HISTORY, color=ft.colors.PURPLE),
                    )
                )
            elif month == 'forecast':
                content_rows.append(
                    ft.ListTile(
                        title=ft.Text("FORECAST", weight=ft.FontWeight.BOLD),
                        subtitle=ft.Text("forecast.csv"),
                        leading=ft.Icon(ft.icons.TRENDING_UP, color=ft.colors.BLUE),
                    )
                )
            else:
                daily_count = month_info.get('daily', 0)
                machine_count = month_info.get('machine', 0)
                operator_count = month_info.get('operator', 0)
                total_month = daily_count + machine_count + operator_count
                
                content_rows.append(
                    ft.ListTile(
                        title=ft.Text(month, weight=ft.FontWeight.BOLD),
                        subtitle=ft.Text(
                            f"Daily: {daily_count}, Machine: {machine_count}, Operator: {operator_count}"
                        ),
                        leading=ft.Icon(ft.icons.FOLDER, color=ft.colors.BLUE),
                    )
                )
        
        dialog = ft.AlertDialog(
            title=ft.Text("Detalhes dos Arquivos CSV"),
            content=ft.Column(
                [
                    ft.Text(f"Total: {summary['total_files']} arquivos"),
                    ft.Text(f"Recentes (24h): {summary['recent_files']} arquivos"),
                    ft.Text(f"Tamanho total: {summary['total_size'] / 1024 / 1024:.2f} MB"),
                    ft.Divider(),
                    ft.Container(
                        content=ft.Column(content_rows, scroll=ft.ScrollMode.ALWAYS),
                        height=200
                    )
                ],
                tight=True
            ),
            actions=[
                ft.TextButton("Fechar", on_click=lambda e: self.page.close(dialog))
            ]
        )
        
        self.page.open(dialog)
    
    def show_months_dialog(self, months_data: Dict):
        """Mostra diálogo com meses disponíveis"""
        content_rows = []
        
        # Shift (Turno)
        if months_data.get('shift'):
            shift_months = months_data['shift'][:5]  # Mostra apenas os 5 primeiros
            shift_text = ", ".join(shift_months)
            if len(months_data['shift']) > 5:
                shift_text += f" (+{len(months_data['shift']) - 5} mais)"
            
            content_rows.append(
                ft.ListTile(
                    title=ft.Text("Dados do Turno (Daily)", weight=ft.FontWeight.BOLD),
                    subtitle=ft.Text(f"{len(months_data['shift'])} meses: {shift_text}"),
                    leading=ft.Icon(ft.icons.TODAY, color=ft.colors.GREEN),
                )
            )
        
        # Operator
        if months_data.get('operator'):
            operator_months = months_data['operator'][:5]
            operator_text = ", ".join(operator_months)
            if len(months_data['operator']) > 5:
                operator_text += f" (+{len(months_data['operator']) - 5} mais)"
            
            content_rows.append(
                ft.ListTile(
                    title=ft.Text("Dados do Operador", weight=ft.FontWeight.BOLD),
                    subtitle=ft.Text(f"{len(months_data['operator'])} meses: {operator_text}"),
                    leading=ft.Icon(ft.icons.PERSON, color=ft.colors.ORANGE),
                )
            )
        
        # History
        if months_data.get('history'):
            history_months = months_data['history'][:5]
            history_text = ", ".join(history_months)
            if len(months_data['history']) > 5:
                history_text += f" (+{len(months_data['history']) - 5} mais)"
            
            content_rows.append(
                ft.ListTile(
                    title=ft.Text("Histórico de Parada", weight=ft.FontWeight.BOLD),
                    subtitle=ft.Text(f"{len(months_data['history'])} meses: {history_text}"),
                    leading=ft.Icon(ft.icons.HISTORY, color=ft.colors.PURPLE),
                )
            )
        
        dialog = ft.AlertDialog(
            title=ft.Text("Meses Disponíveis para Exportação"),
            content=ft.Column(
                [
                    ft.Text("Os últimos 2 meses de cada tipo serão exportados:"),
                    ft.Container(
                        content=ft.Column(content_rows),
                        height=150
                    ),
                    ft.Divider(),
                    ft.Text("Formato TMS: YYYY.MM (ex: 2026.01, 2025.12)")
                ],
                tight=True
            ),
            actions=[
                ft.TextButton("Fechar", on_click=lambda e: self.page.close(dialog))
            ]
        )
        
        self.page.open(dialog)
    
    # ============================================================================
    # MÉTODOS PRINCIPAIS DE EXECUÇÃO - CORRIGIDOS
    # ============================================================================
    
    def run_full_etl(self, e):
        """Executa ETL completo igual à interface web manual"""
        if self.is_running:
            self.add_log("⚠️ Operação já em andamento", "warning")
            return
        
        self.is_running = True
        self.current_operation = "ETL Completo"
        self.update_system_status()
        self.set_buttons_enabled(False)
        
        def execute_etl():
            try:
                self.add_log("🚀 Iniciando ETL completo (igual à interface web)...", "info")
                
                # PASSO 1: Coleta de dados
                self.set_progress(10, "Coletando dados dos teares...")
                self.add_log("📡 Coletando dados dos teares...", "info")
                
                if self.tms_system.collect_data():
                    self.add_log("✅ Coleta concluída com sucesso", "success")
                    self.set_progress(20, "Coleta concluída")
                else:
                    self.add_log("❌ Falha na coleta de dados", "error")
                    self.set_progress(20, "Falha na coleta")
                
                # PASSO 2: Exportação CSV (EXATAMENTE como a interface web)
                self.set_progress(30, "Exportando dados (igual à web)...")
                self.add_log("💾 Exportando dados (igual à interface web manual)...", "info")
                
                # Exporta EXATAMENTE como a interface web: últimos 2 meses de cada tipo + forecast
                export_result = self.tms_system.export_last_two_months_all()
                
                if export_result['success']:
                    self.add_log(f"✅ {export_result['message']}", "success")
                    
                    # Mostra detalhes do que foi exportado
                    exported_types = export_result.get('exported_types', [])
                    if 'shift' in exported_types:
                        self.add_log("📅 Dados do Turno (Daily) exportados", "info")
                    if 'operator' in exported_types:
                        self.add_log("👤 Dados do Operador exportados", "info")
                    if 'history' in exported_types:
                        self.add_log("📈 Histórico de Parada exportado", "info")
                    if 'forecast' in exported_types:
                        self.add_log("📊 Previsão (forecast.csv) exportada", "info")
                    
                    self.set_progress(50, f"Exportação em {export_result['elapsed_time']:.1f}s")
                else:
                    self.add_log(f"⚠️ {export_result['message']}", "warning")
                    self.set_progress(50, "Exportação com possível erro")
                
                # PASSO 3: Processamento CSV (apenas arquivos daily)
                self.set_progress(60, "Processando arquivos daily...")
                self.add_log("📂 Processando arquivos daily para importação...", "info")
                
                csv_files = self.csv_processor.find_daily_files()
                total_files = len(csv_files)
                
                if total_files > 0:
                    self.add_log(f"📁 Encontrados {total_files} arquivos daily para processar", "info")
                    
                    processed_files = 0
                    total_success = 0
                    total_errors = 0
                    
                    for i, csv_file in enumerate(csv_files):
                        progress = 60 + (i/total_files * 35)
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
                    
                    self.add_log(f"✅ Processamento concluído: {total_success} linhas OK, {total_errors} erros", 
                               "success" if total_errors == 0 else "warning")
                    self.set_progress(95, f"Processado: {total_success} OK, {total_errors} erros")
                else:
                    self.add_log("⚠️ Nenhum arquivo daily encontrado para processar", "warning")
                    self.set_progress(95, "Nenhum arquivo daily encontrado")
                
                # PASSO 4: Finalização
                self.set_progress(100, "ETL completo concluído!")
                self.add_log("🎉 ETL completo concluído com sucesso!", "success")
                self.add_log("📂 Arquivos em: C:\\TMSDATA\\YYYY-MM\\daily\\", "info")
                
                # Atualiza status
                self.show_csv_summary()
                self.show_available_months()
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
        
        threading.Thread(target=execute_etl, daemon=True).start()
    
    def run_import_only(self, e):
        """Executa apenas importação de arquivos daily"""
        if self.is_running:
            self.add_log("⚠️ Operação já em andamento", "warning")
            return
        
        self.is_running = True
        self.current_operation = "Importação"
        self.update_system_status()
        self.set_buttons_enabled(False)
        
        def execute_import():
            try:
                self.add_log("📤 Iniciando importação de arquivos daily...", "info")
                self.set_progress(10, "Procurando arquivos daily...")
                
                csv_files = self.csv_processor.find_daily_files()
                total_files = len(csv_files)
                
                if total_files == 0:
                    self.add_log("❌ Nenhum arquivo daily encontrado", "error")
                    self.set_progress(0, "Nenhum arquivo encontrado")
                    return
                
                self.add_log(f"📁 Encontrados {total_files} arquivos daily", "info")
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
    
    def run_export_all(self, e):
        """Exporta tudo (igual à interface web manual)"""
        if self.is_running:
            self.add_log("⚠️ Operação já em andamento", "warning")
            return
        
        self.is_running = True
        self.current_operation = "Exportação Completa"
        self.update_system_status()
        self.set_buttons_enabled(False)
        
        def execute_export():
            try:
                self.add_log("💾 Iniciando exportação completa (igual à web)...", "info")
                self.set_progress(20, "Verificando meses disponíveis...")
                
                # Exporta EXATAMENTE como a interface web manual
                export_result = self.tms_system.export_last_two_months_all()
                
                if export_result['success']:
                    self.add_log(f"✅ {export_result['message']}", "success")
                    
                    # Mostra o que foi exportado
                    exported_types = export_result.get('exported_types', [])
                    type_names = {
                        'shift': 'Dados do Turno (Daily)',
                        'operator': 'Dados do Operador',
                        'history': 'Histórico de Parada',
                        'forecast': 'Previsão'
                    }
                    
                    for exp_type in exported_types:
                        name = type_names.get(exp_type, exp_type)
                        self.add_log(f"✓ {name} exportado", "info")
                    
                    self.set_progress(100, f"Exportação em {export_result['elapsed_time']:.1f}s")
                    self.add_log("📂 Arquivos salvos em: C:\\TMSDATA", "info")
                else:
                    self.add_log(f"⚠️ {export_result['message']}", "warning")
                    self.set_progress(100, "Exportação com possível erro")
                
                # Atualiza status
                self.show_csv_summary()
                self.show_available_months()
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
    
    def run_export_daily(self):
        """Exporta apenas dados daily (turno)"""
        if self.is_running:
            self.add_log("⚠️ Operação já em andamento", "warning")
            return
        
        self.is_running = True
        self.current_operation = "Exportação Daily"
        self.update_system_status()
        self.set_buttons_enabled(False)
        
        def execute():
            try:
                self.add_log("📅 Exportando apenas dados do turno (daily)...", "info")
                self.set_progress(30, "Exportando dados daily...")
                
                export_result = self.tms_system.export_daily_only_last_two_months()
                
                if export_result['success']:
                    self.add_log(f"✅ {export_result['message']}", "success")
                    self.add_log("✓ Dados do Turno (Daily) exportados para C:\\TMSDATA\\YYYY-MM\\daily\\", "info")
                    self.set_progress(100, f"Daily exportado em {export_result['elapsed_time']:.1f}s")
                else:
                    self.add_log(f"❌ {export_result['message']}", "error")
                    self.set_progress(100, "Falha na exportação")
                
                # Atualiza status
                self.show_csv_summary()
                self.show_available_months()
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
        
        threading.Thread(target=execute, daemon=True).start()
    
    def run_export_operator(self):
        """Exporta apenas dados operator"""
        if self.is_running:
            self.add_log("⚠️ Operação já em andamento", "warning")
            return
        
        self.is_running = True
        self.current_operation = "Exportação Operador"
        self.update_system_status()
        self.set_buttons_enabled(False)
        
        def execute():
            try:
                self.add_log("👤 Exportando apenas dados do operador...", "info")
                self.set_progress(30, "Exportando dados operator...")
                
                export_result = self.tms_system.export_operator_only_last_two_months()
                
                if export_result['success']:
                    self.add_log(f"✅ {export_result['message']}", "success")
                    self.add_log("✓ Dados do Operador exportados para C:\\TMSDATA\\YYYY-MM\\operator\\", "info")
                    self.set_progress(100, f"Operator exportado em {export_result['elapsed_time']:.1f}s")
                else:
                    self.add_log(f"❌ {export_result['message']}", "error")
                    self.set_progress(100, "Falha na exportação")
                
                # Atualiza status
                self.show_csv_summary()
                self.show_available_months()
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
        
        threading.Thread(target=execute, daemon=True).start()
    
    def run_export_history(self):
        """Exporta apenas dados history"""
        if self.is_running:
            self.add_log("⚠️ Operação já em andamento", "warning")
            return
        
        self.is_running = True
        self.current_operation = "Exportação Histórico"
        self.update_system_status()
        self.set_buttons_enabled(False)
        
        def execute():
            try:
                self.add_log("📈 Exportando apenas histórico de parada...", "info")
                self.set_progress(30, "Exportando histórico...")
                
                export_result = self.tms_system.export_history_only_last_two_months()
                
                if export_result['success']:
                    self.add_log(f"✅ {export_result['message']}", "success")
                    self.add_log("✓ Histórico de Parada exportado para C:\\TMSDATA\\stop_history\\", "info")
                    self.set_progress(100, f"History exportado em {export_result['elapsed_time']:.1f}s")
                else:
                    self.add_log(f"❌ {export_result['message']}", "error")
                    self.set_progress(100, "Falha na exportação")
                
                # Atualiza status
                self.show_csv_summary()
                self.show_available_months()
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
        
        threading.Thread(target=execute, daemon=True).start()
    
    def run_export_forecast(self):
        """Exporta apenas forecast (com dados mínimos)"""
        if self.is_running:
            self.add_log("⚠️ Operação já em andamento", "warning")
            return
        
        self.is_running = True
        self.current_operation = "Exportação Previsão"
        self.update_system_status()
        self.set_buttons_enabled(False)
        
        def execute():
            try:
                self.add_log("📊 Exportando apenas previsão...", "info")
                self.set_progress(30, "Exportando previsão...")
                
                # Para exportar apenas forecast, precisa de pelo menos um mês de algum tipo
                # Vamos usar um mês de shift (não vai exportar os arquivos, só habilita o forecast)
                export_result = self.tms_system.export_csv_tms_format(
                    shift_months=[],  # Nenhum mês de shift
                    operator_months=[],  # Nenhum mês de operator
                    history_months=[],  # Nenhum mês de history
                    include_forecast=True  # Apenas forecast
                )
                
                if export_result['success']:
                    self.add_log(f"✅ {export_result['message']}", "success")
                    self.add_log("✓ Previsão (forecast.csv) exportada para C:\\TMSDATA\\", "info")
                    self.set_progress(100, f"Forecast exportado em {export_result['elapsed_time']:.1f}s")
                else:
                    self.add_log(f"❌ {export_result['message']}", "error")
                    self.set_progress(100, "Falha na exportação")
                
                # Atualiza status
                self.show_csv_summary()
                self.show_available_months()
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
        
        threading.Thread(target=execute, daemon=True).start()
    
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
                self.show_csv_summary()
                
            except Exception as e:
                self.add_log(f"❌ Erro na limpeza: {str(e)}", "error")
        
        threading.Thread(target=execute_cleanup, daemon=True).start()
    
    def set_buttons_enabled(self, enabled: bool):
        """Habilita/desabilita botões"""
        buttons = [
            self.etl_btn, self.import_btn, self.collect_btn, 
            self.export_all_btn, self.export_daily_btn, self.export_operator_btn,
            self.export_history_btn, self.export_forecast_btn, self.cleanup_btn
        ]
        
        for btn in buttons:
            btn.disabled = not enabled
        
        self.page.update()
    
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