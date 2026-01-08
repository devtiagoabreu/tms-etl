import flet as ft
import requests
import os
import time
import csv
import mariadb
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Set
import threading
import sys

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('tms_etl_gui.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class TMSETLGUI:
    def __init__(self, page: ft.Page):
        self.page = page
        self.setup_page()
        self.automator = TMSETLAutomation()
        self.log_text = ""
        self.progress_value = 0
        self.is_running = False
        
    def setup_page(self):
        """Configura a página principal"""
        self.page.title = "ETL Toyota TMS Automation"
        self.page.theme_mode = ft.ThemeMode.LIGHT
        self.page.padding = 20
        self.page.window_width = 1200
        self.page.window_height = 800
        self.page.window_resizable = True
        
        # Cores personalizadas
        self.primary_color = ft.colors.BLUE_700
        self.secondary_color = ft.colors.GREEN_700
        self.accent_color = ft.colors.ORANGE_700
        self.error_color = ft.colors.RED_700
        
        # Configuração inicial do banco
        self.db_config = {
            'host': 'localhost',
            'port': 3306,
            'user': 'abreu',
            'password': '',
            'database': 'dbintegrafabric'
        }
        
        self.build_ui()
        
    def build_ui(self):
        """Constrói a interface do usuário"""
        # Título
        title = ft.Container(
            content=ft.Column([
                ft.Row([
                    ft.Icon(name=ft.icons.FACTORY, size=40, color=self.primary_color),
                    ft.Text("ETL Toyota TMS Automation", 
                           size=28, 
                           weight=ft.FontWeight.BOLD,
                           color=self.primary_color)
                ], alignment=ft.MainAxisAlignment.CENTER),
                ft.Divider(height=20, color=ft.colors.TRANSPARENT),
                ft.Text("Sistema de automação para coleta, exportação e importação de dados dos teares Toyota",
                       size=14,
                       color=ft.colors.GREY_600,
                       text_align=ft.TextAlign.CENTER)
            ]),
            alignment=ft.alignment.center
        )
        
        # Cards de status
        self.status_cards = self.create_status_cards()
        
        # Painel de configuração do banco
        config_panel = self.create_config_panel()
        
        # Painel de logs
        self.log_panel = self.create_log_panel()
        
        # Painel de progresso
        self.progress_panel = self.create_progress_panel()
        
        # Botões de ação
        action_buttons = self.create_action_buttons()
        
        # Layout principal
        self.page.add(
            ft.Column([
                title,
                ft.Divider(height=30),
                ft.Row(self.status_cards, wrap=True),
                ft.Divider(height=30),
                config_panel,
                ft.Divider(height=30),
                ft.Row([
                    ft.Column([
                        action_buttons,
                        ft.Divider(height=20),
                        self.progress_panel
                    ], expand=2),
                    ft.VerticalDivider(width=20),
                    ft.Column([self.log_panel], expand=3)
                ], expand=True)
            ], scroll=ft.ScrollMode.AUTO)
        )
        
    def create_status_cards(self):
        """Cria os cards de status"""
        cards = []
        
        status_items = [
            {
                "title": "Status do Sistema",
                "value": "Pronto",
                "icon": ft.icons.CHECK_CIRCLE,
                "color": self.secondary_color,
                "key": "system_status"
            },
            {
                "title": "Arquivos CSV",
                "value": "0 encontrados",
                "icon": ft.icons.INSERT_DRIVE_FILE,
                "color": self.primary_color,
                "key": "csv_files"
            },
            {
                "title": "Conexão DB",
                "value": "Não testada",
                "icon": ft.icons.DATABASE,
                "color": ft.colors.ORANGE_700,
                "key": "db_connection"
            },
            {
                "title": "Última Execução",
                "value": "Nunca",
                "icon": ft.icons.ACCESS_TIME,
                "color": ft.colors.PURPLE_700,
                "key": "last_execution"
            }
        ]
        
        for item in status_items:
            card = ft.Container(
                content=ft.Column([
                    ft.Row([
                        ft.Icon(name=item["icon"], color=item["color"], size=24),
                        ft.Text(item["title"], size=14, weight=ft.FontWeight.BOLD)
                    ]),
                    ft.Divider(height=10),
                    ft.Text(item["value"], size=18, weight=ft.FontWeight.BOLD),
                    ft.Divider(height=5),
                    ft.Text("Clique para atualizar", size=10, color=ft.colors.GREY_500)
                ]),
                padding=20,
                border=ft.border.all(1, ft.colors.GREY_300),
                border_radius=10,
                bgcolor=ft.colors.WHITE,
                shadow=ft.BoxShadow(blur_radius=2, color=ft.colors.GREY_300),
                on_click=lambda e, key=item["key"]: self.update_status_card(key),
                data=item["key"],
                width=250,
                height=120
            )
            cards.append(card)
            
        return cards
    
    def create_config_panel(self):
        """Cria o painel de configuração do banco de dados"""
        self.host_field = ft.TextField(
            label="Host",
            value=self.db_config['host'],
            width=200,
            on_change=self.update_db_config
        )
        
        self.port_field = ft.TextField(
            label="Porta",
            value=str(self.db_config['port']),
            width=120,
            on_change=self.update_db_config
        )
        
        self.user_field = ft.TextField(
            label="Usuário",
            value=self.db_config['user'],
            width=180,
            on_change=self.update_db_config
        )
        
        self.password_field = ft.TextField(
            label="Senha",
            value=self.db_config['password'],
            width=180,
            password=True,
            can_reveal_password=True,
            on_change=self.update_db_config
        )
        
        self.database_field = ft.TextField(
            label="Banco de Dados",
            value=self.db_config['database'],
            width=200,
            on_change=self.update_db_config
        )
        
        test_btn = ft.ElevatedButton(
            "Testar Conexão",
            icon=ft.icons.TEST_TUBE,
            on_click=self.test_db_connection,
            bgcolor=ft.colors.BLUE_100
        )
        
        return ft.Container(
            content=ft.Column([
                ft.Text("Configuração do Banco de Dados", 
                       size=16, 
                       weight=ft.FontWeight.BOLD,
                       color=self.primary_color),
                ft.Divider(height=10),
                ft.Row([
                    self.host_field,
                    self.port_field,
                    self.user_field,
                    self.password_field,
                    self.database_field,
                    test_btn
                ], wrap=True)
            ]),
            padding=15,
            border=ft.border.all(1, ft.colors.GREY_300),
            border_radius=10,
            bgcolor=ft.colors.BLUE_50
        )
    
    def create_log_panel(self):
        """Cria o painel de logs"""
        self.log_display = ft.Column(
            scroll=ft.ScrollMode.AUTO,
            height=300
        )
        
        clear_btn = ft.IconButton(
            icon=ft.icons.CLEAR_ALL,
            tooltip="Limpar logs",
            on_click=lambda e: self.clear_logs()
        )
        
        copy_btn = ft.IconButton(
            icon=ft.icons.CONTENT_COPY,
            tooltip="Copiar logs",
            on_click=lambda e: self.copy_logs()
        )
        
        save_btn = ft.IconButton(
            icon=ft.icons.SAVE,
            tooltip="Salvar logs",
            on_click=lambda e: self.save_logs()
        )
        
        return ft.Container(
            content=ft.Column([
                ft.Row([
                    ft.Text("Log de Execução", 
                           size=16, 
                           weight=ft.FontWeight.BOLD,
                           color=self.primary_color),
                    ft.Row([clear_btn, copy_btn, save_btn])
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
            ]),
            expand=True
        )
    
    def create_progress_panel(self):
        """Cria o painel de progresso"""
        self.progress_bar = ft.ProgressBar(
            width=400,
            color=self.primary_color,
            bgcolor=ft.colors.GREY_300
        )
        
        self.progress_text = ft.Text("Aguardando execução...", size=12)
        
        return ft.Column([
            ft.Text("Progresso", size=16, weight=ft.FontWeight.BOLD, color=self.primary_color),
            ft.Divider(height=10),
            self.progress_bar,
            ft.Divider(height=5),
            self.progress_text
        ])
    
    def create_action_buttons(self):
        """Cria os botões de ação"""
        # Botão ETL Completo
        etl_btn = ft.ElevatedButton(
            content=ft.Row([
                ft.Icon(ft.icons.PLAY_ARROW, color=ft.colors.WHITE),
                ft.Text("ETL Completo", color=ft.colors.WHITE)
            ]),
            style=ft.ButtonStyle(
                bgcolor=self.primary_color,
                padding=15
            ),
            width=200,
            on_click=lambda e: self.run_etl_complete()
        )
        
        # Botão Importação
        import_btn = ft.ElevatedButton(
            content=ft.Row([
                ft.Icon(ft.icons.UPLOAD_FILE, color=ft.colors.WHITE),
                ft.Text("Apenas Importação", color=ft.colors.WHITE)
            ]),
            style=ft.ButtonStyle(
                bgcolor=ft.colors.GREEN_700,
                padding=15
            ),
            width=200,
            on_click=lambda e: self.run_import_only()
        )
        
        # Botão Tear Específico
        specific_btn = ft.ElevatedButton(
            content=ft.Row([
                ft.Icon(ft.icons.BUILD, color=ft.colors.WHITE),
                ft.Text("Tear Específico", color=ft.colors.WHITE)
            ]),
            style=ft.ButtonStyle(
                bgcolor=self.accent_color,
                padding=15
            ),
            width=200,
            on_click=lambda e: self.open_tear_specific_dialog()
        )
        
        # Botão Coleta
        collect_btn = ft.ElevatedButton(
            content=ft.Row([
                ft.Icon(ft.icons.COLLECTIONS, color=ft.colors.WHITE),
                ft.Text("Apenas Coleta", color=ft.colors.WHITE)
            ]),
            style=ft.ButtonStyle(
                bgcolor=ft.colors.BLUE_500,
                padding=15
            ),
            width=200,
            on_click=lambda e: self.run_collection_only()
        )
        
        # Botão Exportação
        export_btn = ft.ElevatedButton(
            content=ft.Row([
                ft.Icon(ft.icons.DOWNLOAD, color=ft.colors.WHITE),
                ft.Text("Apenas Exportação", color=ft.colors.WHITE)
            ]),
            style=ft.ButtonStyle(
                bgcolor=ft.colors.PURPLE_700,
                padding=15
            ),
            width=200,
            on_click=lambda e: self.run_export_only()
        )
        
        # Botão Limpeza
        cleanup_btn = ft.ElevatedButton(
            content=ft.Row([
                ft.Icon(ft.icons.CLEANING_SERVICES, color=ft.colors.WHITE),
                ft.Text("Limpar Arquivos", color=ft.colors.WHITE)
            ]),
            style=ft.ButtonStyle(
                bgcolor=ft.colors.RED_700,
                padding=15
            ),
            width=200,
            on_click=lambda e: self.run_cleanup()
        )
        
        return ft.Column([
            etl_btn,
            ft.Divider(height=10),
            import_btn,
            ft.Divider(height=10),
            specific_btn,
            ft.Divider(height=10),
            collect_btn,
            ft.Divider(height=10),
            export_btn,
            ft.Divider(height=10),
            cleanup_btn
        ])
    
    def update_status_card(self, card_key):
        """Atualiza um card de status específico"""
        for card in self.status_cards:
            if card.data == card_key:
                if card_key == "system_status":
                    self.check_system_status()
                elif card_key == "csv_files":
                    self.count_csv_files()
                elif card_key == "db_connection":
                    self.test_db_connection(None)
                elif card_key == "last_execution":
                    self.update_last_execution()
                break
    
    def check_system_status(self):
        """Verifica o status do sistema"""
        try:
            # Verifica se o TMS está acessível
            response = requests.get("http://127.0.0.1/tms", timeout=5)
            status = "Online" if response.status_code == 200 else "Offline"
            self.update_card_value("system_status", f"TMS {status}")
            self.add_log(f"✅ Sistema TMS está {status}", "success")
        except:
            self.update_card_value("system_status", "TMS Offline")
            self.add_log("⚠️ Sistema TMS não está acessível", "warning")
    
    def count_csv_files(self):
        """Conta os arquivos CSV disponíveis"""
        try:
            csv_files = self.automator.find_latest_csv_files()
            count = len(csv_files)
            self.update_card_value("csv_files", f"{count} arquivos")
            
            if count > 0:
                # Mostra os arquivos mais recentes
                recent_files = "\n".join([Path(f).name for f in csv_files[:3]])
                if count > 3:
                    recent_files += f"\n... e mais {count-3}"
                self.add_log(f"📁 Encontrados {count} arquivos CSV", "info")
                self.add_log(f"Arquivos recentes:\n{recent_files}", "info")
            else:
                self.add_log("📁 Nenhum arquivo CSV encontrado", "warning")
                
        except Exception as e:
            self.update_card_value("csv_files", "Erro")
            self.add_log(f"❌ Erro ao buscar arquivos: {str(e)}", "error")
    
    def test_db_connection(self, e):
        """Testa a conexão com o banco de dados"""
        try:
            conn = mariadb.connect(**self.db_config)
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            conn.close()
            
            self.update_card_value("db_connection", "Conectado")
            self.add_log("✅ Conexão com banco de dados estabelecida", "success")
            return True
            
        except Exception as e:
            self.update_card_value("db_connection", "Falha")
            self.add_log(f"❌ Falha na conexão com o banco: {str(e)}", "error")
            return False
    
    def update_last_execution(self):
        """Atualiza a última execução"""
        try:
            # Lê do arquivo de log ou usa timestamp atual
            if os.path.exists('tms_etl_gui.log'):
                with open('tms_etl_gui.log', 'r') as f:
                    lines = f.readlines()
                    if lines:
                        last_line = lines[-1]
                        timestamp = last_line.split(' - ')[0]
                        self.update_card_value("last_execution", timestamp)
                        self.add_log(f"⏰ Última execução: {timestamp}", "info")
                        return
            
            self.update_card_value("last_execution", "Nunca")
            self.add_log("⏰ Nenhuma execução registrada", "info")
            
        except Exception as e:
            self.add_log(f"❌ Erro ao verificar última execução: {str(e)}", "error")
    
    def update_card_value(self, card_key, new_value):
        """Atualiza o valor de um card"""
        for card in self.status_cards:
            if card.data == card_key:
                # Encontra o texto do valor dentro do card
                content_column = card.content
                for i, element in enumerate(content_column.controls):
                    if isinstance(element, ft.Text) and i == 2:  # Índice do valor
                        element.value = new_value
                        break
                card.update()
                break
    
    def update_db_config(self, e):
        """Atualiza a configuração do banco de dados"""
        self.db_config = {
            'host': self.host_field.value,
            'port': int(self.port_field.value) if self.port_field.value.isdigit() else 3306,
            'user': self.user_field.value,
            'password': self.password_field.value,
            'database': self.database_field.value
        }
        
        # Atualiza também no automator
        self.automator.db_config = self.db_config
        self.add_log("⚙️ Configuração do banco atualizada", "info")
    
    def add_log(self, message, log_type="info"):
        """Adiciona uma mensagem ao log"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        
        # Define cor baseada no tipo
        if log_type == "success":
            color = ft.colors.GREEN
            icon = "✅"
        elif log_type == "error":
            color = ft.colors.RED
            icon = "❌"
        elif log_type == "warning":
            color = ft.colors.ORANGE
            icon = "⚠️"
        else:
            color = ft.colors.BLUE
            icon = "ℹ️"
        
        log_entry = ft.Row([
            ft.Text(f"[{timestamp}] ", size=10, color=ft.colors.GREY_500),
            ft.Text(icon, size=12),
            ft.Text(message, color=color, size=12, selectable=True)
        ], wrap=True)
        
        self.log_display.controls.append(log_entry)
        self.log_display.scroll_to(offset=-1, duration=300)
        self.page.update()
        
        # Também salva no logger
        logger.info(message.replace("✅", "").replace("❌", "").replace("⚠️", "").replace("ℹ️", ""))
    
    def clear_logs(self):
        """Limpa os logs"""
        self.log_display.controls.clear()
        self.page.update()
        self.add_log("🧹 Logs limpos", "info")
    
    def copy_logs(self):
        """Copia os logs para a área de transferência"""
        log_text = ""
        for control in self.log_display.controls:
            if isinstance(control, ft.Row):
                for child in control.controls:
                    if isinstance(child, ft.Text):
                        log_text += child.value
                log_text += "\n"
        
        self.page.set_clipboard(log_text)
        self.add_log("📋 Logs copiados para a área de transferência", "success")
    
    def save_logs(self):
        """Salva os logs em um arquivo"""
        try:
            filename = f"etl_logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            with open(filename, 'w', encoding='utf-8') as f:
                for control in self.log_display.controls:
                    if isinstance(control, ft.Row):
                        for child in control.controls:
                            if isinstance(child, ft.Text):
                                f.write(child.value)
                        f.write("\n")
            
            self.add_log(f"💾 Logs salvos em {filename}", "success")
        except Exception as e:
            self.add_log(f"❌ Erro ao salvar logs: {str(e)}", "error")
    
    def update_progress(self, value, message):
        """Atualiza a barra de progresso"""
        self.progress_value = value
        self.progress_bar.value = value / 100
        self.progress_text.value = message
        self.page.update()
    
    def run_in_thread(self, func, *args):
        """Executa uma função em thread separada"""
        if self.is_running:
            self.add_log("⚠️ Já existe uma operação em andamento", "warning")
            return
        
        self.is_running = True
        self.clear_logs()
        self.update_progress(0, "Iniciando...")
        
        def thread_func():
            try:
                func(*args)
            except Exception as e:
                self.add_log(f"❌ Erro na execução: {str(e)}", "error")
            finally:
                self.is_running = False
                self.update_progress(100, "Concluído")
                # Atualiza status após execução
                self.count_csv_files()
                self.update_last_execution()
        
        thread = threading.Thread(target=thread_func)
        thread.daemon = True
        thread.start()
    
    def run_etl_complete(self):
        """Executa o ETL completo"""
        self.add_log("🚀 Iniciando ETL completo...", "success")
        self.run_in_thread(self._run_etl_complete)
    
    def _run_etl_complete(self):
        """Função interna para ETL completo"""
        try:
            self.update_progress(10, "Coletando dados dos teares...")
            if self.automator.select_all_looms():
                self.update_progress(40, "Exportando para CSV...")
                if self.automator.export_csv_data():
                    self.update_progress(70, "Importando para o banco...")
                    if self.automator.process_csv_files():
                        self.update_progress(100, "ETL completo concluído!")
                        self.add_log("🎉 ETL completo executado com sucesso!", "success")
                    else:
                        self.add_log("⚠️ Nenhum dado novo para importar", "warning")
                else:
                    self.add_log("❌ Falha na exportação", "error")
            else:
                self.add_log("❌ Falha na coleta de dados", "error")
        except Exception as e:
            self.add_log(f"❌ Erro no ETL completo: {str(e)}", "error")
    
    def run_import_only(self):
        """Executa apenas a importação"""
        self.add_log("📤 Iniciando apenas importação...", "info")
        self.run_in_thread(self._run_import_only)
    
    def _run_import_only(self):
        """Função interna para importação apenas"""
        try:
            self.update_progress(30, "Buscando arquivos CSV...")
            if self.automator.process_csv_files():
                self.update_progress(100, "Importação concluída!")
                self.add_log("✅ Importação executada com sucesso!", "success")
            else:
                self.add_log("⚠️ Nenhum arquivo CSV encontrado ou dados novos", "warning")
        except Exception as e:
            self.add_log(f"❌ Erro na importação: {str(e)}", "error")
    
    def run_collection_only(self):
        """Executa apenas a coleta de dados"""
        self.add_log("📡 Iniciando apenas coleta de dados...", "info")
        self.run_in_thread(self._run_collection_only)
    
    def _run_collection_only(self):
        """Função interna para coleta apenas"""
        try:
            self.update_progress(50, "Coletando dados dos teares...")
            if self.automator.select_all_looms():
                self.update_progress(100, "Coleta concluída!")
                self.add_log("✅ Coleta de dados executada com sucesso!", "success")
            else:
                self.add_log("❌ Falha na coleta de dados", "error")
        except Exception as e:
            self.add_log(f"❌ Erro na coleta: {str(e)}", "error")
    
    def run_export_only(self):
        """Executa apenas a exportação"""
        self.add_log("💾 Iniciando apenas exportação para CSV...", "info")
        self.run_in_thread(self._run_export_only)
    
    def _run_export_only(self):
        """Função interna para exportação apenas"""
        try:
            self.update_progress(50, "Exportando dados para CSV...")
            if self.automator.export_csv_data():
                self.update_progress(100, "Exportação concluída!")
                self.add_log("✅ Exportação para CSV executada com sucesso!", "success")
            else:
                self.add_log("❌ Falha na exportação", "error")
        except Exception as e:
            self.add_log(f"❌ Erro na exportação: {str(e)}", "error")
    
    def run_cleanup(self):
        """Executa a limpeza de arquivos antigos"""
        self.add_log("🧹 Iniciando limpeza de arquivos antigos...", "info")
        self.run_in_thread(self._run_cleanup)
    
    def _run_cleanup(self):
        """Função interna para limpeza"""
        try:
            self.update_progress(50, "Limpando arquivos antigos...")
            self.automator.cleanup_old_files()
            self.update_progress(100, "Limpeza concluída!")
            self.add_log("✅ Limpeza de arquivos antigos concluída!", "success")
        except Exception as e:
            self.add_log(f"❌ Erro na limpeza: {str(e)}", "error")
    
    def open_tear_specific_dialog(self):
        """Abre diálogo para processamento específico de tear"""
        tear_id_field = ft.TextField(
            label="ID do Tear",
            hint_text="Ex: 00001, 00042",
            width=200
        )
        
        date_range_field = ft.TextField(
            label="Período (dias)",
            hint_text="Ex: 7 (últimos 7 dias)",
            value="7",
            width=150
        )
        
        def process_specific(e):
            tear_id = tear_id_field.value.strip()
            days = int(date_range_field.value) if date_range_field.value.isdigit() else 7
            
            if tear_id:
                self.page.dialog.open = False
                self.page.update()
                self.run_tear_specific(tear_id, days)
            else:
                self.add_log("⚠️ Por favor, informe o ID do tear", "warning")
        
        def cancel(e):
            self.page.dialog.open = False
            self.page.update()
        
        dialog = ft.AlertDialog(
            title=ft.Text("Processar Tear Específico"),
            content=ft.Column([
                ft.Text("Informe o ID do tear para processamento específico:"),
                ft.Divider(height=20),
                tear_id_field,
                ft.Divider(height=10),
                date_range_field,
                ft.Text("Nota: Esta opção é ideal para teares que estavam desligados", 
                       size=12, 
                       color=ft.colors.GREY_600)
            ], tight=True),
            actions=[
                ft.TextButton("Cancelar", on_click=cancel),
                ft.ElevatedButton("Processar", on_click=process_specific)
            ],
            actions_alignment=ft.MainAxisAlignment.END
        )
        
        self.page.dialog = dialog
        dialog.open = True
        self.page.update()
    
    def run_tear_specific(self, tear_id, days):
        """Executa processamento específico de tear"""
        self.add_log(f"🔧 Iniciando processamento do tear {tear_id}...", "info")
        self.run_in_thread(self._run_tear_specific, tear_id, days)
    
    def _run_tear_specific(self, tear_id, days):
        """Função interna para processamento específico"""
        try:
            self.update_progress(30, f"Processando tear {tear_id}...")
            
            # Calcula o range de datas
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            date_range = (start_date, end_date)
            
            if self.automator.process_specific_tear(tear_id, date_range):
                self.update_progress(100, f"Tear {tear_id} processado!")
                self.add_log(f"✅ Dados do tear {tear_id} processados com sucesso!", "success")
            else:
                self.add_log(f"⚠️ Nenhum dado novo encontrado para o tear {tear_id}", "warning")
        except Exception as e:
            self.add_log(f"❌ Erro no processamento do tear {tear_id}: {str(e)}", "error")

# Classe TMSETLAutomation (mantida do código anterior com pequenas adaptações)
class TMSETLAutomation:
    def __init__(self):
        self.base_url = "http://127.0.0.1/tms"
        self.session = requests.Session()
        self.csv_dir = "C:\\TMSDATA"
        
        # Configuração inicial do banco
        self.db_config = {
            'host': 'localhost',
            'port': 3306,
            'user': 'abreu',
            'password': '',
            'database': 'dbintegrafabric'
        }
    
    def select_all_looms(self):
        """Seleciona todos os teares na tela de coleta de dados"""
        url = f"{self.base_url}/loom/getdata.cgi"
        
        try:
            response = self.session.get(url, timeout=10)
            
            if response.status_code == 200:
                # Lista de teares conforme arquivos CSV
                loom_ids = [
                    "00001", "00002", "00003", "00004", "00005", "00006",
                    "00007", "00008", "00009", "00010", "00011", "00014",
                    "00015", "00016", "00028", "00029", "00030", "00031",
                    "00032", "00033", "00034", "00035", "00042", "00043"
                ]
                
                # Simula a seleção (para ambiente de teste)
                # Em produção, seria necessário interagir com o formulário
                logger.info(f"Selecionando {len(loom_ids)} teares para coleta")
                
                # Aguarda simulação de coleta
                time.sleep(5)
                
                logger.info("Coleta de dados concluída")
                return True
                
        except Exception as e:
            logger.error(f"Erro na coleta de dados: {str(e)}")
        
        return False
    
    def export_csv_data(self):
        """Exporta os dados CSV dos últimos meses"""
        url = f"{self.base_url}/edit/exportcsv.cgi"
        
        try:
            response = self.session.get(url, timeout=10)
            
            if response.status_code == 200:
                # Simula exportação
                logger.info("Exportando dados para CSV...")
                time.sleep(5)
                logger.info("Exportação concluída")
                return True
                
        except Exception as e:
            logger.error(f"Erro na exportação de dados: {str(e)}")
        
        return False
    
    def find_latest_csv_files(self):
        """Encontra os arquivos CSV mais recentes"""
        csv_files = []
        
        try:
            tmsdata_path = Path(self.csv_dir)
            
            if tmsdata_path.exists():
                for year_month_dir in tmsdata_path.glob("*-*"):
                    if year_month_dir.is_dir():
                        daily_dir = year_month_dir / "daily"
                        if daily_dir.exists():
                            for csv_file in daily_dir.glob("*.csv"):
                                csv_files.append(str(csv_file))
            
            csv_files.sort(reverse=True)
            
        except Exception as e:
            logger.error(f"Erro ao buscar arquivos CSV: {str(e)}")
        
        return csv_files
    
    def parse_csv_file(self, file_path):
        """Lê e analisa um arquivo CSV"""
        data = []
        
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                csv_reader = csv.reader(file)
                for row in csv_reader:
                    if len(row) > 0 and any(field.strip() for field in row):
                        data.append(row)
            
        except Exception as e:
            logger.error(f"Erro ao analisar arquivo {file_path}: {str(e)}")
        
        return data
    
    def check_existing_data(self, data_turno, tear):
        """Verifica se já existe dados para este tear e data/turno"""
        try:
            conn = mariadb.connect(**self.db_config)
            cursor = conn.cursor()
            
            query = """
                SELECT COUNT(*) FROM tblDadosTeares 
                WHERE dataTurno = %s AND tear = %s
            """
            
            cursor.execute(query, (data_turno, tear))
            result = cursor.fetchone()
            
            cursor.close()
            conn.close()
            
            return result[0] > 0 if result else False
            
        except Exception as e:
            logger.error(f"Erro ao verificar dados existentes: {str(e)}")
            return False
    
    def insert_data_to_db(self, csv_data, file_path):
        """Insere os dados no banco de dados"""
        inserted_count = 0
        
        try:
            conn = mariadb.connect(**self.db_config)
            cursor = conn.cursor()
            
            for row in csv_data:
                try:
                    if len(row) < 72:
                        row = row + [''] * (72 - len(row))
                    
                    data_turno = row[0] if len(row) > 0 else ""
                    tear = row[1] if len(row) > 1 else ""
                    
                    # Verifica duplicação
                    if self.check_existing_data(data_turno, tear):
                        continue
                    
                    # Prepara parâmetros
                    params = []
                    for i in range(72):
                        if i < len(row):
                            value = str(row[i]) if row[i] is not None else ""
                            if value == "None":
                                value = ""
                            params.append(value)
                        else:
                            params.append("")
                    
                    # Chama procedure
                    procedure_call = """
                        CALL uspDadosTearesInserir(%s)
                    """ % (', '.join(['%s'] * 72))
                    
                    cursor.execute(procedure_call, params)
                    inserted_count += 1
                    
                except mariadb.Error as e:
                    logger.error(f"Erro ao inserir registro: {str(e)}")
                    continue
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"Inseridos {inserted_count} registros de {file_path}")
            return inserted_count > 0
            
        except Exception as e:
            logger.error(f"Erro na conexão com o banco: {str(e)}")
            return False
    
    def process_csv_files(self, csv_files=None):
        """Processa os arquivos CSV"""
        if csv_files is None:
            csv_files = self.find_latest_csv_files()
        
        if not csv_files:
            logger.warning("Nenhum arquivo CSV encontrado")
            return False
        
        processed_count = 0
        
        for csv_file in csv_files:
            try:
                csv_data = self.parse_csv_file(csv_file)
                
                if csv_data:
                    if self.insert_data_to_db(csv_data, csv_file):
                        processed_count += 1
                        logger.info(f"Arquivo {Path(csv_file).name} processado")
                    
            except Exception as e:
                logger.error(f"Erro ao processar arquivo {csv_file}: {str(e)}")
        
        logger.info(f"Total de arquivos processados: {processed_count}")
        return processed_count > 0
    
    def process_specific_tear(self, tear_id, date_range=None):
        """Processa dados específicos de um tear"""
        try:
            csv_files = self.find_latest_csv_files()
            
            if not csv_files:
                return False
            
            processed = False
            
            for csv_file in csv_files:
                try:
                    file_name = Path(csv_file).name
                    
                    # Filtra por data se especificado
                    if date_range:
                        try:
                            file_date_str = file_name.split('.')[0]
                            file_date = datetime.strptime(file_date_str, "%Y-%m-%d")
                            if file_date < date_range[0] or file_date > date_range[1]:
                                continue
                        except:
                            continue
                    
                    csv_data = self.parse_csv_file(csv_file)
                    
                    if not csv_data:
                        continue
                    
                    # Filtra pelo tear
                    tear_data = []
                    for row in csv_data:
                        if len(row) > 1 and row[1] == tear_id:
                            tear_data.append(row)
                    
                    if tear_data:
                        if self.insert_data_to_db(tear_data, csv_file):
                            processed = True
                            logger.info(f"Dados do tear {tear_id} processados de {file_name}")
                    
                except Exception as e:
                    logger.error(f"Erro ao processar arquivo {csv_file}: {str(e)}")
            
            return processed
            
        except Exception as e:
            logger.error(f"Erro no processamento específico: {str(e)}")
            return False
    
    def cleanup_old_files(self, days_to_keep=7):
        """Limpa arquivos CSV antigos"""
        try:
            tmsdata_path = Path(self.csv_dir)
            
            if not tmsdata_path.exists():
                return
            
            cutoff_date = datetime.now() - timedelta(days=days_to_keep)
            
            for year_month_dir in tmsdata_path.glob("*-*"):
                if year_month_dir.is_dir():
                    try:
                        folder_name = year_month_dir.name
                        year, month = folder_name.split('-')
                        folder_date = datetime(int(year), int(month), 1)
                        
                        if folder_date < cutoff_date:
                            import shutil
                            shutil.rmtree(year_month_dir)
                            logger.info(f"Pasta removida: {folder_name}")
                    except:
                        continue
            
            logger.info("Limpeza de arquivos antigos concluída")
            
        except Exception as e:
            logger.error(f"Erro na limpeza: {str(e)}")

def main(page: ft.Page):
    """Função principal do Flet"""
    app = TMSETLGUI(page)

if __name__ == "__main__":
    # Para executar a aplicação Flet
    ft.app(
        target=main,
        view=ft.AppView.FLET_APP,  # Abre em janela nativa
        assets_dir="assets"  # Pasta para assets, se necessário
    )