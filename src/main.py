import flet as ft
import mariadb
import os
import time
import logging
from datetime import datetime
from pathlib import Path
import threading

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
        
    def setup_page(self):
        """Configura a página principal"""
        self.page.title = "ETL Toyota TMS Automation"
        self.page.theme_mode = ft.ThemeMode.LIGHT
        self.page.window_width = 1000
        self.page.window_height = 700
        self.page.window_resizable = True
        self.page.padding = 0
        self.page.spacing = 0
        
        # Configuração do banco
        self.db_config = {
            'host': 'localhost',
            'port': 3306,
            'user': 'abreu',
            'password': 'dqgh3ffrdg',
            'database': 'dbintegrafabric'
        }
        
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
        # Card 1: Status do Sistema
        system_card = self.create_status_card(
            "Status do Sistema",
            "Pronto",
            ft.icons.CHECK_CIRCLE_OUTLINE,
            green_color
        )
        
        # Card 2: Arquivos CSV
        self.csv_card = self.create_status_card(
            "Arquivos CSV",
            "0 encontrados",
            ft.icons.INSERT_DRIVE_FILE,
            primary_color
        )
        
        # Card 3: Conexão DB
        self.db_card = self.create_status_card(
            "Conexão DB",
            "Não testada",
            ft.icons.STORAGE,
            orange_color
        )
        
        # Card 4: Última Execução
        self.last_exec_card = self.create_status_card(
            "Última Execução",
            "Nunca",
            ft.icons.ACCESS_TIME,
            purple_color
        )
        
        status_row = ft.Row(
            controls=[system_card, self.csv_card, self.db_card, self.last_exec_card],
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
            value=self.db_config['host'],
            width=150,
            height=40,
            content_padding=10,
            border_color=primary_color
        )
        
        self.port_field = ft.TextField(
            label="Porta",
            value=str(self.db_config['port']),
            width=80,
            height=40,
            content_padding=10,
            border_color=primary_color
        )
        
        self.user_field = ft.TextField(
            label="Usuário",
            value=self.db_config['user'],
            width=130,
            height=40,
            content_padding=10,
            border_color=primary_color
        )
        
        self.password_field = ft.TextField(
            label="Senha",
            value=self.db_config['password'],
            width=130,
            height=40,
            content_padding=10,
            password=True,
            can_reveal_password=True,
            border_color=primary_color
        )
        
        self.database_field = ft.TextField(
            label="Banco de Dados",
            value=self.db_config['database'],
            width=150,
            height=40,
            content_padding=10,
            border_color=primary_color
        )
        
        test_btn = ft.ElevatedButton(
            "Testar Conexão",
            icon=ft.icons.SETTINGS_ETHERNET,
            on_click=self.test_db_connection,
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
        # Botão ETL Completo
        etl_btn = ft.ElevatedButton(
            text="ETL Completo",
            icon=ft.icons.PLAY_ARROW,
            style=ft.ButtonStyle(
                bgcolor=green_color,
                color=ft.colors.WHITE,
                padding=ft.padding.symmetric(horizontal=20, vertical=12),
                shape=ft.RoundedRectangleBorder(radius=8)
            ),
            width=200,
            on_click=lambda e: self.add_log("🚀 ETL Completo iniciado", "info")
        )
        
        # Botão Importação
        import_btn = ft.ElevatedButton(
            text="Apenas Importação",
            icon=ft.icons.UPLOAD_FILE,
            style=ft.ButtonStyle(
                bgcolor=primary_color,
                color=ft.colors.WHITE,
                padding=ft.padding.symmetric(horizontal=20, vertical=12),
                shape=ft.RoundedRectangleBorder(radius=8)
            ),
            width=200,
            on_click=lambda e: self.add_log("📤 Importação iniciada", "info")
        )
        
        # Botão Tear Específico
        specific_btn = ft.ElevatedButton(
            text="Tear Específico",
            icon=ft.icons.BUILD,
            style=ft.ButtonStyle(
                bgcolor=orange_color,
                color=ft.colors.WHITE,
                padding=ft.padding.symmetric(horizontal=20, vertical=12),
                shape=ft.RoundedRectangleBorder(radius=8)
            ),
            width=200,
            on_click=lambda e: self.add_log("🔧 Tear específico selecionado", "info")
        )
        
        # Botão Coleta
        collect_btn = ft.ElevatedButton(
            text="Apenas Coleta",
            icon=ft.icons.COLLECTIONS_BOOKMARK,
            style=ft.ButtonStyle(
                bgcolor=ft.colors.BLUE_500,
                color=ft.colors.WHITE,
                padding=ft.padding.symmetric(horizontal=20, vertical=12),
                shape=ft.RoundedRectangleBorder(radius=8)
            ),
            width=200,
            on_click=lambda e: self.add_log("📡 Coleta iniciada", "info")
        )
        
        # Botão Exportação
        export_btn = ft.ElevatedButton(
            text="Apenas Exportação",
            icon=ft.icons.FILE_DOWNLOAD,
            style=ft.ButtonStyle(
                bgcolor=purple_color,
                color=ft.colors.WHITE,
                padding=ft.padding.symmetric(horizontal=20, vertical=12),
                shape=ft.RoundedRectangleBorder(radius=8)
            ),
            width=200,
            on_click=lambda e: self.add_log("💾 Exportação iniciada", "info")
        )
        
        # Botão Limpeza
        cleanup_btn = ft.ElevatedButton(
            text="Limpar Arquivos",
            icon=ft.icons.CLEANING_SERVICES,
            style=ft.ButtonStyle(
                bgcolor=ft.colors.RED_700,
                color=ft.colors.WHITE,
                padding=ft.padding.symmetric(horizontal=20, vertical=12),
                shape=ft.RoundedRectangleBorder(radius=8)
            ),
            width=200,
            on_click=lambda e: self.add_log("🧹 Limpeza iniciada", "info")
        )
        
        # Layout dos botões em grid
        button_grid = ft.Column(
            controls=[
                ft.Row([etl_btn, import_btn], spacing=10),
                ft.Row([specific_btn, collect_btn], spacing=10),
                ft.Row([export_btn, cleanup_btn], spacing=10)
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
        
        # 5. ÁREA DE LOGS
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
        
        # 6. LAYOUT PRINCIPAL
        main_content = ft.Column(
            controls=[
                title_section,
                ft.Divider(height=0, color=ft.colors.GREY_300),
                status_section,
                ft.Divider(height=0, color=ft.colors.GREY_300),
                ft.Container(config_section, padding=15),
                ft.Row(
                    controls=[
                        actions_section,
                        ft.VerticalDivider(width=20, color=ft.colors.GREY_300),
                        log_section
                    ],
                    expand=True,
                    spacing=0
                )
            ],
            spacing=0,
            expand=True
        )
        
        # Container principal SEM scroll
        main_container = ft.Container(
            content=main_content,
            expand=True,
            padding=0
        )
        
        self.page.add(main_container)
        
        # Atualizações iniciais
        self.page.update()
        self.initial_updates()
        
    def create_status_card(self, title, value, icon, color):
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
            on_click=self.update_card_status
        )
    
    def initial_updates(self):
        """Executa atualizações iniciais"""
        def run_updates():
            time.sleep(1)
            self.count_csv_files()
            time.sleep(0.5)
            self.test_db_connection(None)
            self.update_last_execution()
        
        threading.Thread(target=run_updates, daemon=True).start()
    
    def count_csv_files(self):
        """Conta arquivos CSV"""
        try:
            csv_dir = Path("C:\\TMSDATA")
            count = 0
            
            if csv_dir.exists():
                for ext in ['*.csv', '*.CSV']:
                    try:
                        files = list(csv_dir.rglob(ext))
                        count += len(files)
                    except:
                        pass
            
            # Atualiza o card
            self.update_card(self.csv_card, f"{count} arquivos")
            
            if count > 0:
                self.add_log(f"📁 Encontrados {count} arquivos CSV", "success")
            else:
                self.add_log("📁 Nenhum arquivo CSV encontrado", "warning")
                
        except Exception as e:
            self.update_card(self.csv_card, "Erro")
            self.add_log(f"❌ Erro ao contar arquivos: {str(e)}", "error")
    
    def test_db_connection(self, e):
        """Testa a conexão com o banco"""
        try:
            conn = mariadb.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                database=self.db_config['database']
            )
            
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            
            if result and result[0] == 1:
                self.update_card(self.db_card, "Conectado")
                self.add_log("✅ Conexão com banco estabelecida", "success")
                return True
                
        except Exception as e:
            self.update_card(self.db_card, "Falha")
            self.add_log(f"❌ Falha na conexão: {str(e)}", "error")
            return False
        
        return False
    
    def update_last_execution(self):
        """Atualiza última execução"""
        try:
            if os.path.exists('tms_etl_gui.log'):
                with open('tms_etl_gui.log', 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                    if lines:
                        last_line = lines[-1]
                        timestamp = last_line.split(' - ')[0]
                        self.update_card(self.last_exec_card, timestamp[:16])
                        return
            
            self.update_card(self.last_exec_card, "Nunca")
            
        except Exception as e:
            self.add_log(f"Erro ao verificar última execução: {str(e)}", "error")
    
    def update_card(self, card, new_value):
        """Atualiza o valor de um card"""
        if hasattr(card, 'content'):
            content = card.content
            if isinstance(content, ft.Column):
                for control in content.controls:
                    if isinstance(control, ft.Text) and len(control.value) > 10:  # É o valor
                        control.value = new_value
                        break
        
        self.page.update()
    
    def update_card_status(self, e):
        """Atualiza card quando clicado"""
        card = e.control
        self.add_log(f"🔄 Atualizando card...", "info")
        self.page.update()
    
    def add_log(self, message, log_type="info"):
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
        else:
            color = ft.colors.BLUE_400
            icon = "ℹ️"
        
        log_entry = ft.Row([
            ft.Text(f"[{timestamp}]", size=10, color=ft.colors.GREY_400, width=60),
            ft.Text(icon, size=12, width=20),
            ft.Text(message, color=color, size=12, selectable=True, expand=True)
        ], spacing=5)
        
        self.log_display.controls.append(log_entry)
        
        # Mantém apenas os últimos 50 logs
        if len(self.log_display.controls) > 50:
            self.log_display.controls = self.log_display.controls[-50:]
        
        self.page.update()
        logger.info(message)
    
    def clear_logs(self):
        """Limpa os logs"""
        self.log_display.controls.clear()
        self.page.update()
        self.add_log("🧹 Logs limpos", "info")
    
    def copy_logs(self):
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
    
    def save_logs(self):
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


def main(page: ft.Page):
    """Função principal"""
    # Configurações básicas da página
    page.title = "ETL Toyota TMS Automation"
    page.theme_mode = ft.ThemeMode.LIGHT
    page.window_width = 1000
    page.window_height = 700
    page.window_resizable = True
    page.padding = 0
    
    try:
        app = TMSETLGUI(page)
    except Exception as e:
        page.add(
            ft.Column([
                ft.Text("Erro ao iniciar", size=20, color=ft.colors.RED, weight=ft.FontWeight.BOLD),
                ft.Text(str(e), size=12, color=ft.colors.RED),
                ft.ElevatedButton("Recarregar", on_click=lambda e: page.go("/"))
            ], alignment=ft.MainAxisAlignment.CENTER, horizontal_alignment=ft.CrossAxisAlignment.CENTER)
        )
        logger.error(f"Erro: {str(e)}")


if __name__ == "__main__":
    ft.app(target=main)