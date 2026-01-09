import flet as ft
import requests
from bs4 import BeautifulSoup
import re
import time
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse
import threading

class CSVExporterApp:
    def __init__(self):
        self.base_url = "http://127.0.0.1/tms/edit/"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
    def get_current_and_previous_month(self):
        """Retorna os meses atual e anterior no formato yyyy/mm"""
        now = datetime.now()
        current_month = now.strftime("%Y/%m")
        
        # Mês anterior
        if now.month == 1:
            previous_month = datetime(now.year - 1, 12, 1).strftime("%Y/%m")
        else:
            previous_month = datetime(now.year, now.month - 1, 1).strftime("%Y/%m")
            
        return current_month, previous_month
    
    def get_months_from_page(self, soup, column_name):
        """Extrai os meses disponíveis de uma coluna específica"""
        months = []
        
        # Procura a tabela e a coluna específica
        tables = soup.find_all('table')
        for table in tables:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all(['td', 'th'])
                for i, cell in enumerate(cells):
                    if column_name in cell.text:
                        # Procura o select na mesma linha ou próxima
                        select = row.find('select', {'name': self.get_select_name(column_name)})
                        if select:
                            for option in select.find_all('option'):
                                month_value = option.get('value') or option.text.strip()
                                if month_value and re.match(r'\d{4}\.\d{2}', month_value):
                                    months.append(month_value.replace('.', '/'))
        return list(set(months))
    
    def get_select_name(self, column_name):
        """Mapeia o nome da coluna para o nome do campo select"""
        mapping = {
            'Dados do Turno': 'shift',
            'Dados do Operador': 'operator',
            'Histórico de Parada': 'history'
        }
        for key, value in mapping.items():
            if key in column_name:
                return value
        return ''
    
    def select_months(self, soup, column_name, months_to_select):
        """Modifica o HTML para selecionar os meses específicos"""
        select_name = self.get_select_name(column_name)
        if not select_name:
            return soup
        
        select = soup.find('select', {'name': select_name})
        if select:
            for option in select.find_all('option'):
                option_text = option.text.strip()
                option_value = option.get('value') or option_text
                formatted_value = option_value.replace('.', '/')
                
                # Verifica se deve selecionar este mês
                should_select = any(
                    month in formatted_value 
                    for month in months_to_select
                )
                
                if should_select:
                    option['selected'] = 'selected'
                elif 'selected' in option.attrs:
                    del option['selected']
        
        return soup
    
    def export_data(self, page, progress_bar, status_text, results_text):
        """Executa todo o processo de exportação"""
        try:
            # Atualiza status
            status_text.value = "Acessando página de exportação..."
            page.update()
            
            # Acessa a página inicial de exportação
            url = urljoin(self.base_url, "exportcsv.cgi")
            response = self.session.get(url)
            
            if response.status_code != 200:
                status_text.value = f"Erro ao acessar página: {response.status_code}"
                page.update()
                return
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Obtém meses atual e anterior
            current_month, previous_month = self.get_current_and_previous_month()
            months_to_select = [current_month, previous_month]
            
            status_text.value = f"Selecionando meses: {current_month} e {previous_month}"
            page.update()
            
            # Seleciona os meses em todas as colunas
            columns = ['Dados do Turno', 'Dados do Operador', 'Histórico de Parada']
            for column in columns:
                soup = self.select_months(soup, column, months_to_select)
            
            # Marca o checkbox de inventário
            checkbox = soup.find('input', {'type': 'checkbox', 'name': 'forecast'})
            if checkbox:
                checkbox['checked'] = 'checked'
            
            # Prepara os dados para envio
            form_data = {}
            
            # Adiciona meses selecionados
            for column in columns:
                select_name = self.get_select_name(column)
                select = soup.find('select', {'name': select_name})
                if select:
                    selected_values = [
                        option.get('value') or option.text.strip().replace('/', '.')
                        for option in select.find_all('option', selected=True)
                    ]
                    if selected_values:
                        form_data[select_name] = selected_values
            
            # Adiciona checkbox
            if checkbox and checkbox.get('checked'):
                form_data['forecast'] = 'on'
            
            # Adiciona outros campos do formulário
            hidden_inputs = soup.find_all('input', {'type': 'hidden'})
            for hidden in hidden_inputs:
                if hidden.get('name'):
                    form_data[hidden['name']] = hidden.get('value', '')
            
            status_text.value = "Enviando dados para exportação..."
            progress_bar.value = 0.3
            page.update()
            
            # Envia o formulário para exportcsv2.cgi
            export_url = urljoin(self.base_url, "exportcsv2.cgi")
            response = self.session.post(export_url, data=form_data)
            
            if response.status_code != 200:
                status_text.value = f"Erro ao iniciar exportação: {response.status_code}"
                page.update()
                return
            
            status_text.value = "Exportação em andamento..."
            progress_bar.value = 0.6
            page.update()
            
            # Aguarda um tempo para processamento
            time.sleep(5)
            
            # Verifica página de conclusão
            final_url = urljoin(self.base_url, "exportcsv3.cgi")
            response = self.session.get(final_url)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                if "Exportacao Concluida" in soup.text or "EXPORT_DONE" in soup.text:
                    status_text.value = "✅ Exportação concluída com sucesso!"
                    progress_bar.value = 1.0
                    
                    # Extrai informações da página de conclusão
                    result_info = []
                    
                    # Procura pela pasta salva
                    folder_text = soup.find(string=re.compile(r'C:\\TMSDATA|SAVE_FOLDER', re.I))
                    if folder_text:
                        result_info.append(f"Pasta: {folder_text.strip()}")
                    
                    # Adiciona informações gerais
                    result_info.append(f"Meses exportados: {current_month}, {previous_month}")
                    result_info.append("Inventário de Fio e Previsão: ✓ SIM")
                    result_info.append(f"Data/hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                    
                    results_text.value = "\n".join(result_info)
                else:
                    status_text.value = "Exportação realizada, mas página final não confirmada"
                    progress_bar.value = 0.9
            else:
                status_text.value = f"Erro ao verificar conclusão: {response.status_code}"
                progress_bar.value = 0.8
                
        except Exception as e:
            status_text.value = f"❌ Erro durante a exportação: {str(e)}"
            progress_bar.value = 0
            
        finally:
            page.update()
    
    def main(self, page: ft.Page):
        """Interface principal do aplicativo"""
        page.title = "Auto Exportador TMS CSV"
        page.theme_mode = ft.ThemeMode.LIGHT
        
        # Configurações da janela
        page.window_width = 700
        page.window_height = 600
        page.window_resizable = False
        page.padding = 20
        page.scroll = ft.ScrollMode.AUTO
        
        # Elementos da interface
        title = ft.Text(
            "Auto Exportador de Dados TMS",
            size=28,
            weight=ft.FontWeight.BOLD,
            color=ft.colors.BLUE_800
        )
        
        subtitle = ft.Text(
            "Exportação automática de dados para CSV",
            size=16,
            color=ft.colors.GREY_600
        )
        
        # Informações de configuração
        config_card = ft.Card(
            content=ft.Container(
                content=ft.Column([
                    ft.Text("Configuração Automática:", weight=ft.FontWeight.BOLD, size=18),
                    ft.Divider(height=1),
                    ft.Row([
                        ft.Icon(ft.icons.CHECK_CIRCLE, color=ft.colors.GREEN),
                        ft.Text("Mês atual e anterior selecionados", size=14)
                    ]),
                    ft.Row([
                        ft.Icon(ft.icons.CHECK_CIRCLE, color=ft.colors.GREEN),
                        ft.Text("Inventário de Fio e Previsão marcado", size=14)
                    ]),
                    ft.Row([
                        ft.Icon(ft.icons.CHECK_CIRCLE, color=ft.colors.GREEN),
                        ft.Text("Exportação para C:\\TMSDATA", size=14)
                    ]),
                ]),
                padding=20,
            ),
            elevation=5
        )
        
        # Barra de progresso
        progress_bar = ft.ProgressBar(
            width=400,
            color=ft.colors.BLUE,
            bgcolor=ft.colors.GREY_300
        )
        
        # Status
        status_text = ft.Text(
            "Pronto para exportar",
            size=16,
            weight=ft.FontWeight.W_500
        )
        
        # Botão de exportação
        export_button = ft.ElevatedButton(
            content=ft.Row([
                ft.Icon(ft.icons.FILE_DOWNLOAD, color=ft.colors.WHITE),
                ft.Text("Iniciar Exportação Automática", size=16, color=ft.colors.WHITE)
            ]),
            bgcolor=ft.colors.BLUE,
            color=ft.colors.WHITE,
            width=300,
            height=50,
            on_click=lambda e: self.start_export(page, progress_bar, status_text, results_text)
        )
        
        # Área de resultados
        results_text = ft.TextField(
            label="Resultados da Exportação",
            multiline=True,
            read_only=True,
            min_lines=5,
            max_lines=10,
            width=500,
            border_color=ft.colors.BLUE_GREY_300,
            text_size=12
        )
        
        # Layout
        page.add(
            ft.Column(
                controls=[
                    ft.Container(
                        content=ft.Column([
                            ft.Row([title], alignment=ft.MainAxisAlignment.CENTER),
                            ft.Row([subtitle], alignment=ft.MainAxisAlignment.CENTER),
                            ft.Divider(height=10),
                            ft.Row([config_card], alignment=ft.MainAxisAlignment.CENTER),
                            ft.Divider(height=20),
                            ft.Row([export_button], alignment=ft.MainAxisAlignment.CENTER),
                            ft.Divider(height=20),
                            ft.Row([progress_bar], alignment=ft.MainAxisAlignment.CENTER),
                            ft.Row([status_text], alignment=ft.MainAxisAlignment.CENTER),
                            ft.Divider(height=20),
                            ft.Row([results_text], alignment=ft.MainAxisAlignment.CENTER),
                        ], 
                        horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                        spacing=15
                        ),
                        padding=10
                    )
                ],
                scroll=ft.ScrollMode.AUTO,
                expand=True
            )
        )
    
    def start_export(self, page, progress_bar, status_text, results_text):
        """Inicia o processo de exportação em uma thread separada"""
        progress_bar.value = 0
        status_text.value = "Iniciando..."
        results_text.value = ""
        page.update()
        
        # Executa em thread para não travar a interface
        thread = threading.Thread(
            target=self.export_data,
            args=(page, progress_bar, status_text, results_text),
            daemon=True
        )
        thread.start()

def main(page: ft.Page):
    app = CSVExporterApp()
    app.main(page)

if __name__ == "__main__":
    ft.app(target=main)