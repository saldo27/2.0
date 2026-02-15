"""
Analizador de Horarios - Procesamiento y análisis de archivos de horarios
Versión: 1.0 (Enero 2026)

Funcionalidades:
- Carga de archivos (PDF, Excel, CSV)
- Parseo de calendarios de guardias
- Cálculo de estadísticas por trabajador
- Generación de reportes en PDF
"""

import pandas as pd  # type: ignore
import datetime as dt
from datetime import datetime, timedelta, date
import re
import logging
from pathlib import Path
import io

# Suprimir debug output de pdfplumber
logging.getLogger('pdfplumber').setLevel(logging.WARNING)

try:
    import pdfplumber  # type: ignore
    PDFPLUMBER_AVAILABLE = True
except ImportError:
    PDFPLUMBER_AVAILABLE = False
    logging.warning("pdfplumber no disponible - funcionalidad PDF limitada")

try:
    import openpyxl  # type: ignore
    OPENPYXL_AVAILABLE = True
except ImportError:
    OPENPYXL_AVAILABLE = False
    logging.warning("openpyxl no disponible - funcionalidad Excel limitada")

from reportlab.lib.pagesizes import letter, A4  # type: ignore
from reportlab.lib import colors  # type: ignore
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle  # type: ignore
from reportlab.lib.units import inch  # type: ignore
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak, Image  # type: ignore
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT  # type: ignore
from utilities import DateTimeUtils

logger = logging.getLogger(__name__)


class CalendarFileProcessor:
    """Procesa archivos de calendario (PDF, Excel, CSV) y extrae información de horarios"""
    
    def __init__(self):
        self.text_content = ""
        self.calendar_data = {}
        
    def extract_text_from_pdf(self, file_content):
        """Extrae texto de archivo PDF"""
        if not PDFPLUMBER_AVAILABLE:
            raise RuntimeError("pdfplumber no está disponible. Instale con: pip install pdfplumber")
        
        try:
            text = ""
            with pdfplumber.open(io.BytesIO(file_content)) as pdf:
                for page in pdf.pages:
                    text += page.extract_text() + "\n"
            return text
        except Exception as e:
            logger.error(f"Error extrayendo PDF: {e}")
            raise
    
    def extract_text_from_excel(self, file_content):
        """Extrae texto de archivo Excel (.xlsx, .xls)"""
        if not OPENPYXL_AVAILABLE:
            raise RuntimeError("openpyxl no está disponible. Instale con: pip install openpyxl")
        
        try:
            excel_file = io.BytesIO(file_content)
            df = pd.read_excel(excel_file)
            
            # Convertir dataframe a texto
            text = df.to_string()
            return text
        except Exception as e:
            logger.error(f"Error extrayendo Excel: {e}")
            raise
    
    def extract_text_from_csv(self, file_content):
        """Extrae texto de archivo CSV"""
        try:
            # Decodificar bytes a string
            if isinstance(file_content, bytes):
                text = file_content.decode('utf-8', errors='ignore')
            else:
                text = file_content
            return text
        except Exception as e:
            logger.error(f"Error extrayendo CSV: {e}")
            raise
    
    def process_file(self, uploaded_file):
        """
        Procesa archivo cargado (detecta tipo y extrae texto)
        
        Args:
            uploaded_file: Archivo cargado por Streamlit
            
        Returns:
            str: Texto extraído del archivo
        """
        try:
            file_content = uploaded_file.read()
            filename = uploaded_file.name.lower()
            
            if filename.endswith('.pdf'):
                self.text_content = self.extract_text_from_pdf(file_content)
            elif filename.endswith(('.xlsx', '.xls')):
                self.text_content = self.extract_text_from_excel(file_content)
            elif filename.endswith('.csv'):
                self.text_content = self.extract_text_from_csv(file_content)
            else:
                raise ValueError(f"Formato de archivo no soportado: {filename}")
            
            return self.text_content
        except Exception as e:
            logger.error(f"Error procesando archivo: {e}")
            raise
    
    def detect_calendar_structure(self, text):
        """
        Detecta la estructura del calendario en el texto
        
        Formato esperado:
        22 23 24 25 26 27 28           (Números de días - 7 columnas)
        MANUEL MAR SANTI LOLA ELENA... (Trabajador 1)
        ELENA JOSE REQUE MARINA KIKO...  (Trabajador 2)
        """
        lines = text.strip().split('\n')
        lines = [line.strip() for line in lines if line.strip()]
        
        if len(lines) < 2:
            return None
        
        # Intentar encontrar la línea de días
        calendar_lines = []
        for i, line in enumerate(lines):
            # Línea de días contiene números separados por espacios
            if self._is_days_line(line):
                # Encontramos línea de días, el resto son trabajadores
                calendar_lines = lines[i+1:]
                break
        
        if not calendar_lines:
            calendar_lines = lines
        
        return {
            'raw_lines': calendar_lines,
            'text_content': text
        }
    
    @staticmethod
    def _is_days_line(line):
        """Verifica si una línea contiene números de días"""
        tokens = line.split()
        if len(tokens) < 5:
            return False
        
        # Contar cuántos tokens son números
        numeric_count = sum(1 for token in tokens if token.isdigit())
        return numeric_count >= len(tokens) * 0.7  # Al menos 70% son números


class ScheduleAnalyzer:
    """Analiza calendarios de guardias y calcula estadísticas"""
    
    def __init__(self, start_date, calendar_text, name_mapping=None, holidays=None, shifts_per_day=4):
        """
        Inicializa el analizador
        
        Args:
            start_date: Fecha inicial (datetime)
            calendar_text: Texto del calendario (trabajadores por línea)
            name_mapping: Dict de mapeo de nombres {corto: largo}
            holidays: Lista de fechas festivas (datetime)
            shifts_per_day: Número de guardias/trabajadores por día (filas de trabajadores)
        """
        self.start_date = start_date
        self.calendar_text = calendar_text
        self.name_mapping = name_mapping or {}
        self.holidays = holidays or []
        self.shifts_per_day = shifts_per_day
        self.workers_stats = {}
        self.calendar_dates = []
        self.calendar_array = []
        
        # Identificar períodos de puente
        date_utils = DateTimeUtils()
        year = start_date.year if hasattr(start_date, 'year') else datetime.now().year
        self.bridge_periods = date_utils.identify_bridge_periods(self.holidays, year)
        
        logging.info(f"ScheduleAnalyzer: Detectados {len(self.bridge_periods)} períodos de puente para año {year}")
        
        # Crear conjunto de fechas de puente para búsqueda rápida
        self.bridge_dates = set()
        for bridge_period in self.bridge_periods:
            current = bridge_period['start_date']
            end = bridge_period['end_date']
            while current <= end:
                # Convertir datetime a date si es necesario
                if isinstance(current, dt.datetime):
                    self.bridge_dates.add(current.date())
                else:
                    self.bridge_dates.add(current)
                current += timedelta(days=1)
        
        logging.info(f"ScheduleAnalyzer: Total de {len(self.bridge_dates)} fechas de puente: {sorted(self.bridge_dates)}")
        
    def parse_calendar(self):
        """
        Parsea el calendario de texto a estructura de datos
        
        Formato esperado (1 + N líneas por semana, donde N = shifts_per_day):
        22 23 24 25 26 27 28           (Números de días)
        MANUEL MAR SANTI LOLA ELENA... (Fila 1 de trabajadores)
        ELENA JOSE REQUE MARINA KIKO... (Fila 2)
        ... (tantas filas como shifts_per_day)
        
        El calendario va de Lunes a Domingo (7 columnas).
        Cuando el día actual es menor que el anterior, cambia de mes.
        
        Retorna:
            dict: {worker_name: [fecha1, fecha2, ...]}
        """
        lines = self.calendar_text.strip().split('\n')
        lines = [line.strip() for line in lines if line.strip()]
        
        if not lines:
            return {}
        
        workers_schedule = {}
        calendar_data = []  # Lista de {day, month, year, workers}
        
        current_date = date(self.start_date.year, self.start_date.month, 1) if hasattr(self.start_date, 'year') else self.start_date
        previous_day = 0
        
        # Número de filas por bloque: 1 (días) + shifts_per_day (trabajadores)
        lines_per_block = 1 + self.shifts_per_day
        
        # Procesar en bloques de (1 + shifts_per_day) líneas
        i = 0
        while i < len(lines):
            # Línea de días
            days_line = lines[i].strip().split()
            
            # Verificar si es línea de números de días
            numeric_count = sum(1 for token in days_line if token.isdigit())
            if numeric_count < len(days_line) * 0.5:
                # No es línea de días, saltar
                i += 1
                continue
            
            day_count = len(days_line)
            
            # Obtener las N filas de trabajadores (según shifts_per_day)
            worker_rows = []
            for row_num in range(self.shifts_per_day):
                line_idx = i + 1 + row_num
                if line_idx < len(lines):
                    row = self._parse_worker_names(lines[line_idx].split(), day_count)
                else:
                    row = [None] * day_count
                worker_rows.append(row)
            
            # Procesar cada día
            for j, day_str in enumerate(days_line):
                try:
                    day = int(day_str)
                except ValueError:
                    continue
                
                # Detectar cambio de mes: si el día actual es menor que el anterior
                if previous_day > 0 and day < previous_day:
                    # Cambiar al siguiente mes
                    if current_date.month == 12:
                        current_date = date(current_date.year + 1, 1, 1)
                    else:
                        current_date = date(current_date.year, current_date.month + 1, 1)
                
                # Crear la fecha real
                try:
                    actual_date = date(current_date.year, current_date.month, day)
                except ValueError:
                    # Día inválido para el mes, saltar
                    continue
                
                # Recopilar trabajadores de todas las filas CON SU POSICIÓN
                workers_today = []
                workers_with_position = []  # Lista de (worker, position)
                for pos, row in enumerate(worker_rows):
                    if j < len(row) and row[j]:
                        worker_name = row[j]
                        # Aplicar mapeo de nombres
                        worker_name = self.name_mapping.get(worker_name, worker_name)
                        worker_name = self.name_mapping.get(worker_name.upper(), worker_name)
                        workers_today.append(worker_name)
                        workers_with_position.append((worker_name, pos))
                
                # Agregar a calendar_data (incluyendo posiciones para calcular Rosell)
                calendar_data.append({
                    'date': actual_date,
                    'day': day,
                    'month': actual_date.month,
                    'year': actual_date.year,
                    'day_of_week': j % 7,  # 0=Lunes, 6=Domingo
                    'workers': workers_today,
                    'workers_with_position': workers_with_position,  # Para cálculo de Rosell
                    'last_position': len(workers_today) - 1 if workers_today else -1  # Índice de última posición
                })
                
                # Agregar fechas a cada trabajador
                for worker in workers_today:
                    if worker not in workers_schedule:
                        workers_schedule[worker] = []
                    workers_schedule[worker].append(actual_date)
                
                previous_day = day
            
            # Avanzar al siguiente bloque (1 línea de días + N filas de trabajadores)
            i += lines_per_block
        
        self.workers_schedule = workers_schedule
        self.calendar_data = calendar_data
        return workers_schedule
    
    @staticmethod
    def _is_likely_initial(word):
        """
        Detecta si una palabra es probablemente una inicial o segunda parte de nombre compuesto.
        
        Una inicial es:
        - Una sola letra: "H", "R", "M"
        - Dos letras: "HZ", "RM"
        - Letra con punto: "H.", "R."
        
        Retorna: bool
        """
        word = word.strip()
        if not word:
            return False
        
        # Eliminar puntos
        word_clean = word.replace('.', '')
        
        # Una o dos letras (sin números)
        if len(word_clean) <= 2 and word_clean.isalpha():
            return True
        
        return False
    
    @staticmethod
    def _parse_worker_names(names, day_count=7):
        """
        Parsea nombres de trabajadores, detectando y combinando nombres compuestos.
        
        Por ejemplo:
        ["LUIS", "H", "LUIS", "R", "CARLOS"] → ["LUIS H", "LUIS R", "CARLOS"]
        
        Usa heurística: si una palabra es una inicial (1-2 letras), se combina con la anterior.
        
        Args:
            names: Lista de palabras/nombres
            day_count: Número de días esperados (columnas)
        
        Retorna: Lista de exactamente day_count nombres (completa con None si es necesario)
        """
        if not names:
            return [None] * day_count
        
        # Si tenemos exactamente la cantidad correcta y no hay iniciales, retornar directo
        if len(names) == day_count:
            has_initials = any(ScheduleAnalyzer._is_likely_initial(n) for n in names[1:])
            if not has_initials:
                return [n if n and not n.isdigit() else None for n in names]
        
        processed = []
        current_name = ""
        
        for i, name in enumerate(names):
            name = name.strip()
            
            if not name or name.isdigit():
                # Si encontramos un vacío pero tenemos nombre actual, guardarlo
                if current_name:
                    processed.append(current_name)
                    current_name = ""
                continue
            
            # Si es el primer nombre o el anterior no fue una inicial
            if not current_name:
                current_name = name
            # Si la palabra actual es una inicial, combinarla con la anterior
            elif ScheduleAnalyzer._is_likely_initial(name):
                current_name += " " + name
            # Si no es una inicial, guardar el nombre anterior y empezar uno nuevo
            else:
                processed.append(current_name)
                current_name = name
        
        # Agregar el último nombre
        if current_name:
            processed.append(current_name)
        
        # Completar con None hasta tener exactamente day_count elementos
        while len(processed) < day_count:
            processed.append(None)
        
        # Si hay más de day_count, solo tomar los primeros
        return processed[:day_count]
    
    def calculate_statistics(self):
        """
        Calcula estadísticas por trabajador usando calendar_data
        
        REGLAS DE CONTABILIZACIÓN:
        - Festivo cuenta como DOMINGO
        - PreFestivo (día anterior a festivo) cuenta como VIERNES (solo Lun-Jue)
        - Viernes/Sábado/Domingo Festivo cuenta como DOMINGO (festivo tiene prioridad)
        - Rosell = guardias en última posición
        
        Retorna:
            DataFrame con columnas:
            - Trabajador, Total, Viernes, Sábado, Domingo, Total FS, % FS
            - Rosell, % Rosell
            - Desglose por mes
        """
        # Convertir festivos a date (pueden venir como datetime)
        holidays_set = set()
        if self.holidays:
            for h in self.holidays:
                if hasattr(h, 'date'):
                    holidays_set.add(h.date())  # datetime -> date
                else:
                    holidays_set.add(h)  # ya es date
        
        # Diccionario para meses en español
        meses_es = {
            1: 'Ene', 2: 'Feb', 3: 'Mar', 4: 'Abr',
            5: 'May', 6: 'Jun', 7: 'Jul', 8: 'Ago',
            9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dic'
        }
        
        # Inicializar estadísticas por trabajador
        worker_stats = {}
        
        # Procesar cada día del calendario
        for day_data in self.calendar_data:
            actual_date = day_data['date']
            workers = day_data.get('workers', [])
            workers_with_pos = day_data.get('workers_with_position', [])
            last_pos = day_data.get('last_position', len(workers) - 1 if workers else -1)
            
            # Día de la semana real (0=Lunes, 6=Domingo)
            day_of_week = actual_date.weekday()
            
            # Determinar si es festivo o prefestivo
            is_holiday = actual_date in holidays_set
            next_day = actual_date + timedelta(days=1)
            is_pre_holiday = next_day in holidays_set
            
            # Mes en español
            month_key = meses_es.get(actual_date.month, f'Mes{actual_date.month}')
            
            # Procesar cada trabajador de este día
            for idx, worker in enumerate(workers):
                if not worker:
                    continue
                
                # Inicializar si no existe
                if worker not in worker_stats:
                    worker_stats[worker] = {
                        'total': 0,
                        'viernes': 0,
                        'sabado': 0,
                        'domingo': 0,
                        'weekend': 0,
                        'rosell': 0,
                        'bridge': 0,
                        'monthly': {}
                    }
                
                stats = worker_stats[worker]
                stats['total'] += 1
                
                # Contar mes
                if month_key not in stats['monthly']:
                    stats['monthly'][month_key] = 0
                stats['monthly'][month_key] += 1
                
                # Contar Rosell (última posición)
                if idx == last_pos:
                    stats['rosell'] += 1
                
                # Contar puentes (días en períodos de puente)
                # actual_date ya es un objeto date del parse_calendar
                if actual_date in self.bridge_dates:
                    stats['bridge'] += 1
                    logging.debug(f"Turno de puente detectado para {worker} en {actual_date}")
                
                # LÓGICA DE CONTABILIZACIÓN DE FIN DE SEMANA:
                # 1. Si es FESTIVO → cuenta como DOMINGO (sin importar qué día sea)
                # 2. Si es PREFESTIVO y es Lun-Jue → cuenta como VIERNES
                # 3. Si no es festivo ni prefestivo → cuenta según día de semana
                
                if is_holiday:
                    # Festivo = Domingo
                    stats['domingo'] += 1
                    stats['weekend'] += 1
                elif is_pre_holiday and day_of_week in [0, 1, 2, 3]:
                    # PreFestivo (solo Lun-Jue) = Viernes
                    stats['viernes'] += 1
                    stats['weekend'] += 1
                elif day_of_week == 4:  # Viernes
                    stats['viernes'] += 1
                    stats['weekend'] += 1
                elif day_of_week == 5:  # Sábado
                    stats['sabado'] += 1
                    stats['weekend'] += 1
                elif day_of_week == 6:  # Domingo
                    stats['domingo'] += 1
                    stats['weekend'] += 1
        
        # Construir lista de estadísticas
        stats_list = []
        for worker, stats in worker_stats.items():
            total = stats['total']
            weekend_pct = (stats['weekend'] / total * 100) if total > 0 else 0
            rosell_pct = (stats['rosell'] / total * 100) if total > 0 else 0
            bridge_pct = (stats['bridge'] / total * 100) if total > 0 else 0
            
            stats_list.append({
                'Trabajador': worker,
                'Total': total,
                'Viernes': stats['viernes'],
                'Sábado': stats['sabado'],
                'Domingo': stats['domingo'],
                'Total FS': stats['weekend'],
                '% FS': round(weekend_pct, 1),
                'Puente': stats['bridge'],
                '% Puente': round(bridge_pct, 1),
                'Rosell': stats['rosell'],
                '% Rosell': round(rosell_pct, 1),
                'monthly_stats': stats['monthly'],
                'consecutive_dates': []  # Se calcula en get_alerts()
            })
        
        self.workers_stats = stats_list
        
        # Crear DataFrame
        df_data = []
        all_months = set()
        for stat in stats_list:
            all_months.update(stat['monthly_stats'].keys())
        
        # Ordenar meses correctamente
        meses_orden = ['Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun', 'Jul', 'Ago', 'Sep', 'Oct', 'Nov', 'Dic']
        sorted_months = sorted(all_months, key=lambda m: meses_orden.index(m) if m in meses_orden else 99)
        
        for stat in stats_list:
            row = {
                'Trabajador': stat['Trabajador'],
                'Total': stat['Total'],
                'Viernes': stat['Viernes'],
                'Sábado': stat['Sábado'],
                'Domingo': stat['Domingo'],
                'Total FS': stat['Total FS'],
                '% FS': stat['% FS'],
                'Puente': stat['Puente'],
                '% Puente': stat['% Puente'],
                'Rosell': stat['Rosell'],
                '% Rosell': stat['% Rosell']
            }
            # Agregar columnas de meses en orden
            for month in sorted_months:
                row[f'Mes: {month}'] = stat['monthly_stats'].get(month, 0)
            
            df_data.append(row)
        
        # Log resumen de puentes
        total_bridge_shifts = sum(stat['Puente'] for stat in stats_list)
        logging.info(f"Total de turnos en puente detectados: {total_bridge_shifts}")
        for stat in stats_list:
            if stat['Puente'] > 0:
                logging.info(f"  {stat['Trabajador']}: {stat['Puente']} turnos en puente ({stat['% Puente']}%)")
        
        return pd.DataFrame(df_data)
    
    def get_alerts(self):
        """
        Retorna alertas de guardias consecutivas.
        
        Detecta cuando un trabajador tiene guardia en días consecutivos
        usando calendar_data (días del calendario en orden).
        
        Retorna:
            DataFrame con alertas [(trabajador, fecha1, fecha2), ...]
        """
        alerts = []
        
        # Usar calendar_data si existe (método preferido)
        if hasattr(self, 'calendar_data') and self.calendar_data and len(self.calendar_data) >= 2:
            # Iterar sobre días consecutivos
            for i in range(len(self.calendar_data) - 1):
                current_day = self.calendar_data[i]
                next_day = self.calendar_data[i + 1]
                
                # Verificar si son días consecutivos (diferencia de 1 día)
                current_date = current_day['date']
                next_date = next_day['date']
                
                if (next_date - current_date).days == 1:
                    # Encontrar trabajadores que están en ambos días
                    current_workers = set(current_day.get('workers', []))
                    next_workers = set(next_day.get('workers', []))
                    
                    consecutive_workers = current_workers & next_workers
                    
                    for worker in consecutive_workers:
                        alerts.append({
                            'Trabajador': worker,
                            'Fecha 1': current_date.strftime('%d-%m-%Y'),
                            'Fecha 2': next_date.strftime('%d-%m-%Y'),
                            'Alerta': '⚠️ Guardias consecutivas'
                        })
        else:
            # Fallback: usar workers_stats si no hay calendar_data
            for stat in self.workers_stats:
                if stat.get('consecutive_dates'):
                    for date1, date2 in stat['consecutive_dates']:
                        alerts.append({
                            'Trabajador': stat['Trabajador'],
                            'Fecha 1': date1.strftime('%d-%m-%Y'),
                            'Fecha 2': date2.strftime('%d-%m-%Y'),
                            'Alerta': '⚠️ Guardias consecutivas'
                        })
        
        if alerts:
            return pd.DataFrame(alerts)
        return pd.DataFrame()


class PDFReportGenerator:
    """Genera reportes en PDF con estadísticas y gráficos"""
    
    def __init__(self, filename="reporte_guardias.pdf"):
        self.filename = filename
        self.elements = []
        
    def generate_report(self, analyzer, df_stats, include_charts=False):
        """
        Genera reporte PDF completo
        
        Args:
            analyzer: Instancia de ScheduleAnalyzer
            df_stats: DataFrame con estadísticas
            include_charts: Si incluir gráficos (requiere img_buffer)
            
        Retorna:
            bytes: Contenido del PDF
        """
        buffer = io.BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=A4)
        elements = []
        
        styles = getSampleStyleSheet()
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=16,
            textColor=colors.HexColor('#1f4e78'),
            spaceAfter=20,
            alignment=TA_CENTER
        )
        
        heading_style = ParagraphStyle(
            'CustomHeading',
            parent=styles['Heading2'],
            fontSize=12,
            textColor=colors.HexColor('#4472c4'),
            spaceAfter=10,
            spaceBefore=10
        )
        
        # Título
        title = Paragraph("📊 Reporte de Análisis de Guardias", title_style)
        elements.append(title)
        elements.append(Spacer(1, 0.2*inch))
        
        # Información general
        info_text = f"""
        <b>Período:</b> {analyzer.start_date.strftime('%d-%m-%Y')} <br/>
        <b>Total de Trabajadores:</b> {len(df_stats)} <br/>
        <b>Total de Guardias:</b> {df_stats['Total'].sum()} <br/>
        <b>Guardias en Fin de Semana:</b> {df_stats['Total FS'].sum()}
        """
        elements.append(Paragraph(info_text, styles['Normal']))
        elements.append(Spacer(1, 0.3*inch))
        
        # Tabla de estadísticas
        heading = Paragraph("Estadísticas por Trabajador", heading_style)
        elements.append(heading)
        
        # Preparar datos para tabla
        table_data = [['Trabajador', 'Total', 'Vie', 'Sab', 'Dom', 'Tot FS', '% FS', 'Rosell', '% Ros']]
        for _, row in df_stats.iterrows():
            table_data.append([
                row['Trabajador'][:15],  # Truncar nombre
                str(row['Total']),
                str(row['Viernes']),
                str(row['Sábado']),
                str(row['Domingo']),
                str(row['Total FS']),
                f"{row['% FS']}%",
                str(row.get('Rosell', 0)),
                f"{row.get('% Rosell', 0)}%"
            ])
        
        # Crear tabla
        table = Table(table_data, colWidths=[1.3*inch, 0.5*inch, 0.45*inch, 0.45*inch, 0.45*inch, 0.55*inch, 0.55*inch, 0.5*inch, 0.55*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#4472c4')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('FONTSIZE', (0, 1), (-1, -1), 9),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f0f0f0')])
        ]))
        
        elements.append(table)
        
        # Construir PDF
        doc.build(elements)
        buffer.seek(0)
        return buffer.getvalue()
