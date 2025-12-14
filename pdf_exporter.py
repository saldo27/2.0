from kivy.app import App
from kivy.uix.popup import Popup
from kivy.uix.label import Label
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape, letter # Keep A4 if needed elsewhere
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch, cm # Use cm for better control maybe
from calendar import monthcalendar
from datetime import datetime
from utilities import numeric_sort_key
from performance_cache import cached, time_function, monitor_performance
from collections import defaultdict
import logging

def numeric_sort_key(item):
    """
    Attempts to convert the first element of a tuple (the key) to an integer
    for sorting. Returns a tuple to prioritize numeric keys and handle errors.
    item[0] is assumed to be the worker ID (key).
    """
    try:
        # Try converting the key (worker ID) to an integer
        return (0, int(item[0])) # (0, numeric_value) - sorts numbers first
    except (ValueError, TypeError):
        # If conversion fails, return a tuple indicating it's non-numeric
        return (1, item[0]) # (1, original_string) - sorts non-numbers after numbers

class PDFExporter:
    def __init__(self, schedule_config):
        self.schedule = schedule_config.get('schedule', {})
        self.workers_data = schedule_config.get('workers_data', [])
        self.num_shifts = schedule_config.get('num_shifts', 0)
        self.holidays = schedule_config.get('holidays', [])
        self.styles = getSampleStyleSheet()
        self.styles.add(ParagraphStyle(name='SmallNormal', parent=self.styles['Normal'], fontSize=9))
        self.styles.add(ParagraphStyle(name='SmallBold', parent=self.styles['SmallNormal'], fontName='Helvetica-Bold'))
        
        # Performance optimization: Pre-compute frequently used data
        self.holidays_set = set(self.holidays)  # O(1) lookup
        self.workers_dict = {worker.get('id'): worker for worker in self.workers_data}  # O(1) lookup

    @time_function
    @monitor_performance("export_summary_pdf")
    def export_summary_pdf(self, stats_data): # Takes the whole stats dictionary now
        """Export a detailed GLOBAL summary with shift listings and distributions (optimized)."""

        # --- Determine Filename and Title from stats_data ---
        start = stats_data.get('period_start')
        end = stats_data.get('period_end')
        if start and end:
            period_str_file = f"{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}"
            period_str_title = f"{start.strftime('%d-%m-%Y')} to {end.strftime('%d-%m-%Y')}"
            filename = f"summary_global_{period_str_file}.pdf"
            title_text = f"Schedule Summary ({period_str_title})"
        else:
            filename = "summary_global_full_period.pdf"
            title_text = "Schedule Summary (Full Period)"
        # --- End Filename/Title ---

        try:
            doc = SimpleDocTemplate(
                filename, pagesize=A4, # Portrait A4
                rightMargin=1.5*cm, leftMargin=1.5*cm, topMargin=1.5*cm, bottomMargin=1.5*cm
            )
            styles = self.styles
            story = []
            weekdays_short = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']

            # --- PDF Title (using new title_text) ---
            title = Paragraph(title_text, styles['h1'])
            story.append(title)
            story.append(Spacer(1, 0.5*cm))

            # --- Worker Details ---
            worker_title = Paragraph("Worker Details & Distributions", styles['h2'])
            story.append(worker_title)
            story.append(Spacer(1, 0.3*cm))
            story.append(Paragraph("<u>Assigned Shifts:</u>", styles['SmallBold']))

            # Use the workers data directly from stats_data
            workers_stats = stats_data.get('workers', {})
            worker_shifts_all = stats_data.get('worker_shifts', {})

            if not workers_stats:
                 story.append(Paragraph("No worker statistics found for this period.", styles['Normal']))
            else:
                # Process workers efficiently using batch operations
                worker_items = list(workers_stats.items())
                
                # Numeric sort: 1,2,3,…10,11 (using cached sort key)
                for worker_id, stats in sorted(worker_items, key=numeric_sort_key):
                    # --- Worker Header ---
                    worker_header = Paragraph(f"Worker {worker_id}", styles['h3'])
                    story.append(worker_header)
                    story.append(Spacer(1, 0.2*cm))

                    # Get stats efficiently
                    total_w = stats.get('total', 0)
                    holidays_w = stats.get('holidays', 0)
                    last_post_w = stats.get('last_post', 0)
                    weekday_counts = stats.get('weekday_counts', {})
                    post_counts = stats.get('post_counts', {})
                    worker_shifts = worker_shifts_all.get(worker_id, []) 

                    # --- Worker Summary Stats ---
                    summary_text = f"<b>Total Shifts:</b> {total_w} | <b>Weekend Shifts:</b> {stats.get('weekends', 0)} | <b>Holiday Shifts:</b> {holidays_w} | <b>Last Post Shifts:</b> {last_post_w}"
                    story.append(Paragraph(summary_text, styles['Normal']))
                    story.append(Spacer(1, 0.1*cm))

                    # --- Weekday Distribution (optimized string building) ---
                    weekdays_str = "<b>Weekdays:</b> " + " ".join(
                        f"{weekdays_short[i]}:{weekday_counts.get(i, 0)}" 
                        for i in range(7)
                    )
                    story.append(Paragraph(weekdays_str, styles['Normal']))
                    story.append(Spacer(1, 0.1*cm))

                    # --- Post Distribution (optimized) ---
                    posts_str = "<b>Posts:</b> " + " ".join(
                        f"P{post+1}:{count}" 
                        for post, count in sorted(post_counts.items())
                    )
                    story.append(Paragraph(posts_str, styles['Normal']))
                    story.append(Spacer(1, 0.3*cm))

                    # --- Assigned Shifts Table ---
                    if worker_shifts:
                        story.append(Paragraph("<u>Assigned Shifts:</u>", styles['SmallBold']))
                        story.append(Spacer(1, 0.1*cm))

                        # Build table data efficiently
                        shifts_data = [['Date', 'Day', 'Post', 'Type']]
                        
                        # Sort shifts once and process efficiently
                        sorted_shifts = sorted(worker_shifts, key=lambda x: x['date'])
                        for shift in sorted_shifts:
                            date_str = shift['date'].strftime('%d-%m-%Y')
                            day_str = shift['day'][:3]
                            post_str = f"P{shift['post']}"
                            day_type = ""
                            if shift['is_holiday']: 
                                day_type = "HOL"
                            elif shift['is_weekend']: 
                                day_type = "W/E"
                            shifts_data.append([date_str, day_str, post_str, day_type])

                        # Create table with optimized settings
                        shifts_table = Table(
                            shifts_data, 
                            colWidths=[2.5*cm, 1.5*cm, 1.5*cm, 1.5*cm], 
                            repeatRows=1
                        )
                        shifts_table.setStyle(TableStyle([
                            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                            ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
                            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                            ('FONTSIZE', (0, 0), (-1, -1), 9),
                            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                            ('ALIGN', (0, 1), (0, -1), 'LEFT'),
                        ]))
                        story.append(shifts_table)
                    else:
                        story.append(Paragraph("No shifts assigned in this period.", styles['SmallNormal']))

                    story.append(Spacer(1, 0.5*cm)) # Space between workers

            # --- Build PDF ---
            doc.build(story)
            logging.info(f"Successfully created GLOBAL summary PDF: {filename}")
            return filename # Return filename on success

        except Exception as e:
            logging.error(f"Failed to export GLOBAL summary PDF: {str(e)}", exc_info=True)
            raise e # Re-raise for main.py to catch
        
    def export_monthly_calendar(self, year, month, filename=None):
        """Export monthly calendar view to PDF"""
        if not filename:
            filename = f'schedule_{year}_{month:02d}.pdf'
            
        doc = SimpleDocTemplate(
            filename,
            pagesize=landscape(A4),
            rightMargin=30,
            leftMargin=30,
            topMargin=30,
            bottomMargin=30
        )
        
        # Prepare story (content)
        story = []
        
        # Add title
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=self.styles['Heading1'],
            fontSize=16,
            spaceAfter=30
        )
        title = Paragraph(
            f"Schedule for {datetime(year, month, 1).strftime('%B %Y')}",
            title_style
        )
        story.append(title)
        
        # Create calendar data
        cal = monthcalendar(year, month)
        calendar_data = [['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']]
        
        for week in cal:
            week_data = []
            for day in week:
                if day == 0:
                    cell_content = ''
                else:
                    date = datetime(year, month, day)
                    cell_content = [str(day)]
                    
                    # Add scheduled workers
                    if date in self.schedule:
                        for i, worker_id in enumerate(self.schedule[date]):
                            if worker_id is not None:  # Check for None values
                                cell_content.append(f'S{i+1}: {worker_id}')
                    
                    # Mark holidays
                    if date in self.holidays:
                        cell_content.append('HOLIDAY')
                    
                    cell_content = '\n'.join(cell_content)
                week_data.append(cell_content)
            calendar_data.append(week_data)
        
        # Create table
        table = Table(calendar_data, colWidths=[1.5*inch]*7, rowHeights=[0.5*inch] + [1.2*inch]*len(cal))
        
        # Style the table
        style = TableStyle([
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (5, 1), (6, -1), colors.lightgrey),  # Weekend columns
        ])
        table.setStyle(style)
        
        story.append(table)
        doc.build(story)
        return filename

    def export_all_months_calendar(self, filename=None):
        """
        Export ALL months in the schedule period to PDF in landscape A4 format.
        Each month gets its own page.
        """
        # Determine the date range from the schedule
        if not self.schedule:
            logging.warning("No schedule data available for PDF export")
            return None
            
        schedule_dates = sorted(self.schedule.keys())
        start_date = schedule_dates[0]
        end_date = schedule_dates[-1]
        
        # Generate filename with period
        if not filename:
            period_str = f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
            filename = f'schedule_all_months_{period_str}.pdf'
        
        try:
            # Create PDF document in LANDSCAPE A4 format
            doc = SimpleDocTemplate(
                filename,
                pagesize=landscape(A4),
                rightMargin=30,
                leftMargin=30,
                topMargin=30,
                bottomMargin=30
            )
            
            story = []
            
            # Generate a page for each month in the period
            current_date = datetime(start_date.year, start_date.month, 1)
            end_month = datetime(end_date.year, end_date.month, 1)
            
            first_month = True
            while current_date <= end_month:
                # Add page break between months (except for first month)
                if not first_month:
                    from reportlab.platypus import PageBreak
                    story.append(PageBreak())
                first_month = False
                
                # Add title for this month
                title_style = ParagraphStyle(
                    'MonthTitle',
                    parent=self.styles['Heading1'],
                    fontSize=16,
                    spaceAfter=20,
                    alignment=1  # Center alignment
                )
                month_title = Paragraph(
                    f"Schedule for {current_date.strftime('%B %Y')}",
                    title_style
                )
                story.append(month_title)
                story.append(Spacer(1, 0.3*cm))
                
                # Create calendar data for this month
                cal = monthcalendar(current_date.year, current_date.month)
                calendar_data = [['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']]
                
                for week in cal:
                    week_data = []
                    for day in week:
                        if day == 0:
                            cell_content = ''
                        else:
                            date = datetime(current_date.year, current_date.month, day)
                            cell_lines = [f'<b>{day}</b>']
                            
                            # Add scheduled workers
                            if date in self.schedule:
                                for i, worker_id in enumerate(self.schedule[date]):
                                    if worker_id is not None:
                                        cell_lines.append(f'Post {i+1}: {worker_id}')
                            
                            # Mark holidays
                            if date in self.holidays_set:
                                cell_lines.append('<i><font color="red">HOLIDAY</font></i>')
                            
                            # Join with line breaks
                            cell_content = Paragraph('<br/>'.join(cell_lines), self.styles['SmallNormal'])
                        
                        week_data.append(cell_content)
                    calendar_data.append(week_data)
                
                # Create table with appropriate column widths for landscape A4
                # Landscape A4 width ≈ 27.7 cm, minus margins (2x3cm) = 21.7 cm
                col_width = (landscape(A4)[0] - 60) / 7  # Divide available width by 7 days
                
                table = Table(
                    calendar_data, 
                    colWidths=[col_width] * 7,
                    rowHeights=[0.5*cm] + [2.5*cm] * len(cal)  # Header + weeks
                )
                
                # Style the table
                style = TableStyle([
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                    ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                    ('GRID', (0, 0), (-1, -1), 1, colors.black),
                    ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 10),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
                    ('TOPPADDING', (0, 1), (-1, -1), 5),
                    ('LEFTPADDING', (0, 0), (-1, -1), 5),
                    ('RIGHTPADDING', (0, 0), (-1, -1), 5),
                    # Highlight weekends (Saturday and Sunday are columns 5 and 6)
                    ('BACKGROUND', (5, 1), (6, -1), colors.Color(0.95, 0.95, 0.95)),
                ])
                table.setStyle(style)
                
                story.append(table)
                
                # Move to next month
                if current_date.month == 12:
                    current_date = datetime(current_date.year + 1, 1, 1)
                else:
                    current_date = datetime(current_date.year, current_date.month + 1, 1)
            
            # Build the PDF
            doc.build(story)
            logging.info(f"Successfully created multi-month calendar PDF: {filename}")
            return filename
            
        except Exception as e:
            logging.error(f"Failed to export multi-month calendar PDF: {str(e)}", exc_info=True)
            raise

    @time_function
    @monitor_performance("export_worker_statistics")
    def export_worker_statistics(self, filename=None):
        """Export worker statistics to PDF (optimized for performance)"""
        if not filename:
            filename = 'worker_statistics.pdf'
            
        doc = SimpleDocTemplate(
            filename,
            pagesize=A4,
            rightMargin=30,
            leftMargin=30,
            topMargin=30,
            bottomMargin=30
        )
        
        story = []
        
        # Add title
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=self.styles['Heading1'],
            fontSize=16,
            spaceAfter=30
        )
        title = Paragraph("Worker Statistics Report", title_style)
        story.append(title)
        
        # Pre-compute data for all workers to avoid repeated calculations
        worker_stats_cache = {}
        
        # Prepare statistics for each worker efficiently
        for worker in self.workers_data:
            worker_id = worker['id']
            
            # Get worker's assignments using efficient set operations
            assignments = {
                date for date, workers in self.schedule.items()
                if worker_id in workers
            }
            
            # Calculate statistics in a single pass
            total_shifts = len(assignments)
            weekend_shifts = sum(1 for date in assignments if date.weekday() >= 4)
            holiday_shifts = sum(1 for date in assignments if date in self.holidays_set)
            
            # Calculate post distribution efficiently using defaultdict
            post_distribution = defaultdict(int)
            for date in assignments:
                shifts_for_date = self.schedule.get(date, [])
                try:
                    if worker_id in shifts_for_date:
                        post = shifts_for_date.index(worker_id)
                        post_distribution[post] += 1
                except ValueError:
                    # Skip if worker_id isn't in the list
                    pass
            
            # Calculate weekday distribution efficiently
            weekday_distribution = defaultdict(int)
            for date in assignments:
                weekday_distribution[date.weekday()] += 1
            
            # Store computed stats
            worker_stats_cache[worker_id] = {
                'worker': worker,
                'total_shifts': total_shifts,
                'weekend_shifts': weekend_shifts,
                'holiday_shifts': holiday_shifts,
                'post_distribution': dict(post_distribution),
                'weekday_distribution': {i: weekday_distribution[i] for i in range(7)}
            }
        
        # Generate PDF content efficiently
        for worker_id, stats in worker_stats_cache.items():
            worker = stats['worker']
            
            # Create worker section
            worker_title = Paragraph(
                f"Worker {worker_id}",
                self.styles['Heading2']
            )
            story.append(worker_title)
            
            # Build details efficiently using list comprehension
            details_parts = [
                f"Work Percentage: {worker.get('work_percentage', 100)}%",
                f"Total Shifts: {stats['total_shifts']}",
                f"Weekend Shifts: {stats['weekend_shifts']}",
                f"Holiday Shifts: {stats['holiday_shifts']}",
                "\nPost Distribution:",
                *[f"  Post {post+1}: {count}" for post, count in stats['post_distribution'].items()],
                "\nWeekday Distribution:",
                "  Mon Tue Wed Thu Fri Sat Sun",
                "  " + " ".join(f"{stats['weekday_distribution'][i]:3d}" for i in range(7))
            ]
            
            details_text = Paragraph(
                '<br/>'.join(details_parts),
                self.styles['Normal']
            )
            story.append(details_text)
            story.append(Spacer(1, 20))
        
        doc.build(story)
        return filename
