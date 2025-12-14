try:
    from fpdf import FPDF
except ImportError:
    try:
        from fpdf2 import FPDF
    except ImportError:
        FPDF = None
        
import csv
from datetime import datetime
import calendar
from collections import defaultdict
from performance_cache import cached, time_function, monitor_performance

class StatsExporter:
    def __init__(self, scheduler):
        self.scheduler = scheduler
        
        # Cache frequently accessed data for performance
        self.workers_data = scheduler.workers_data
        self.worker_assignments = scheduler.worker_assignments
        self.worker_weekends = scheduler.worker_weekends

    @time_function
    @monitor_performance("gather_worker_statistics")
    def gather_worker_statistics(self):
        """Gather comprehensive statistics for all workers (optimized for performance)"""
        stats = {}
    
        # Process all workers in a single pass for better cache locality
        for worker in self.workers_data:
            worker_id = worker['id']
            assignments = self.worker_assignments.get(worker_id, set())
            weekend_assignments = self.worker_weekends.get(worker_id, set())
        
            # Calculate monthly distribution efficiently using defaultdict
            monthly_distribution = defaultdict(int)
            weekday_distribution = defaultdict(int)
            
            # Process assignments in a single pass
            for date in assignments:
                month_key = f"{date.year}-{date.month:02d}"
                monthly_distribution[month_key] += 1
                weekday_distribution[date.weekday()] += 1
            
            # Convert to regular dicts and fill missing weekdays
            monthly_dist_dict = dict(monthly_distribution)
            weekday_dist_dict = {i: weekday_distribution[i] for i in range(7)}
            
            # Calculate gaps efficiently
            if len(assignments) > 1:
                sorted_dates = sorted(assignments)
                gaps = [(sorted_dates[i+1] - sorted_dates[i]).days 
                       for i in range(len(sorted_dates)-1)]
                average_gap = sum(gaps) / len(gaps)
                min_gap = min(gaps)
                max_gap = max(gaps)
            else:
                average_gap = min_gap = max_gap = 0
            
            # Store all stats for this worker
            stats[worker_id] = {
                'worker_id': worker_id,
                'work_percentage': worker.get('work_percentage', 100),
                'total_shifts': len(assignments),
                'target_shifts': worker.get('target_shifts', 0),
                'weekend_shifts': len(weekend_assignments),
                'weekday_shifts': len(assignments) - len(weekend_assignments),
                'monthly_distribution': monthly_dist_dict,
                'weekday_distribution': weekday_dist_dict,
                'average_gap': average_gap,
                'min_gap': min_gap if min_gap != float('inf') else 0,
                'max_gap': max_gap
            }
        
        return stats

    def export_worker_stats(self, format='txt'):
        """Export worker statistics to file
        Args:
            format (str): 'txt' or 'pdf'
        Returns:
            str: Path to the generated file
        """
        stats = self.gather_worker_statistics()
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
        if format.lower() == 'txt':
            filename = f'worker_stats_{timestamp}.txt'
            with open(filename, 'w', encoding='utf-8') as f:
                f.write("=== Worker Statistics Report ===\n")
                f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
                for worker_id, worker_stats in stats.items():
                    f.write(f"\nWorker {worker_id}\n")
                    f.write("="* 20 + "\n")
                    f.write(f"Work Percentage: {worker_stats['work_percentage']}%\n")
                    f.write(f"Total Shifts: {worker_stats['total_shifts']}")
                    f.write(f" (Target: {worker_stats['target_shifts']})\n")
                    f.write(f"Weekend Shifts: {worker_stats['weekend_shifts']}\n")
                    f.write(f"Weekday Shifts: {worker_stats['weekday_shifts']}\n\n")
                
                    f.write("Monthly Distribution:\n")
                    for month, count in worker_stats['monthly_distribution'].items():
                        f.write(f"  {month}: {count} shifts\n")
                
                    f.write("\nWeekday Distribution:\n")
                    days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 
                           'Friday', 'Saturday', 'Sunday']
                    for day_num, count in worker_stats['weekday_distribution'].items():
                        f.write(f"  {days[day_num]}: {count} shifts\n")
                
                    if worker_stats['total_shifts'] > 1:
                        f.write("\nGaps Analysis:\n")
                        f.write(f"  Average gap: {worker_stats['average_gap']:.1f} days\n")
                        f.write(f"  Minimum gap: {worker_stats['min_gap']} days\n")
                        f.write(f"  Maximum gap: {worker_stats['max_gap']} days\n")
                
                    f.write("\n" + "-"*50 + "\n")
    
        elif format.lower() == 'pdf':
            try:
                from reportlab.lib import colors
                from reportlab.lib.pagesizes import letter
                from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph
                from reportlab.lib.styles import getSampleStyleSheet
            
                filename = f'worker_stats_{timestamp}.pdf'
                doc = SimpleDocTemplate(filename, pagesize=letter)
                elements = []
                styles = getSampleStyleSheet()
            
                # Add content to PDF
                # ... (PDF generation code would go here)
            # We can implement this part if you want to use PDF format
            
            except ImportError:
                raise ImportError("reportlab is required for PDF export. "
                                "Install it with: pip install reportlab")
    
        return filename
