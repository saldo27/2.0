# Imports
import logging
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Dict, List, Set, Any, Optional
from collections import defaultdict, Counter
from exceptions import SchedulerError
from performance_cache import cached, memoize, time_function, monitor_performance
if TYPE_CHECKING:
    from scheduler import Schedulerr

class StatisticsCalculator:
    """Calculates statistics and metrics for schedules"""
    
    def __init__(self, scheduler):
        """
        Initialize the statistics calculator
    
        Args:
            scheduler: The main Scheduler object
        """
        self.scheduler = scheduler
    
        # Store references to frequently accessed attributes
        self.schedule = scheduler.schedule
        self.worker_assignments = scheduler.worker_assignments
        self.worker_posts = scheduler.worker_posts
        self.worker_weekdays = scheduler.worker_weekdays
        self.worker_weekends = scheduler.worker_weekends
        self.workers_data = scheduler.workers_data
        self.num_shifts = scheduler.num_shifts
        self.holidays = scheduler.holidays  # Add this line to reference holidays
        self.start_date = scheduler.start_date  # Add start_date reference
        self.end_date = scheduler.end_date      # Add end_date reference
        
        # Add constraint_skips reference
        self.constraint_skips = scheduler.constraint_skips
        
        # Performance optimization: Pre-compute worker ID set for O(1) lookups
        self.worker_ids = {worker['id'] for worker in self.workers_data}
        
        # Cache for expensive computations
        self._monthly_distribution_cache = {}
        self._gaps_analysis_cache = {}
        self._post_counts_cache = {}
    
        logging.info("StatisticsCalculator initialized with performance optimizations")
    
    @memoize(maxsize=256)
    def get_post_counts(self, worker_id):
        """
        Get the counts of different posts for a worker (cached for performance)
        
        Args:
            worker_id: The worker ID to check
        Returns:
            dict: Dictionary mapping post numbers to counts
        """
        if worker_id not in self.worker_posts:
            return {}
        
        # Use Counter for efficient counting
        return dict(Counter(self.worker_posts[worker_id]))
    
    @cached(ttl=1800)  # Cache for 30 minutes
    def _get_monthly_distribution(self, worker_id):
        """
        Get monthly shift distribution for a worker (cached for performance)
        
        Args:
            worker_id: The worker's ID
        Returns:
            dict: Monthly shift counts {YYYY-MM: count}
        """
        # Use defaultdict for efficient counting
        distribution = defaultdict(int)
        
        # Get assignments once and iterate efficiently
        assignments = self.worker_assignments.get(worker_id, set())
        
        for date in assignments:
            month_key = f"{date.year}-{date.month:02d}"
            distribution[month_key] += 1
            
        return dict(distribution)
    
    @cached(ttl=1800)  # Cache for 30 minutes  
    def _analyze_gaps(self, worker_id):
        """
        Analyze gaps between shifts for a worker (cached for performance)
        
        Args:
            worker_id: The worker's ID
        Returns:
            dict: Statistics about gaps between assignments
        """
        assignments = self.worker_assignments.get(worker_id, set())
        
        if len(assignments) <= 1:
            return {'min_gap': None, 'max_gap': None, 'avg_gap': None}

        # Sort assignments once
        sorted_assignments = sorted(assignments)
        
        # Calculate gaps in a single pass using list comprehension
        gaps = [(sorted_assignments[i+1] - sorted_assignments[i]).days 
                for i in range(len(sorted_assignments)-1)]
        
        if not gaps:
            return {'min_gap': None, 'max_gap': None, 'avg_gap': None}
        
        # Use built-in functions for efficiency
        return {
            'min_gap': min(gaps),
            'max_gap': max(gaps),
            'avg_gap': sum(gaps) / len(gaps)
        }

    def _get_least_used_weekday(self, worker_id):
        """
        Get the weekday that has been used least often for this worker
        
        Args:
            worker_id: The worker's ID
        Returns:
            int: Weekday number (0-6 for Monday-Sunday)
        """
        weekdays = self.worker_weekdays[worker_id]
        min_count = min(weekdays.values())
        # If there are multiple weekdays with the same minimum count,
        # prefer the earliest one in the week
        for weekday in range(7):  # 0-6 for Monday-Sunday
            if weekdays[weekday] == min_count:
                return weekday
        return 0  # Fallback to Monday if something goes wrong

    @time_function
    @monitor_performance("gather_statistics")
    def gather_statistics(self):
        """
        Gather comprehensive schedule statistics (optimized for performance)
        
        Returns:
            dict: Detailed statistics about the schedule and worker assignments
        """
        # Pre-calculate general stats once
        total_days = (self.end_date - self.start_date).days + 1
        total_shifts = sum(len(shifts) for shifts in self.schedule.values())
        
        # Calculate constraint skips efficiently using generator expressions
        constraint_skips_summary = {
            'gap': sum(len(skips.get('gap', [])) for skips in self.constraint_skips.values()),
            'incompatibility': sum(len(skips.get('incompatibility', [])) for skips in self.constraint_skips.values()),
            'reduced_gap': sum(len(skips.get('reduced_gap', [])) for skips in self.constraint_skips.values())
        }
        
        stats = {
            'general': {
                'total_days': total_days,
                'total_shifts': total_shifts,
                'constraint_skips': constraint_skips_summary
            },
            'workers': {}
        }

        # Process workers in batch for better cache locality
        for worker in self.workers_data:
            worker_id = worker['id']
            
            # Get all data for this worker once
            assignments = self.worker_assignments.get(worker_id, set())
            monthly_dist = self._get_monthly_distribution(worker_id)
            
            # Calculate monthly stats efficiently
            monthly_values = list(monthly_dist.values()) if monthly_dist else [0]
            monthly_stats = {
                'distribution': monthly_dist,
                'min_monthly': min(monthly_values),
                'max_monthly': max(monthly_values),
                'monthly_imbalance': max(monthly_values) - min(monthly_values)
            }
            
            stats['workers'][worker_id] = {
                'total_shifts': len(assignments),
                'target_shifts': worker.get('target_shifts', 0),
                'work_percentage': worker.get('work_percentage', 100),
                'weekend_shifts': len(self.worker_weekends.get(worker_id, set())),
                'weekday_distribution': self.worker_weekdays.get(worker_id, {}),
                'post_distribution': self.get_post_counts(worker_id),
                'constraint_skips': self.constraint_skips.get(worker_id, {}),
                'monthly_stats': monthly_stats,
                'gaps_analysis': self._analyze_gaps(worker_id)
            }

        # Add monthly balance analysis
        stats['monthly_balance'] = self._analyze_monthly_balance()
        
        return stats

    @cached(ttl=3600)  # Cache for 1 hour
    def _analyze_monthly_balance(self):
        """
        Analyze monthly balance across all workers (cached for performance)
        
        Returns:
            dict: Statistics about monthly distribution balance
        """
        monthly_stats = {}
        
        # Collect all months efficiently using set comprehension
        all_months = {
            f"{date.year}-{date.month:02d}"
            for assignments in self.worker_assignments.values()
            for date in assignments
        }
        
        # Pre-compute monthly distributions for all workers
        worker_monthly_distributions = {
            worker_id: self._get_monthly_distribution(worker_id)
            for worker_id in self.worker_assignments
        }
        
        for month in sorted(all_months):
            # Use list comprehension for efficient data collection
            worker_counts = [
                worker_dist.get(month, 0)
                for worker_dist in worker_monthly_distributions.values()
            ]
            
            if worker_counts:
                monthly_stats[month] = {
                    'min_shifts': min(worker_counts),
                    'max_shifts': max(worker_counts),
                    'avg_shifts': sum(worker_counts) / len(worker_counts),
                    'imbalance': max(worker_counts) - min(worker_counts)
                }
        
        return monthly_stats
    
    def _get_worker_shift_ratio(self, worker_id):
        """
        Calculate the ratio of assigned shifts to target shifts for a worker
        
        Args:
            worker_id: The worker's ID
        Returns:
            float: Ratio of assigned/target shifts (1.0 = perfect match)
        """
        worker = next(w for w in self.workers_data if w['id'] == worker_id)
        target = worker.get('target_shifts', 0)
        if target == 0:
            return 0
        return len(self.worker_assignments[worker_id]) / target
   
    def _calculate_post_rotation_coverage(self):
        """Calculate how well the post rotation is working across all workers"""
        worker_scores = {}
        total_score = 0
    
        for worker in self.workers_data:
            worker_id = worker['id']
            post_counts = self._get_post_counts(worker_id)
            total_assignments = sum(post_counts.values())
        
            if total_assignments == 0:
                worker_scores[worker_id] = 100  # No assignments, perfect score
                continue
            
            post_imbalance = 0
            expected_per_post = total_assignments / self.num_shifts
        
            for post in range(self.num_shifts):
                post_count = post_counts.get(post, 0)
                post_imbalance += abs(post_count - expected_per_post)
        
            # Calculate a score where 0 imbalance = 100%
            imbalance_ratio = post_imbalance / total_assignments
            worker_score = max(0, 100 - (imbalance_ratio * 100))
            worker_scores[worker_id] = worker_score
        
        # Calculate overall score
        if worker_scores:
            total_score = sum(worker_scores.values()) / len(worker_scores)
        else:
            total_score = 0
        
        return {
            'overall_score': total_score,
            'worker_scores': worker_scores
        }
    
    def _calculate_weekday_imbalance(self, worker_id, date):
        """Calculate how much this assignment would affect weekday balance"""
        weekday = date.weekday()
        counts = self.worker_weekdays[worker_id].copy()
        counts[weekday] += 1
        return max(counts.values()) - min(counts.values())
    
    def _calculate_coverage(self):
        """Calculate schedule coverage percentage"""
        total_required_shifts = (
            (self.end_date - self.start_date).days + 1
        ) * self.num_shifts
        
        actual_shifts = sum(len(shifts) for shifts in self.schedule.values())
        return (actual_shifts / total_required_shifts) * 100

    def _calculate_balance_score(self):
        """Calculate overall balance score based on various factors"""
        scores = []
    
        # Post rotation balance
        for worker_id in self.worker_assignments:
            post_counts = self._get_post_counts(worker_id)
            if post_counts.values():
                post_imbalance = max(post_counts.values()) - min(post_counts.values())
                scores.append(max(0, 100 - (post_imbalance * 20)))
    
        # Weekday distribution balance
        for worker_id, weekdays in self.worker_weekdays.items():
            if weekdays.values():
                weekday_imbalance = max(weekdays.values()) - min(weekdays.values())
                scores.append(max(0, 100 - (weekday_imbalance * 20)))
    
        return sum(scores) / len(scores) if scores else 0
    
    def _count_constraint_violations(self):
        """Count total constraint violations"""
        return {
            'gap_violations': sum(
                len(skips['gap']) for skips in self.constraint_skips.values()
            ),
            'incompatibility_violations': sum(
                len(skips['incompatibility']) for skips in self.constraint_skips.values()
            ),
            'reduced_gap_violations': sum(
                len(skips['reduced_gap']) for skips in self.constraint_skips.values()
            )
        }

    @time_function
    @monitor_performance("calculate_statistics")
    def calculate_statistics(self):
        """
        Calculate comprehensive statistics for the entire schedule (optimized)
        Returns a dictionary with various statistics
        """
        stats = {
            'workers': {},
            'schedule': {},
            'coverage': 0.0
        }
    
        # Get schedule data once
        schedule = self.scheduler.schedule
        worker_assignments = self.scheduler.worker_assignments
    
        # Calculate coverage efficiently
        total_shifts = (self.scheduler.end_date - self.scheduler.start_date).days * self.scheduler.num_shifts
        filled_shifts = sum(
            sum(1 for worker in shifts if worker is not None) 
            for shifts in schedule.values()
        )
        coverage = (filled_shifts / total_shifts * 100) if total_shifts > 0 else 0
        stats['coverage'] = round(coverage, 2)
    
        # Pre-compute holidays set for O(1) lookup
        holidays_set = set(self.scheduler.holidays)
        
        # Calculate stats for each worker efficiently
        for worker in self.scheduler.workers_data:
            worker_id = worker['id']
            worker_name = worker.get('name', worker_id)
        
            # Get assignments for this worker
            assignments = worker_assignments.get(worker_id, set())
            total_shifts = len(assignments)
        
            # Get target shifts
            target_shifts = worker.get('target_shifts', 0)
        
            # Calculate weekend and holiday shifts efficiently
            weekend_shifts = sum(
                1 for date in assignments 
                if date.weekday() >= 4 or date in holidays_set
            )
            weekday_shifts = total_shifts - weekend_shifts
        
            # Calculate post distribution efficiently
            post_distribution = defaultdict(int)
            for date in assignments:
                shifts_for_date = schedule.get(date, [])
                try:
                    if worker_id in shifts_for_date:
                        post = shifts_for_date.index(worker_id)
                        post_distribution[post] += 1
                except ValueError:
                    # Skip if worker_id isn't in the list
                    pass
        
            # Store worker stats
            stats['workers'][worker_id] = {
                'name': worker_name,
                'total_shifts': total_shifts,
                'target_shifts': target_shifts,
                'weekend_shifts': weekend_shifts,
                'weekday_shifts': weekday_shifts,
                'post_distribution': dict(post_distribution)
            }
    
        return stats

    def _calculate_worker_satisfaction(self):
        """Calculate worker satisfaction score based on preferences and constraints"""
        satisfaction_scores = []
        
        for worker in self.workers_data:
            worker_id = worker['id']
            assignments = len(self.worker_assignments[worker_id])
            target = worker.get('target_shifts', 0)
            
            # Calculate basic satisfaction score
            if target > 0:
                target_satisfaction = 100 - (abs(assignments - target) / target * 100)
                satisfaction_scores.append(target_satisfaction)
            
            # Deduct points for constraint violations
            violations = sum(len(v) for v in self.constraint_skips[worker_id].values())
            if violations > 0:
                violation_penalty = min(violations * 10, 50)  # Cap penalty at 50%
                satisfaction_scores.append(100 - violation_penalty)
        
        return sum(satisfaction_scores) / len(satisfaction_scores) if satisfaction_scores else 0
    
    def get_schedule_metrics(self):
        """
        Calculate schedule performance metrics
        
        Returns:
            dict: Dictionary containing various schedule performance metrics
        """
        metrics = {
            'coverage': self._calculate_coverage(),
            'balance_score': self._calculate_balance_score(),
            'constraint_violations': self._count_constraint_violations(),
            'worker_satisfaction': self._calculate_worker_satisfaction()
        }
        
        logging.info("Generated schedule metrics")
        return metrics

    def _generate_schedule_header(self):
        """Generate the header section of the schedule output"""
        return (
            "=== Guard Schedule ===\n"
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"Generated by: {self.current_user}\n"
            f"Period: {self.start_date.strftime('%Y-%m-%d')} to {self.end_date.strftime('%Y-%m-%d')}\n"
            f"Total Workers: {len(self.workers_data)}\n"
            f"Shifts per Day: {self.num_shifts}\n"
            "=" * 40 + "\n\n"
        )
    
    def _generate_schedule_body(self):
        """Generate the main body of the schedule output"""
        output = ""
        for date in sorted(self.schedule.keys()):
            # Date header
            output += f"\n{date.strftime('%Y-%m-%d')} ({date.strftime('%A')})"
            if self._is_holiday(date):
                output += " [HOLIDAY]"
            elif self._is_pre_holiday(date):
                output += " [PRE-HOLIDAY]"
            output += "\n"
            
            # Shift assignments
            for i, worker_id in enumerate(self.schedule[date], 1):
                worker = next(w for w in self.workers_data if w['id'] == worker_id)
                output += f"  Shift {i}: Worker {worker_id}"
                
                # Add worker details
                work_percentage = worker.get('work_percentage', 100)
                if float(work_percentage) < 100:
                    output += f" (Part-time: {work_percentage}%)"
                
                # Add post rotation info
                post_counts = self._get_post_counts(worker_id)
                output += f" [Post {i} count: {post_counts.get(i-1, 0)}]"
                
                output += "\n"
            output += "-" * 40 + "\n"
        
        return output

    def _generate_schedule_summary(self):
        """Generate summary statistics for the schedule output"""
        stats = self.gather_statistics()
        
        summary = "\n=== Schedule Summary ===\n"
        summary += f"Total Days: {stats['general']['total_days']}\n"
        summary += f"Total Shifts: {stats['general']['total_shifts']}\n"
        
        # Constraint skip summary
        summary += "\nConstraint Skips:\n"
        for skip_type, count in stats['general']['constraint_skips'].items():
            summary += f"  {skip_type.title()}: {count}\n"
        
        # Worker summary
        summary += "\nWorker Statistics:\n"
        for worker_id, worker_stats in stats['workers'].items():
            summary += f"\nWorker {worker_id}:\n"
            summary += f"  Assigned/Target: {worker_stats['total_shifts']}/{worker_stats['target_shifts']}\n"
            summary += f"  Weekend Shifts: {worker_stats['weekend_shifts']}\n"
            
            # Monthly distribution
            monthly_stats = worker_stats['monthly_stats']
            summary += "  Monthly Distribution:\n"
            for month, count in monthly_stats['distribution'].items():
                summary += f"    {month}: {count}\n"
        
        summary += "\n" + "=" * 40 + "\n"
        return summary

    
    def generate_worker_report(self, worker_id):
        """
        Generate a detailed report for a specific worker
    
        Args:
            worker_id: The worker's ID to generate report for
        Returns:
            str: Formatted report text
        """
        try:
            # Find worker details
            worker = next((w for w in self.workers_data if w['id'] == worker_id), None)
            if not worker:
                return f"Error: Worker {worker_id} not found"
        
            # Get worker assignments
            assignments = sorted(list(self.worker_assignments.get(worker_id, set())))
            if not assignments:
                return f"Worker {worker_id} has no assignments in the schedule"
            
            # Start building the report
            report = []
            report.append(f"=== Worker Schedule Report: Worker {worker_id} ===")
            report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} UTC")
            report.append(f"Generated by: {self.scheduler.current_user}")
            report.append("=" * 60)
        
            # Add worker details
            report.append("\nWORKER DETAILS")
            report.append(f"Work Percentage: {worker.get('work_percentage', 100)}%")
            report.append(f"Target Shifts: {worker.get('target_shifts', 0)}")
            report.append(f"Actual Shifts: {len(assignments)} ({len(assignments) - worker.get('target_shifts', 0):+d})")
        
            # Add schedule summary statistics
            post_counts = {}
            weekday_counts = self.worker_weekdays.get(worker_id, {i: 0 for i in range(7)})
            weekend_count = sum(1 for date in assignments 
                               if self.scheduler.date_utils.is_weekend_day(date, self.scheduler.holidays))
            holiday_count = sum(1 for date in assignments if date in self.scheduler.holidays)
        
            # Calculate post distribution
            for date in assignments:
                if date in self.scheduler.schedule:
                    try:
                        post = self.scheduler.schedule[date].index(worker_id)
                        post_counts[post] = post_counts.get(post, 0) + 1
                    except ValueError:
                        # This would indicate a data inconsistency
                        pass
        
            # Add assignment statistics
            report.append("\nASSIGNMENT STATISTICS")
            report.append(f"Total Shifts: {len(assignments)}")
            report.append(f"Weekend Shifts: {weekend_count} ({weekend_count/len(assignments)*100:.1f}% of total)")
            report.append(f"Holiday Shifts: {holiday_count}")
        
            # Monthly distribution
            monthly_dist = self._get_monthly_distribution(worker_id)
            report.append("\nMONTHLY DISTRIBUTION")
            for month, count in sorted(monthly_dist.items()):
                report.append(f"  {month}: {count} shifts")
        
            # Weekday distribution
            report.append("\nWEEKDAY DISTRIBUTION")
            days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
            for day_num, count in sorted(weekday_counts.items()):
                report.append(f"  {days[day_num]}: {count} shifts")
        
            # Post distribution
            report.append("\nPOST DISTRIBUTION")
            for post, count in sorted(post_counts.items()):
                report.append(f"  Post {post+1}: {count} shifts ({count/len(assignments)*100:.1f}%)")
            
            # Post balance analysis
            total_posts = sum(post_counts.values())
            expected_per_post = total_posts / self.scheduler.num_shifts
            post_imbalance = sum(abs(count - expected_per_post) for count in post_counts.values())
            post_balance_score = max(0, 100 - (post_imbalance / total_posts * 100)) if total_posts > 0 else 100
            report.append(f"\nPost Balance Score: {post_balance_score:.1f}%")
        
            # Get gap analysis
            gaps = self._analyze_gaps(worker_id)
            report.append("\nSCHEDULE GAPS")
            if gaps['min_gap'] is not None:
                report.append(f"  Minimum gap: {gaps['min_gap']} days")
                report.append(f"  Maximum gap: {gaps['max_gap']} days")
                report.append(f"  Average gap: {gaps['avg_gap']:.1f} days")
            else:
                report.append("  No gaps (only one assignment)")
        
            # Constraint violations
            report.append("\nCONSTRAINT VIOLATIONS")
            violations = self.scheduler.constraint_skips.get(worker_id, {})
            violation_count = sum(len(v) for v in violations.values())
        
            if violation_count == 0:
                report.append("  No constraint violations")
            else:
                for constraint_type, skips in violations.items():
                    if skips:
                        report.append(f"  {constraint_type.replace('_', ' ').title()}: {len(skips)}")
                        for skip in skips[:5]:  # Show up to 5 violations per type
                            report.append(f"    - {skip}")
                        if len(skips) > 5:
                            report.append(f"    (... and {len(skips) - 5} more)")
        
            # Detailed schedule
            report.append("\nDETAILED SCHEDULE")
            for date in assignments:
                date_str = date.strftime('%Y-%m-%d')
                day_name = date.strftime('%A')
            
                post = "Unknown"
                if date in self.scheduler.schedule:
                    try:
                        post_index = self.scheduler.schedule[date].index(worker_id)
                        post = f"Post {post_index + 1}"
                    except ValueError:
                        post = "Not found in day schedule"
                    
                day_type = ""
                if date in self.scheduler.holidays:
                    day_type = " [HOLIDAY]"
                elif date.weekday() >= 4:  # Friday, Saturday or Sunday
                    day_type = " [WEEKEND]"
                                
                report.append(f"  {date_str} ({day_name}){day_type}: {post}")
        
            # Future recommendations
            report.append("\nRECOMMENDATIONS")
        
            # Check post distribution balance
            if post_balance_score < 85:
                report.append("  - Post balance needs improvement")
            
            # Check weekday distribution
            max_weekday = max(weekday_counts.values())
            min_weekday = min(weekday_counts.values())
            if max_weekday - min_weekday > 1:
                report.append("  - Weekday distribution is unbalanced")
            
            # Check target alignment
            target_diff = len(assignments) - worker.get('target_shifts', 0)
            if abs(target_diff) > 1:
                report.append(f"  - Worker is {'over-assigned' if target_diff > 0 else 'under-assigned'} "
                             f"by {abs(target_diff)} shifts")
        
            report.append("\n" + "=" * 60)
            return "\n".join(report)
        
        except Exception as e:
            logging.error(f"Error generating report for worker {worker_id}: {str(e)}", exc_info=True)
            return f"Error generating report: {str(e)}"
    
    

