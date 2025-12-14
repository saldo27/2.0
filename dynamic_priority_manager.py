"""
Dynamic Priority Manager
Adjusts scoring weights dynamically based on schedule generation progress
"""

import logging
from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime, timedelta
from dataclasses import dataclass, field


@dataclass
class PriorityWeights:
    """Dynamic weights for scoring calculations"""
    mandatory_weight: float = float('inf')  # Always maximum priority
    target_deficit_weight: float = 2000.0
    monthly_target_weight: float = 2000.0
    target_excess_penalty: float = -1500.0
    weekend_balance_penalty: float = -300.0
    weekly_balance_bonus: float = 500.0
    post_rotation_bonus: float = 10.0
    progression_bonus: float = 500.0
    
    # New dynamic weights
    coverage_urgency_multiplier: float = 1.0
    balance_urgency_multiplier: float = 1.0
    constraint_strictness_multiplier: float = 1.0


@dataclass
class ScheduleProgress:
    """Tracks schedule generation progress metrics"""
    total_shifts: int
    filled_shifts: int
    empty_shifts: int
    coverage_percentage: float
    phase: str  # 'early', 'middle', 'late', 'final'
    
    # Balance metrics
    workload_imbalance_score: float = 0.0
    weekend_imbalance_score: float = 0.0
    post_rotation_imbalance: float = 0.0
    
    # Problem indicators
    has_critical_gaps: bool = False
    has_severe_imbalance: bool = False
    has_constraint_violations: int = 0
    stuck_iterations: int = 0


class DynamicPriorityManager:
    """
    Manages dynamic adjustment of priority weights based on schedule generation progress.
    
    This system monitors the schedule state and adapts scoring priorities to focus on
    the most critical issues at each stage of generation.
    """
    
    def __init__(self, scheduler):
        """
        Initialize the dynamic priority manager.
        
        Args:
            scheduler: Reference to the main Scheduler instance
        """
        self.scheduler = scheduler
        self.base_weights = PriorityWeights()
        self.current_weights = PriorityWeights()
        self.progress_history: List[ScheduleProgress] = []
        
        # Phase thresholds
        self.early_phase_threshold = 0.30  # < 30% filled
        self.middle_phase_threshold = 0.60  # 30-60% filled
        self.late_phase_threshold = 0.85   # 60-85% filled
        # > 85% is final phase
        
        logging.info("DynamicPriorityManager initialized")
    
    def analyze_progress(self) -> ScheduleProgress:
        """
        Analyze current schedule generation progress.
        
        Returns:
            ScheduleProgress object with current metrics
        """
        # Calculate basic coverage
        total_shifts = 0
        filled_shifts = 0
        
        for date, shifts in self.scheduler.schedule.items():
            if shifts:
                total_shifts += len(shifts)
                filled_shifts += sum(1 for worker in shifts if worker is not None)
        
        empty_shifts = total_shifts - filled_shifts
        coverage_percentage = (filled_shifts / total_shifts * 100) if total_shifts > 0 else 0
        
        # Determine phase
        if coverage_percentage < self.early_phase_threshold * 100:
            phase = 'early'
        elif coverage_percentage < self.middle_phase_threshold * 100:
            phase = 'middle'
        elif coverage_percentage < self.late_phase_threshold * 100:
            phase = 'late'
        else:
            phase = 'final'
        
        # Calculate balance metrics
        workload_imbalance = self._calculate_workload_imbalance()
        weekend_imbalance = self._calculate_weekend_imbalance()
        post_rotation_imbalance = self._calculate_post_rotation_imbalance()
        
        # Detect problems
        has_critical_gaps = self._detect_critical_gaps()
        has_severe_imbalance = workload_imbalance > 20.0 or weekend_imbalance > 20.0
        constraint_violations = len(self._check_constraint_violations())
        
        # Check if stuck (from progress history)
        stuck_iterations = self._count_stuck_iterations()
        
        progress = ScheduleProgress(
            total_shifts=total_shifts,
            filled_shifts=filled_shifts,
            empty_shifts=empty_shifts,
            coverage_percentage=coverage_percentage,
            phase=phase,
            workload_imbalance_score=workload_imbalance,
            weekend_imbalance_score=weekend_imbalance,
            post_rotation_imbalance=post_rotation_imbalance,
            has_critical_gaps=has_critical_gaps,
            has_severe_imbalance=has_severe_imbalance,
            has_constraint_violations=constraint_violations,
            stuck_iterations=stuck_iterations
        )
        
        # Store in history
        self.progress_history.append(progress)
        
        return progress
    
    def adjust_weights(self, progress: Optional[ScheduleProgress] = None) -> PriorityWeights:
        """
        Dynamically adjust scoring weights based on current progress.
        
        Args:
            progress: Current schedule progress (will analyze if not provided)
            
        Returns:
            Adjusted PriorityWeights object
        """
        if progress is None:
            progress = self.analyze_progress()
        
        # Start with base weights
        weights = PriorityWeights()
        
        # Phase-based adjustments
        if progress.phase == 'early':
            weights = self._adjust_for_early_phase(weights, progress)
        elif progress.phase == 'middle':
            weights = self._adjust_for_middle_phase(weights, progress)
        elif progress.phase == 'late':
            weights = self._adjust_for_late_phase(weights, progress)
        else:  # final
            weights = self._adjust_for_final_phase(weights, progress)
        
        # Problem-based adjustments
        if progress.has_critical_gaps:
            weights = self._adjust_for_critical_gaps(weights, progress)
        
        if progress.has_severe_imbalance:
            weights = self._adjust_for_severe_imbalance(weights, progress)
        
        if progress.has_constraint_violations > 0:
            weights = self._adjust_for_violations(weights, progress)
        
        if progress.stuck_iterations > 3:
            weights = self._adjust_for_stagnation(weights, progress)
        
        self.current_weights = weights
        
        self._log_weight_adjustments(progress, weights)
        
        return weights
    
    def _adjust_for_early_phase(self, weights: PriorityWeights, 
                               progress: ScheduleProgress) -> PriorityWeights:
        """
        Adjust weights for early phase (< 30% coverage).
        
        Priority: Fill shifts quickly, focus on coverage over balance.
        """
        logging.debug(f"Adjusting weights for EARLY phase ({progress.coverage_percentage:.1f}%)")
        
        # Increase coverage urgency dramatically
        weights.coverage_urgency_multiplier = 2.5
        
        # Increase weight for workers below target (encourage filling)
        weights.target_deficit_weight = 3000.0  # Up from 2000
        
        # Reduce penalties for exceeding targets (we need to fill shifts)
        weights.target_excess_penalty = -800.0  # Less penalty
        
        # Reduce balance concerns (not critical yet)
        weights.weekend_balance_penalty = -150.0  # Half penalty
        weights.weekly_balance_bonus = 300.0
        
        # Relax constraint strictness slightly for coverage
        weights.constraint_strictness_multiplier = 0.9
        
        return weights
    
    def _adjust_for_middle_phase(self, weights: PriorityWeights, 
                                progress: ScheduleProgress) -> PriorityWeights:
        """
        Adjust weights for middle phase (30-60% coverage).
        
        Priority: Balance coverage with initial workload balancing.
        """
        logging.debug(f"Adjusting weights for MIDDLE phase ({progress.coverage_percentage:.1f}%)")
        
        # Moderate coverage urgency
        weights.coverage_urgency_multiplier = 1.5
        
        # Start caring more about balance
        weights.target_deficit_weight = 2500.0
        weights.target_excess_penalty = -1200.0
        
        # Start enforcing weekend balance
        weights.weekend_balance_penalty = -250.0
        weights.weekly_balance_bonus = 400.0
        
        # Begin considering post rotation
        weights.post_rotation_bonus = 15.0
        
        # Standard constraint strictness
        weights.constraint_strictness_multiplier = 1.0
        
        return weights
    
    def _adjust_for_late_phase(self, weights: PriorityWeights, 
                              progress: ScheduleProgress) -> PriorityWeights:
        """
        Adjust weights for late phase (60-85% coverage).
        
        Priority: Fine-tune balance while maintaining coverage.
        """
        logging.debug(f"Adjusting weights for LATE phase ({progress.coverage_percentage:.1f}%)")
        
        # Reduced coverage urgency
        weights.coverage_urgency_multiplier = 1.2
        
        # Strong focus on balance
        weights.balance_urgency_multiplier = 1.5
        weights.target_deficit_weight = 2200.0
        weights.target_excess_penalty = -1800.0  # Stronger penalty
        
        # Enforce balance metrics
        weights.weekend_balance_penalty = -400.0  # Stronger penalty
        weights.weekly_balance_bonus = 600.0
        weights.post_rotation_bonus = 20.0
        
        # Stricter constraints
        weights.constraint_strictness_multiplier = 1.1
        
        return weights
    
    def _adjust_for_final_phase(self, weights: PriorityWeights, 
                               progress: ScheduleProgress) -> PriorityWeights:
        """
        Adjust weights for final phase (> 85% coverage).
        
        Priority: Perfect balance and constraint adherence.
        """
        logging.debug(f"Adjusting weights for FINAL phase ({progress.coverage_percentage:.1f}%)")
        
        # Minimal coverage urgency (should be mostly filled)
        weights.coverage_urgency_multiplier = 1.0
        
        # Maximum focus on balance
        weights.balance_urgency_multiplier = 2.0
        weights.target_deficit_weight = 2000.0
        weights.target_excess_penalty = -2500.0  # Very strong penalty
        
        # Perfect balance requirements
        weights.weekend_balance_penalty = -500.0
        weights.weekly_balance_bonus = 800.0
        weights.post_rotation_bonus = 30.0
        weights.progression_bonus = 800.0
        
        # Very strict constraints
        weights.constraint_strictness_multiplier = 1.2
        
        return weights
    
    def _adjust_for_critical_gaps(self, weights: PriorityWeights, 
                                 progress: ScheduleProgress) -> PriorityWeights:
        """Adjust weights when critical gaps detected (weekends, holidays)."""
        logging.debug("Adjusting weights for CRITICAL GAPS")
        
        # Dramatically increase coverage urgency
        weights.coverage_urgency_multiplier *= 1.5
        
        # Increase deficit weight to fill gaps
        weights.target_deficit_weight *= 1.3
        
        # Temporarily relax some balance concerns
        weights.weekend_balance_penalty *= 0.7
        
        return weights
    
    def _adjust_for_severe_imbalance(self, weights: PriorityWeights, 
                                    progress: ScheduleProgress) -> PriorityWeights:
        """Adjust weights when severe workload imbalance detected."""
        logging.debug(f"Adjusting weights for SEVERE IMBALANCE "
                     f"(workload: {progress.workload_imbalance_score:.1f}, "
                     f"weekend: {progress.weekend_imbalance_score:.1f})")
        
        # Dramatically increase balance urgency
        weights.balance_urgency_multiplier *= 2.0
        
        # Stronger penalties for excess
        weights.target_excess_penalty *= 1.5
        
        # Stronger bonuses for balanced distribution
        weights.weekly_balance_bonus *= 1.5
        weights.post_rotation_bonus *= 1.5
        
        # Focus on weekend balance if that's the issue
        if progress.weekend_imbalance_score > progress.workload_imbalance_score:
            weights.weekend_balance_penalty *= 1.5
        
        return weights
    
    def _adjust_for_violations(self, weights: PriorityWeights, 
                              progress: ScheduleProgress) -> PriorityWeights:
        """Adjust weights when constraint violations detected."""
        logging.debug(f"Adjusting weights for {progress.has_constraint_violations} VIOLATIONS")
        
        # Increase constraint strictness
        violation_factor = min(2.0, 1.0 + (progress.has_constraint_violations / 10.0))
        weights.constraint_strictness_multiplier *= violation_factor
        
        # Reduce coverage urgency to focus on fixing violations
        weights.coverage_urgency_multiplier *= 0.8
        
        return weights
    
    def _adjust_for_stagnation(self, weights: PriorityWeights, 
                              progress: ScheduleProgress) -> PriorityWeights:
        """Adjust weights when progress stagnates."""
        logging.debug(f"Adjusting weights for STAGNATION ({progress.stuck_iterations} iterations)")
        
        # Randomize weights slightly to escape local optimum
        import random
        perturbation = 0.1 * progress.stuck_iterations / 10.0  # Max 10% perturbation
        
        weights.target_deficit_weight *= (1.0 + random.uniform(-perturbation, perturbation))
        weights.weekend_balance_penalty *= (1.0 + random.uniform(-perturbation, perturbation))
        weights.weekly_balance_bonus *= (1.0 + random.uniform(-perturbation, perturbation))
        
        # Relax constraints slightly to allow different solutions
        weights.constraint_strictness_multiplier *= 0.95
        
        return weights
    
    def _calculate_workload_imbalance(self) -> float:
        """Calculate overall workload imbalance score (0-100)."""
        if not self.scheduler.workers_data:
            return 0.0
        
        deviations = []
        for worker in self.scheduler.workers_data:
            worker_id = worker['id']
            target = worker.get('target_shifts', 0)
            actual = len(self.scheduler.worker_assignments.get(worker_id, set()))
            
            if target > 0:
                deviation = abs((actual - target) / target * 100)
                deviations.append(deviation)
        
        return sum(deviations) / len(deviations) if deviations else 0.0
    
    def _calculate_weekend_imbalance(self) -> float:
        """Calculate weekend shift imbalance score (0-100)."""
        if not self.scheduler.workers_data:
            return 0.0
        
        weekend_deviations = []
        holidays_set = set(self.scheduler.holidays)
        
        for worker in self.scheduler.workers_data:
            worker_id = worker['id']
            
            # Count weekend shifts
            weekend_count = 0
            for date in self.scheduler.worker_assignments.get(worker_id, set()):
                if (date.weekday() >= 4 or  # Fri, Sat, Sun
                    date in holidays_set):
                    weekend_count += 1
            
            # Calculate expected weekend shifts
            total_shifts = len(self.scheduler.worker_assignments.get(worker_id, set()))
            total_days = (self.scheduler.end_date - self.scheduler.start_date).days + 1
            weekend_days = sum(1 for i in range(total_days) 
                             if (self.scheduler.start_date + timedelta(days=i)).weekday() >= 4)
            
            expected_weekend_proportion = weekend_days / total_days if total_days > 0 else 0
            expected_weekend = total_shifts * expected_weekend_proportion
            
            if expected_weekend > 0:
                deviation = abs((weekend_count - expected_weekend) / expected_weekend * 100)
                weekend_deviations.append(deviation)
        
        return sum(weekend_deviations) / len(weekend_deviations) if weekend_deviations else 0.0
    
    def _calculate_post_rotation_imbalance(self) -> float:
        """Calculate post rotation imbalance score (0-100)."""
        # Use worker_post_counts which contains the actual count per post
        if not hasattr(self.scheduler, 'worker_post_counts') or not self.scheduler.worker_post_counts:
            return 0.0
        
        imbalances = []
        for worker_id, post_counts in self.scheduler.worker_post_counts.items():
            if not post_counts:
                continue
            
            # post_counts is a dict like {0: 5, 1: 3, 2: 4}
            total_assignments = sum(post_counts.values())
            if total_assignments < self.scheduler.num_shifts:
                continue  # Not enough data
            
            expected_per_post = total_assignments / self.scheduler.num_shifts
            
            for post, count in post_counts.items():
                if expected_per_post > 0:
                    deviation = abs((count - expected_per_post) / expected_per_post * 100)
                    imbalances.append(deviation)
        
        return sum(imbalances) / len(imbalances) if imbalances else 0.0
    
    def _detect_critical_gaps(self) -> bool:
        """Detect if there are critical gaps (weekends, holidays unfilled)."""
        holidays_set = set(self.scheduler.holidays)
        
        for date, shifts in self.scheduler.schedule.items():
            # Check if it's a weekend or holiday
            if date.weekday() >= 4 or date in holidays_set:
                # Check if any shifts are unfilled
                if any(worker is None for worker in shifts):
                    return True
        
        return False
    
    def _check_constraint_violations(self) -> List[str]:
        """Check for constraint violations in current schedule."""
        violations = []
        
        # Check if scheduler maintains a violations list
        if hasattr(self.scheduler, 'constraint_violations'):
            violations = self.scheduler.constraint_violations
        
        # Alternatively, do a simple validation by checking worker assignments
        # against basic constraints (gap between shifts, consecutive patterns, etc.)
        if not violations:
            violations = self._validate_basic_constraints()
        
        return violations
    
    def _validate_basic_constraints(self) -> List[str]:
        """Perform basic constraint validation."""
        violations = []
        
        try:
            for worker_id, assignments in self.scheduler.worker_assignments.items():
                if not assignments:
                    continue
                
                sorted_dates = sorted(assignments)
                
                # Check gap between shifts
                for i in range(len(sorted_dates) - 1):
                    days_diff = (sorted_dates[i + 1] - sorted_dates[i]).days
                    if days_diff < self.scheduler.gap_between_shifts:
                        violations.append(
                            f"Worker {worker_id}: gap violation ({days_diff} days)"
                        )
        except Exception as e:
            logging.debug(f"Error validating basic constraints: {e}")
        
        return violations
    
    def _count_stuck_iterations(self) -> int:
        """Count how many iterations we've been stuck without progress."""
        if len(self.progress_history) < 2:
            return 0
        
        stuck_count = 0
        last_coverage = self.progress_history[-1].coverage_percentage
        
        # Look back at last 5 iterations
        for i in range(min(5, len(self.progress_history) - 1)):
            idx = -(i + 2)  # -2, -3, -4, -5, -6
            if abs(self.progress_history[idx].coverage_percentage - last_coverage) < 0.5:
                stuck_count += 1
            else:
                break
        
        return stuck_count
    
    def _log_weight_adjustments(self, progress: ScheduleProgress, 
                               weights: PriorityWeights) -> None:
        """Log the weight adjustments made."""
        logging.info(f"🎯 Dynamic Priority Adjustment [{progress.phase.upper()}]:")
        logging.info(f"   Coverage: {progress.coverage_percentage:.1f}% "
                    f"({progress.filled_shifts}/{progress.total_shifts})")
        logging.info(f"   Workload Imbalance: {progress.workload_imbalance_score:.1f}")
        logging.info(f"   Weekend Imbalance: {progress.weekend_imbalance_score:.1f}")
        
        if progress.has_critical_gaps:
            logging.info(f"   ⚠️  Critical gaps detected")
        if progress.has_severe_imbalance:
            logging.info(f"   ⚠️  Severe imbalance detected")
        if progress.has_constraint_violations > 0:
            logging.info(f"   ⚠️  {progress.has_constraint_violations} constraint violations")
        
        logging.info(f"   Multipliers: Coverage={weights.coverage_urgency_multiplier:.2f}, "
                    f"Balance={weights.balance_urgency_multiplier:.2f}, "
                    f"Constraints={weights.constraint_strictness_multiplier:.2f}")
    
    def get_current_weights(self) -> PriorityWeights:
        """Get current active weights."""
        return self.current_weights
    
    def get_progress_summary(self) -> Dict[str, Any]:
        """Get summary of progress over time."""
        if not self.progress_history:
            return {"message": "No progress history available"}
        
        return {
            "total_iterations": len(self.progress_history),
            "initial_coverage": self.progress_history[0].coverage_percentage,
            "current_coverage": self.progress_history[-1].coverage_percentage,
            "current_phase": self.progress_history[-1].phase,
            "improvement": (self.progress_history[-1].coverage_percentage - 
                          self.progress_history[0].coverage_percentage),
            "workload_imbalance": self.progress_history[-1].workload_imbalance_score,
            "weekend_imbalance": self.progress_history[-1].weekend_imbalance_score,
            "has_critical_issues": (
                self.progress_history[-1].has_critical_gaps or
                self.progress_history[-1].has_severe_imbalance or
                self.progress_history[-1].has_constraint_violations > 0
            )
        }
