#!/usr/bin/env python3
"""
Iterative Optimization System for Schedule Assignment
Automatically retries and optimizes schedule assignments until tolerance requirements are met.
"""

import logging
import random
import copy
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Any, Optional
from dataclasses import dataclass

from shift_tolerance_validator import ShiftToleranceValidator
from balance_validator import BalanceValidator
from performance_cache import cached, time_function

@dataclass
class OptimizationResult:
    """Result of optimization attempt"""
    success: bool
    iteration: int
    total_violations: int
    general_violations: int
    weekend_violations: int
    schedule: Optional[Dict] = None
    validation_report: Optional[Dict] = None

class IterativeOptimizer:
    """
    Iterative optimization system that continuously improves schedule assignments
    until tolerance requirements are met.
    """
    
    def __init__(self, max_iterations: int = 50, tolerance: float = 0.12):
        """
        Initialize the iterative optimizer with enhanced redistribution algorithms.
        
        TOLERANCE SYSTEM:
        - This optimizer works with the ACTIVE tolerance phase from schedule_builder
        - Phase 1 (Initial): ±8% strict target
        - Phase 2 (Emergency): ±12% absolute maximum (NEVER exceeded)
        - Default tolerance=0.12 represents the absolute maximum boundary
        
        Args:
            max_iterations: Maximum number of optimization iterations (default: 50, increased from 30)
            tolerance: Maximum tolerance percentage (0.12 = 12% absolute limit)
        """
        self.max_iterations = max_iterations
        self.tolerance = tolerance
        self.convergence_threshold = 3  # Stop after 3 iterations without improvement
        self.stagnation_counter = 0
        self.best_result = None
        self.optimization_history = []
        self.weekend_only_mode = False  # Special mode when only weekend violations remain
        self.no_change_counter = 0  # Track iterations with zero changes
        self.max_no_change = 2  # Stop if no changes for 2 consecutive iterations
        
        # Constraint parameters - will be updated from scheduler config
        self.gap_between_shifts = 3  # Default minimum gap between shifts
        
        # Initialize balance validator for strict balance checking
        self.balance_validator = BalanceValidator(tolerance_percentage=tolerance * 100)
        
        logging.info(f"IterativeOptimizer initialized: max_iterations={max_iterations}, tolerance={tolerance:.1%}")
        logging.info(f"Default gap_between_shifts={self.gap_between_shifts} (will be updated from config)")
        logging.info(f"Balance validator initialized with {tolerance * 100}% tolerance")
    
    @time_function
    def optimize_schedule(self, scheduler_core, schedule: Dict, workers_data: List[Dict], 
                         schedule_config: Dict) -> OptimizationResult:
        """
        Optimize a schedule iteratively until tolerance requirements are met.
        
        Args:
            scheduler_core: The core scheduler instance
            schedule: Current schedule to optimize
            workers_data: Worker data configuration
            schedule_config: Schedule configuration parameters
            
        Returns:
            OptimizationResult with final optimization status
        """
        logging.info("🔄 Starting iterative schedule optimization...")
        # CRITICAL: Reset state for fresh optimization run
        self.stagnation_counter = 0
        self.no_change_counter = 0
        self.best_result = None
        self.optimization_history = []
        self.weekend_only_mode = False
        logging.info("🔄 Optimizer state reset for new optimization run")
        self.weekend_only_mode = False
        logging.info("🔄 Optimizer state reset for new optimization run")
        
        # Store reference to scheduler for mandatory shift checks
        self.scheduler = getattr(scheduler_core, 'scheduler', None)
        if not self.scheduler:
            logging.warning("Scheduler reference not found in scheduler_core")
        
        # Update constraint parameters from scheduler
        if hasattr(scheduler_core, 'scheduler') and hasattr(scheduler_core.scheduler, 'gap_between_shifts'):
            self.gap_between_shifts = scheduler_core.scheduler.gap_between_shifts
            logging.info(f"Updated gap_between_shifts from scheduler: {self.gap_between_shifts}")
        
        # Use the scheduler's tolerance validator
        if hasattr(scheduler_core, 'tolerance_validator'):
            validator = scheduler_core.tolerance_validator
            logging.info("Debug: Using scheduler's tolerance validator")
        else:
            logging.error("Scheduler core missing tolerance validator")
            return OptimizationResult(
                success=False,
                iteration=0,
                total_violations=float('inf'),
                general_violations=0,
                weekend_violations=0
            )
        
        current_schedule = copy.deepcopy(schedule)
        best_schedule = copy.deepcopy(schedule)
        best_violations = float('inf')
        
        # DEBUG: Verify max_iterations value before loop
        logging.info(f"🔍 DEBUG: About to start loop with range(1, {self.max_iterations} + 1) = range(1, {self.max_iterations + 1})")
        logging.info(f"🔍 DEBUG: This should generate iterations: {list(range(1, min(self.max_iterations + 1, 6)))[:5]}...")
        
        for iteration in range(1, self.max_iterations + 1):
            logging.info(f"🔄 Optimization iteration {iteration}/{self.max_iterations}")
            logging.info(f"   📊 State: stagnation={self.stagnation_counter}, best_violations={best_violations}")
            
            # Validate current schedule using existing methods
            validation_report = self._create_validation_report(validator, current_schedule)
            
            # Count violations
            general_violations = len(validation_report.get('general_shift_violations', []))
            weekend_violations = len(validation_report.get('weekend_shift_violations', []))
            total_violations = general_violations + weekend_violations
            
            # Calcular porcentaje de violaciones de fin de semana
            weekend_percentage = (weekend_violations / total_violations * 100) if total_violations > 0 else 0
            
            logging.info(f"   General violations: {general_violations}, Weekend violations: {weekend_violations}")
            if self.weekend_only_mode:
                logging.info(f"   🎯 WEEKEND-ONLY MODE ACTIVE - specialized optimization in progress")
            
            # Check if we've achieved optimal result
            if total_violations == 0:
                logging.info(f"✅ Optimal schedule achieved in iteration {iteration}!")
                return OptimizationResult(
                    success=True,
                    iteration=iteration,
                    total_violations=0,
                    general_violations=0,
                    weekend_violations=0,
                    schedule=current_schedule,
                    validation_report=validation_report
                )
            # Track best result so far
            if total_violations < best_violations:
                improvement_ratio = (best_violations - total_violations) / max(best_violations, 1)
                best_violations = total_violations
                best_schedule = copy.deepcopy(current_schedule)
                self.stagnation_counter = 0  # Reset stagnation counter
                self.no_change_counter = 0  # Reset no-change counter
                
                self.best_result = OptimizationResult(
                    success=False,
                    iteration=iteration,
                    total_violations=total_violations,
                    general_violations=general_violations,
                    weekend_violations=weekend_violations,
                    schedule=best_schedule,
                    validation_report=validation_report
                )
                logging.info(f"   📈 New best result: {total_violations} violations (improvement: {improvement_ratio:.2%})")
            elif total_violations == best_violations:
                # No change at all - increment no-change counter
                self.no_change_counter += 1
                self.stagnation_counter += 1
                logging.warning(f"   ⚠️  No change this iteration (no-change: {self.no_change_counter}/{self.max_no_change}, stagnation: {self.stagnation_counter}/{self.convergence_threshold})")
                
                if self.no_change_counter >= self.max_no_change:
                    logging.warning(f"   🛑 Stopping: {self.no_change_counter} iterations with NO changes - system cannot improve further with current constraints")
                    break
            else:
                self.stagnation_counter += 1
                self.no_change_counter = 0  # Worse but changed
                logging.info(f"   📊 No improvement this iteration (stagnation: {self.stagnation_counter}/{self.convergence_threshold})")
                logging.info(f"   📊 No improvement this iteration (stagnation: {self.stagnation_counter}/{self.convergence_threshold})")
                
                # Apply stagnation penalty for more aggressive optimization
                if self.stagnation_counter >= 2:
                    logging.info("   🎯 Applying stagnation penalty - increasing optimization intensity")
            
            # Detect weekend-only violations mode (ENHANCED - more flexible activation)
            # Activate if: (1) only weekend violations OR (2) ≥75% are weekend violations OR (3) weekend > general and ≥2
            activate_weekend_mode = (
                weekend_violations >= 2 and (
                    weekend_percentage >= 75.0 or  # ≥75% son de fin de semana
                    (weekend_violations > general_violations and weekend_violations >= 2) or
                    (weekend_violations >= 2 and general_violations <= 2)
                )
            )
            
            if activate_weekend_mode:
                if not self.weekend_only_mode:
                    logging.info(f"🎯 Activating WEEKEND-ONLY optimization mode "
                                f"({weekend_violations} weekend, {general_violations} general, "
                                f"{weekend_percentage:.1f}% are weekend violations)")
                    self.weekend_only_mode = True
                    # Reset stagnation counter for weekend-specific optimization
                    if self.stagnation_counter > 2:
                        self.stagnation_counter = 2  # Give it more chances
            else:
                if self.weekend_only_mode:
                    logging.info(f"🔄 Deactivating WEEKEND-ONLY mode (general: {general_violations}, weekend: {weekend_violations})")
                    self.weekend_only_mode = False
                # DEBUG: Log why weekend-only mode was NOT activated
                if weekend_violations > 0 and not self.weekend_only_mode:
                    logging.debug(f"   ℹ️  Weekend-only NOT active: weekend={weekend_violations}, general={general_violations} "
                                 f"({weekend_percentage:.1f}% weekend)")
            
            # Store optimization history
            self.optimization_history.append({
                'iteration': iteration,
                'total_violations': total_violations,
                'general_violations': general_violations,
                'weekend_violations': weekend_violations,
                'improvement_made': total_violations < best_violations,
                'weekend_only_mode': self.weekend_only_mode
            })
            
            # Enhanced convergence checks (more lenient for weekend-only mode)
            should_stop = self._should_stop_optimization(iteration, total_violations)
            logging.info(f"   🔍 Stop check: should_stop={should_stop}, violations={total_violations}")
            
            if should_stop:
                logging.info(f"🛑 Perfect schedule achieved - stopping optimization at iteration {iteration}")
                break
            
            # Apply optimization strategies
            try:
                # Calculate optimization intensity based on stagnation
                optimization_intensity = min(1.0, 0.3 + (self.stagnation_counter * 0.2))
                logging.info(f"   🎚️ Optimization intensity: {optimization_intensity:.2f}")
                
                current_schedule = self._apply_optimization_strategies(
                    current_schedule, validation_report, scheduler_core, 
                    workers_data, schedule_config, iteration, optimization_intensity
                )
                logging.info(f"   ✅ Optimization strategies applied, continuing to next iteration...")
            except Exception as e:
                logging.error(f"❌ Error in iteration {iteration}: {e}", exc_info=True)
                continue
            
            # DEBUG: Confirm we're about to loop back
            logging.info(f"🔁 DEBUG: End of iteration {iteration}, about to loop back to iteration {iteration + 1}")
        
        # DEBUG: Confirm loop has exited
        logging.info(f"🏁 DEBUG: Loop has exited after completing all iterations or breaking early")
        
        # Return best result found
        if self.best_result:
            logging.warning(f"⚠️  Optimization completed with {self.best_result.total_violations} violations remaining")
            self.best_result.success = (self.best_result.total_violations == 0)
            return self.best_result
        else:
            logging.error("❌ Optimization failed completely")
            return OptimizationResult(
                success=False,
                iteration=self.max_iterations,
                total_violations=total_violations,
                general_violations=general_violations,
                weekend_violations=weekend_violations,
                schedule=current_schedule,
                validation_report=validation_report
            )
    
    def _apply_optimization_strategies(self, schedule: Dict, validation_report: Dict, 
                                     scheduler_core, workers_data: List[Dict], 
                                     schedule_config: Dict, iteration: int, 
                                     intensity: float = 0.3) -> Dict:
        """
        Apply various optimization strategies to improve the schedule.
        
        Args:
            schedule: Current schedule
            validation_report: Validation report with violations
            scheduler_core: Scheduler core instance
            workers_data: Worker configuration
            schedule_config: Schedule configuration
            iteration: Current iteration number
            intensity: Optimization intensity (0.0 to 1.0)
            
        Returns:
            Improved schedule
        """
        logging.info(f"   🔧 Applying optimization strategies (intensity: {intensity:.2f})...")
        
        # Check for extreme deviations that should never occur
        general_violations = validation_report.get('general_shift_violations', [])
        extreme_deviations = [v for v in general_violations if abs(v.get('deviation_percentage', 0)) > 15]
        
        if extreme_deviations:
            logging.warning(f"   ⚠️ Found {len(extreme_deviations)} workers with EXTREME deviations (>15%):")
            for v in extreme_deviations:
                logging.warning(f"      Worker {v['worker']}: {v['deviation_percentage']:.1f}% deviation")
        
        optimized_schedule = copy.deepcopy(schedule)
        
        # WEEKEND-ONLY MODE: Apply aggressive weekend-specific strategies
        weekend_violations = validation_report.get('weekend_shift_violations', [])
        general_violations = validation_report.get('general_shift_violations', [])
        
        if self.weekend_only_mode and weekend_violations:
            logging.info(f"   🎯 WEEKEND-ONLY MODE: Applying focused weekend optimization")
            
            # Strategy 1A: Aggressive weekend redistribution (double passes)
            optimized_schedule = self._redistribute_weekend_shifts(
                optimized_schedule, weekend_violations, workers_data, schedule_config
            )
            # Second pass with higher intensity
            optimized_schedule = self._redistribute_weekend_shifts(
                optimized_schedule, weekend_violations, workers_data, schedule_config
            )
            
            # Strategy 1B: Direct weekend swaps between over/under assigned workers
            optimized_schedule = self._apply_weekend_swaps(
                optimized_schedule, validation_report, workers_data, schedule_config
            )
            
            # Strategy 1C: Third aggressive pass for persistent violations (NEW)
            if len(weekend_violations) >= 4 and self.stagnation_counter >= 2:
                logging.info(f"   🔥 AGGRESSIVE MODE: {len(weekend_violations)} persistent violations, stagnation: {self.stagnation_counter}")
                # Try swaps again with relaxed constraints
                optimized_schedule = self._apply_weekend_swaps(
                    optimized_schedule, validation_report, workers_data, schedule_config
                )
                # Try redistribution one more time
                optimized_schedule = self._redistribute_weekend_shifts(
                    optimized_schedule, weekend_violations, workers_data, schedule_config
                )
        else:
            # NORMAL MODE: Standard redistribution
            # Strategy 1: Redistribute weekend shifts FIRST (more specific constraints)
            if weekend_violations:
                optimized_schedule = self._redistribute_weekend_shifts(
                    optimized_schedule, weekend_violations, workers_data, schedule_config
                )
            
            # Strategy 2: Redistribute general shifts SECOND (broader adjustments)
            if general_violations:
                optimized_schedule = self._redistribute_general_shifts(
                    optimized_schedule, general_violations, workers_data, schedule_config
                )
        
        # CRITICAL: Validate balance after redistribution
        logging.info(f"   🔍 Validating balance after redistribution...")
        balance_result = self.balance_validator.validate_schedule_balance(optimized_schedule, workers_data)
        
        if not balance_result['is_balanced']:
            critical_count = len(balance_result['violations']['critical'])
            extreme_count = len(balance_result['violations']['extreme'])
            
            if extreme_count > 0 or critical_count > 3:
                logging.error(f"   🚨 Balance validation FAILED: {extreme_count} extreme, {critical_count} critical violations")
                logging.error(f"   ⚠️  Max deviation: {balance_result['stats']['max_deviation']:.1f}%")
                
                # Get rebalancing recommendations
                recommendations = self.balance_validator.get_rebalancing_recommendations(optimized_schedule, workers_data)
                if recommendations:
                    logging.info(f"   💡 Applying {len(recommendations[:3])} top rebalancing recommendations")
                    # Apply will be done in next iteration
        else:
            logging.info(f"   ✅ Balance validation PASSED")
        
        # Strategy 2.5: Fill empty slots using greedy algorithm (NEW)
        empty_slots_count = self._count_empty_slots(optimized_schedule)
        if empty_slots_count > 0:
            logging.info(f"   🕳️ Found {empty_slots_count} empty slots - applying greedy fill")
            optimized_schedule = self._greedy_fill_empty_slots(
                optimized_schedule, workers_data, schedule_config, scheduler_core
            )
        
        # Strategy 3: Apply random perturbations based on intensity - more aggressive for persistent violations
        total_violations = len(general_violations) + len(weekend_violations)
        if iteration > 1 and (total_violations > 8 or self.stagnation_counter > 0):  # Lower threshold and earlier activation
            # Scale perturbation intensity based on violation count and stagnation
            base_intensity = intensity * 0.8
            violation_multiplier = min(2.0, 1.0 + (total_violations / 10.0))  # More aggressive for more violations
            stagnation_multiplier = 1.0 + (self.stagnation_counter * 0.3)  # Increase with stagnation
            
            perturbation_intensity = min(base_intensity * violation_multiplier * stagnation_multiplier, 0.6)  # Higher max intensity
            
            logging.info(f"   🎲 Enhanced perturbations - violations: {total_violations}, stagnation: {self.stagnation_counter}, intensity: {perturbation_intensity:.3f}")
            
            optimized_schedule = self._apply_random_perturbations(
                optimized_schedule, workers_data, schedule_config, intensity=perturbation_intensity
            )
        
        # Strategy 4: Forced redistribution for high stagnation
        if self.stagnation_counter >= 2 and total_violations > 8:
            logging.info(f"   🚨 Applying forced redistribution due to high stagnation and violations")
            optimized_schedule = self._apply_forced_redistribution(
                optimized_schedule, general_violations + weekend_violations, workers_data, schedule_config
            )
        
        return optimized_schedule
    
    def _redistribute_general_shifts(self, schedule: Dict, violations: List[Dict], 
                                   workers_data: List[Dict], schedule_config: Dict) -> Dict:
        """Redistribute general shifts to fix tolerance violations with smart targeting."""
        logging.info(f"   📊 Redistributing general shifts for {len(violations)} workers")
        
        try:
            optimized_schedule = copy.deepcopy(schedule)
            
            # Debug: Log workers_data structure
            logging.info(f"Debug: workers_data type: {type(workers_data)}")
            logging.info(f"Debug: workers_data length: {len(workers_data)}")
            if workers_data:
                logging.info(f"Debug: First worker structure: {workers_data[0]}")
                logging.info(f"Debug: Available keys: {list(workers_data[0].keys()) if isinstance(workers_data[0], dict) else 'Not a dict'}")
            
            # Extract worker names safely
            worker_names = []
            for i, w in enumerate(workers_data):
                if isinstance(w, dict):
                    if 'id' in w:
                        # Handle both string and numeric IDs
                        worker_id = w['id']
                        if isinstance(worker_id, str) and worker_id.startswith('Worker'):
                            worker_names.append(worker_id)  # Already has "Worker" prefix
                        else:
                            worker_names.append(f"Worker {worker_id}")  # Add prefix for numeric
                    elif 'name' in w:
                        worker_names.append(w['name'])
                    else:
                        worker_names.append(f"Worker {i+1}")  # Fallback
                        logging.warning(f"Worker {i} missing id/name, using fallback")
                else:
                    worker_names.append(f"Worker {i+1}")  # Fallback for non-dict
                    logging.warning(f"Worker {i} is not a dict: {type(w)}")
            
            # Debug: Log the structures
            logging.info(f"Debug: Worker names extracted: {worker_names[:5]}...")  # First 5
            logging.info(f"Debug: Violations structure: {violations}")
            
            # Separate workers by violation type with priority scoring
            need_more_shifts = []
            have_excess_shifts = []
            
            for violation in violations:
                logging.info(f"Debug: Processing violation: {violation}")
                worker_name = violation['worker']
                deviation = violation['deviation_percentage']
                
                logging.info(f"Debug: worker_name='{worker_name}', deviation={deviation}")
                
                if deviation < -self.tolerance * 100:  # Worker needs more shifts
                    priority = abs(deviation)  # Higher absolute deviation = higher priority
                    need_more_shifts.append({
                        'worker': worker_name,
                        'shortage': abs(violation['shortage']),
                        'priority': priority,
                        'deviation': deviation,
                        'abs_deviation': abs(deviation)  # For easier sorting
                    })
                    logging.info(f"Debug: Added to need_more_shifts: {worker_name} (priority: {priority:.1f})")
                elif deviation > self.tolerance * 100:  # Worker has excess shifts
                    priority = abs(deviation)  # Higher absolute deviation = higher priority
                    have_excess_shifts.append({
                        'worker': worker_name,
                        'excess': violation['excess'],
                        'priority': priority,
                        'deviation': deviation,
                        'abs_deviation': abs(deviation)  # For easier sorting
                    })
                    logging.info(f"Debug: Added to have_excess_shifts: {worker_name} (priority: {priority:.1f})")
                    
        except Exception as e:
            logging.error(f"❌ Error in _redistribute_general_shifts: {e}", exc_info=True)
            return schedule  # Return original schedule on error
        
        # Sort by priority (most urgent first)
        need_more_shifts.sort(key=lambda x: x['priority'], reverse=True)
        have_excess_shifts.sort(key=lambda x: x['priority'], reverse=True)
        
        logging.info(f"   📊 Need more: {len(need_more_shifts)}, Have excess: {len(have_excess_shifts)}")
        
        # Debug: Log detailed violation info
        for need in need_more_shifts:
            logging.info(f"      🔴 {need['worker']} needs {need['shortage']} more shifts (deviation: {need['deviation']:.1f}%)")
        for excess in have_excess_shifts:
            logging.info(f"      🔵 {excess['worker']} has {excess['excess']} excess shifts (deviation: {excess['deviation']:.1f}%)")
        
        # BALANCED redistribution algorithm - focus on quality over quantity
        redistributions_made = 0
        successful_transfers = 0
        failed_attempts = 0
        
        # Set reasonable redistribution limits based on violations
        # CRITICAL: Match removals with assignments to maintain balance
        max_redistributions = min(100, len(violations) * 5)
        
        # Track balance metrics
        balance_tracker = {
            'shifts_removed': {},  # Track removals by worker
            'shifts_added': {}     # Track additions by worker
        }
        
        logging.info(f"   📊 Max redistributions allowed: {max_redistributions}")
        logging.info(f"   🎯 BALANCED MODE: Each removal must match with an assignment")
        
        for excess_info in have_excess_shifts:
            if redistributions_made >= max_redistributions:
                logging.info(f"   🛑 Max redistributions reached ({max_redistributions})")
                break
                
            excess_worker = excess_info['worker']
            logging.info(f"   🔄 Processing {excess_worker} (deviation: {excess_info['deviation']:.1f}%, excess: {excess_info['excess']})")
            
            # ULTRA AGGRESSIVE shift removal - scale with violation severity
            base_shifts = min(excess_info['excess'], 8)  # Increased from 4 to 8
            if excess_info['deviation'] > 20:  # Very high deviation
                shifts_to_remove = min(excess_info['excess'], 15)  # Increased from 6 to 15
            elif excess_info['deviation'] > 15:
                shifts_to_remove = min(excess_info['excess'], 12)  # Increased from 5 to 12
            elif excess_info['deviation'] > 10:
                shifts_to_remove = min(excess_info['excess'], 10)  # New tier
            else:
                shifts_to_remove = base_shifts
            
            logging.info(f"      📋 Will attempt to remove {shifts_to_remove} shifts from {excess_worker}")
            
            # Find shifts assigned to this worker, prioritize recent dates
            worker_shifts = []
            for date_key, assignments in optimized_schedule.items():
                # Handle different schedule formats
                if isinstance(assignments, dict):
                    # Format: {date: {'Morning': [workers], 'Afternoon': [workers]}}
                    for shift_type, workers in assignments.items():
                        if excess_worker in workers:
                            worker_shifts.append((date_key, shift_type, workers))
                elif isinstance(assignments, list):
                    # Format: {date: [worker1, worker2, worker3]} - positional
                    for post_idx, worker in enumerate(assignments):
                        if worker == excess_worker:
                            worker_shifts.append((date_key, f"Post_{post_idx}", assignments))
                else:
                    logging.warning(f"Unknown schedule format for {date_key}: {type(assignments)}")
                    continue
            
            # Sort by date (prefer redistributing from later dates)
            worker_shifts.sort(key=lambda x: x[0], reverse=True)
            
            shifts_removed = 0
            for date_key, shift_type, workers in worker_shifts:
                if shifts_removed >= shifts_to_remove:
                    break
                
                # CRITICAL: Skip mandatory shifts - they cannot be redistributed
                if self._is_mandatory_shift(excess_worker, date_key, workers_data):
                    logging.debug(f"      🔒 SKIPPING mandatory shift for {excess_worker} on {date_key} - cannot redistribute")
                    continue
                
                logging.debug(f"      📅 Trying to reassign {shift_type} on {date_key} from {excess_worker}")
                
                # Find best recipient for this shift
                best_recipient = None
                best_priority = 0
                candidates_checked = 0
                candidates_blocked = 0
                
                for need_info in need_more_shifts:
                    if need_info['shortage'] <= 0:
                        continue
                        
                    need_worker = need_info['worker']
                    candidates_checked += 1
                    
                    # Check if worker can take this shift
                    if need_worker not in workers:
                        if self._can_worker_take_shift(need_worker, date_key, shift_type, optimized_schedule, workers_data):
                            # CRITICAL: Validate that this transfer would improve balance
                            transfer_valid, reason = self.balance_validator.check_transfer_validity(
                                excess_worker, need_worker, optimized_schedule, workers_data
                            )
                            
                            if not transfer_valid:
                                candidates_blocked += 1
                                logging.debug(f"         ❌ {need_worker} blocked by balance check: {reason}")
                                continue
                            
                            # Calculate priority for this assignment
                            assignment_priority = need_info['priority']
                            
                            # Bonus for workers with severe shortages
                            if need_info['deviation'] < -15:
                                assignment_priority *= 1.5
                            
                            if assignment_priority > best_priority:
                                best_recipient = need_worker
                                best_priority = assignment_priority
                        else:
                            candidates_blocked += 1
                            logging.debug(f"         ❌ {need_worker} blocked by constraints for {shift_type} on {date_key}")
                
                logging.debug(f"      📊 Candidates: {candidates_checked} checked, {candidates_blocked} blocked, best: {best_recipient}")
                
                # Make the reassignment
                if best_recipient:
                    # CRITICAL: Calculate balance impact before making change
                    current_excess_deviation = excess_info['abs_deviation']
                    current_need_deviation = next(
                        (n['abs_deviation'] for n in need_more_shifts if n['worker'] == best_recipient),
                        0
                    )
                    
                    # Projected improvement: both workers move closer to target
                    projected_improvement = current_excess_deviation + current_need_deviation
                    
                    # Only proceed if this improves overall balance
                    if projected_improvement > 0.5:  # Minimum improvement threshold
                        # Handle both list and dict formats for reassignment
                        if isinstance(workers, list):
                            # Find and replace in the list
                            try:
                                idx = workers.index(excess_worker)
                                workers[idx] = best_recipient
                            except ValueError:
                                logging.warning(f"Worker {excess_worker} not found in list {workers}")
                                continue
                        else:
                            # Dictionary format (original logic)
                            workers.remove(excess_worker)
                            workers.append(best_recipient)
                        
                        # Update tracking
                        for need_info in need_more_shifts:
                            if need_info['worker'] == best_recipient:
                                need_info['shortage'] -= 1
                                break
                        
                        # Update balance tracker
                        balance_tracker['shifts_removed'][excess_worker] = balance_tracker['shifts_removed'].get(excess_worker, 0) + 1
                        balance_tracker['shifts_added'][best_recipient] = balance_tracker['shifts_added'].get(best_recipient, 0) + 1
                        
                        shifts_removed += 1
                        redistributions_made += 1
                        successful_transfers += 1
                        
                        # Format date for display
                        date_display = date_key.strftime('%Y-%m-%d') if isinstance(date_key, datetime) else str(date_key)
                        logging.info(f"      🔄 Moved {shift_type} from {excess_worker} to {best_recipient} on {date_display} (improvement: {projected_improvement:.1f})")
                    else:
                        failed_attempts += 1
                        logging.debug(f"      ⏭️ Skipped transfer - insufficient improvement ({projected_improvement:.1f})")
        
        # Report balance results
        logging.info(f"   ✅ General shift redistribution complete:")
        logging.info(f"      Successful transfers: {successful_transfers}")
        logging.info(f"      Failed attempts: {failed_attempts}")
        logging.info(f"      Total redistributions: {redistributions_made}")
        
        # Verify balance: removals should roughly equal additions
        total_removed = sum(balance_tracker['shifts_removed'].values())
        total_added = sum(balance_tracker['shifts_added'].values())
        balance_check = total_removed == total_added
        
        if balance_check:
            logging.info(f"      ✅ Balance verified: {total_removed} removed = {total_added} added")
        else:
            logging.warning(f"      ⚠️ Balance mismatch: {total_removed} removed ≠ {total_added} added")
        
        # Show top movers
        if balance_tracker['shifts_removed']:
            top_removed = sorted(balance_tracker['shifts_removed'].items(), key=lambda x: x[1], reverse=True)[:3]
            logging.info(f"      Top reductions: {', '.join([f'{w}: -{c}' for w, c in top_removed])}")
        if balance_tracker['shifts_added']:
            top_added = sorted(balance_tracker['shifts_added'].items(), key=lambda x: x[1], reverse=True)[:3]
            logging.info(f"      Top increases: {', '.join([f'{w}: +{c}' for w, c in top_added])}")
        
        return optimized_schedule
    
    def _can_worker_take_shift(self, worker_name: str, date_key, shift_type: str, 
                              schedule: Dict, workers_data: List[Dict]) -> bool:
        """
        Check if a worker can take a specific shift based on constraints.
        
        Args:
            worker_name: Name of the worker (e.g., "Worker 12")
            date_key: Date of the shift (datetime object or string)
            shift_type: Type of shift (Morning, Afternoon, Night, etc.)
            schedule: Current schedule
            workers_data: Worker configuration data
            
        Returns:
            bool: True if worker can take the shift
        """
        try:
            # Parse date from both datetime and string formats
            if isinstance(date_key, datetime):
                shift_date = date_key
            else:
                shift_date = datetime.strptime(date_key, "%Y-%m-%d")
            
            logging.debug(f"Checking {worker_name} for {shift_date} {shift_type}")
            
            # Extract worker ID from worker name - Enhanced logic
            worker_id = worker_name  # Start with the full name
            
            # Find worker data using flexible matching
            worker_data = None
            for w in workers_data:
                w_id = w.get('id', '')
                
                # Try exact match first
                if w_id == worker_name:
                    worker_data = w
                    break
                # Try string representation match
                elif str(w_id) == str(worker_name):
                    worker_data = w  
                    break
                # Try "Worker X" format matching - extract number from worker_name
                elif worker_name.startswith('Worker '):
                    # Extract number from "Worker 23" -> "23"
                    try:
                        worker_number = worker_name.split(' ')[1]
                        if str(w_id) == worker_number:
                            worker_data = w
                            break
                    except (IndexError, ValueError):
                        continue
                # Try reverse: if w_id is numeric and worker_name is "Worker X"
                elif str(w_id).isdigit() and worker_name == f"Worker {w_id}":
                    worker_data = w
                    break
            
            if not worker_data:
                logging.debug(f"Worker data not found for {worker_name}. Available IDs: {[w.get('id') for w in workers_data]}")
                return False
            
            logging.debug(f"Found worker data for {worker_name}: {worker_data.get('id')}")
            
            # Check basic availability
            worker_availability = worker_data.get('availability', {})
            day_name = shift_date.strftime('%A')
            logging.debug(f"Checking availability for {day_name}: {worker_availability.get(day_name, 'NOT_FOUND')}")
            
            if day_name in worker_availability:
                available_shifts = worker_availability[day_name]
                if available_shifts != 'ALL' and shift_type not in available_shifts:
                    logging.debug(f"Blocked by availability - {shift_type} not in {available_shifts}")
                    return False
            
            # Check if worker already has the SAME shift on this date (avoid duplicates)
            if date_key in schedule:
                assignments = schedule[date_key]
                if isinstance(assignments, dict):
                    # Dictionary format: check specific shift type
                    if shift_type in assignments and worker_name in assignments[shift_type]:
                        return False  # Worker already assigned to THIS specific shift
                elif isinstance(assignments, list):
                    # List format: more complex - need to determine position/shift mapping
                    # For now, be more permissive in list format during redistribution
                    pass  # Allow reassignments in list format
            
            # CRITICAL: Check 7/14 day pattern constraint
            # This is the key constraint that prevents same-weekday assignments 7 or 14 days apart
            worker_assignments = set()
            for date, assignments in schedule.items():
                if isinstance(assignments, dict):
                    for shift, workers in assignments.items():
                        if worker_name in workers:
                            if isinstance(date, datetime):
                                worker_assignments.add(date)
                            else:
                                try:
                                    worker_assignments.add(datetime.strptime(date, "%Y-%m-%d"))
                                except:
                                    continue
                elif isinstance(assignments, list):
                    if worker_name in assignments:
                        if isinstance(date, datetime):
                            worker_assignments.add(date)
                        else:
                            try:
                                worker_assignments.add(datetime.strptime(date, "%Y-%m-%d"))
                            except:
                                continue
            
            # Check 7/14 day pattern violations
            for assigned_date in worker_assignments:
                days_between = abs((shift_date - assigned_date).days)
                
                # CRITICAL CONSTRAINT: Prevent same weekday assignments 7 or 14 days apart
                # This only applies to weekdays (Mon-Thu), not weekends (Fri-Sun)
                if (days_between == 7 or days_between == 14) and shift_date.weekday() == assigned_date.weekday():
                    # Allow weekend days to be assigned on same weekday 7/14 days apart
                    if shift_date.weekday() >= 4 or assigned_date.weekday() >= 4:  # Fri, Sat, Sun
                        continue  # Skip constraint for weekend days
                    
                    logging.debug(f"❌ {worker_name} blocked: 7/14 day pattern violation - {shift_date.strftime('%A %Y-%m-%d')} vs {assigned_date.strftime('%A %Y-%m-%d')}")
                    return False
                
                # Check minimum gap constraint - VERY FLEXIBLE for redistribution
                gap_between_shifts = getattr(self, 'gap_between_shifts', 3)  # Default gap
                
                # SUPER FLEXIBLE GAP: For redistribution, allow much more flexibility
                # This gives maximum flexibility when redistributing to balance workload
                min_gap_redistribution = max(1, gap_between_shifts - 2)  # Even more flexible: gap - 2, minimum 1
                
                # Apply very flexible gap constraint 
                if 0 < days_between < min_gap_redistribution:
                    logging.debug(f"❌ {worker_name} blocked: Min redistribution gap violation - {days_between} days < {min_gap_redistribution} required")
                    return False
                elif min_gap_redistribution <= days_between < gap_between_shifts:
                    # In the super flexible zone - allow and log it
                    logging.debug(f"⚠️ {worker_name} super flexible gap: {days_between} days (below normal {gap_between_shifts} but allowed for redistribution)")
                    # Continue - allow this assignment
            
            # CRITICAL: Check tolerance limit (±12% absolute maximum during optimization)
            # This prevents swaps from violating tolerance limits
            
            # Count ACTUAL shifts (not just dates) - a worker can have multiple shifts per date
            current_shifts = 0
            for date, assignments in schedule.items():
                if isinstance(assignments, dict):
                    for shift, workers in assignments.items():
                        if worker_name in workers:
                            current_shifts += 1
                elif isinstance(assignments, list):
                    current_shifts += assignments.count(worker_name)
            
            target_shifts = worker_data.get('target_shifts', 0)
            work_percentage = worker_data.get('work_percentage', 100) / 100.0
            
            if target_shifts > 0:
                # Use Phase 2 tolerance (12%) during optimization
                # Part-time workers get adjusted tolerance (minimum 5%)
                base_tolerance = 0.12  # ±12% absolute maximum
                adjusted_tolerance = max(base_tolerance * work_percentage, 0.05)
                
                max_shifts = round(target_shifts * (1 + adjusted_tolerance))
                
                # Check if adding this shift would exceed the limit
                if current_shifts + 1 > max_shifts:
                    logging.debug(f"❌ {worker_name} blocked: Tolerance violation - "
                                f"would have {current_shifts + 1}/{target_shifts} shifts "
                                f"(max: {max_shifts}, tolerance: {adjusted_tolerance*100:.1f}%)")
                    return False
            
            # Check consecutive shift limits (basic check)
            # This is a simplified version - full implementation would check actual constraints
            return True
            
        except Exception as e:
            logging.error(f"Error checking if {worker_name} can take shift: {e}")
            return False
    
    def _is_mandatory_shift(self, worker_name: str, date_key, workers_data: List[Dict]) -> bool:
        """
        Check if a shift is mandatory for a given worker on a specific date.
        Mandatory shifts MUST NOT be removed, modified, or redistributed.
        
        Args:
            worker_name: Name of the worker (e.g., "Worker 12")
            date_key: Date of the shift (datetime object or string)
            workers_data: Worker configuration data
            
        Returns:
            bool: True if this is a mandatory shift (DO NOT TOUCH)
        """
        try:
            # Parse date from both datetime and string formats
            if isinstance(date_key, datetime):
                shift_date = date_key
            else:
                shift_date = datetime.strptime(date_key, "%Y-%m-%d")
            
            # Find worker data using flexible matching (same as _can_worker_take_shift)
            worker_data = None
            for w in workers_data:
                w_id = w.get('id', '')
                
                # Try exact match first
                if w_id == worker_name:
                    worker_data = w
                    break
                # Try string representation match
                elif str(w_id) == str(worker_name):
                    worker_data = w  
                    break
                # Try "Worker X" format matching
                elif worker_name.startswith('Worker '):
                    try:
                        worker_number = worker_name.split(' ')[1]
                        if str(w_id) == worker_number:
                            worker_data = w
                            break
                    except (IndexError, ValueError):
                        continue
                # Try reverse: if w_id is numeric and worker_name is "Worker X"
                elif str(w_id).isdigit() and worker_name == f"Worker {w_id}":
                    worker_data = w
                    break
            
            if not worker_data:
                return False
            
            # Check if this date is in the worker's mandatory_days
            mandatory_days_str = worker_data.get('mandatory_days', '')
            if not mandatory_days_str:
                return False
            
            # Use scheduler's method if available
            if self.scheduler and hasattr(self.scheduler, 'is_mandatory_shift'):
                # Extract numeric ID from worker_name if needed
                worker_id = worker_data.get('id', worker_name)
                return self.scheduler.is_mandatory_shift(worker_id, shift_date)
            
            # Fallback: parse mandatory_days manually
            try:
                # Split by semicolon and parse dates
                date_strings = [d.strip() for d in mandatory_days_str.split(';') if d.strip()]
                mandatory_dates = []
                for date_str in date_strings:
                    try:
                        # Try DD-MM-YYYY format
                        mandatory_date = datetime.strptime(date_str, '%d-%m-%Y')
                        mandatory_dates.append(mandatory_date)
                    except ValueError:
                        # Try other formats if needed
                        pass
                
                # Check if shift_date matches any mandatory date (compare just the date part)
                for mandatory_date in mandatory_dates:
                    if shift_date.date() == mandatory_date.date():
                        return True
                
                return False
                
            except Exception as e:
                logging.error(f"Error parsing mandatory_days for {worker_name}: {e}")
                return False
            
        except Exception as e:
            logging.error(f"Error checking if shift is mandatory for {worker_name}: {e}")
            return False
    
    def _redistribute_weekend_shifts(self, schedule: Dict, violations: List[Dict], 
                                   workers_data: List[Dict], schedule_config: Dict) -> Dict:
        """Redistribute weekend shifts to fix tolerance violations with enhanced targeting."""
        logging.info(f"   📅 Redistributing weekend shifts for {len(violations)} workers")
        
        optimized_schedule = copy.deepcopy(schedule)
        
        # Separate weekend violations with priority scoring
        need_more_weekends = []
        have_excess_weekends = []
        
        for violation in violations:
            worker_name = violation['worker']
            deviation = violation['deviation_percentage']
            
            if deviation < -self.tolerance * 100:
                priority = abs(deviation)  # Higher absolute deviation = higher priority
                need_more_weekends.append({
                    'worker': worker_name,
                    'shortage': abs(violation['shortage']),
                    'priority': priority,
                    'deviation': deviation,
                    'abs_deviation': abs(deviation)  # For easier sorting
                })
            elif deviation > self.tolerance * 100:
                priority = abs(deviation)  # Higher absolute deviation = higher priority
                have_excess_weekends.append({
                    'worker': worker_name,
                    'excess': violation['excess'],
                    'priority': priority,
                    'deviation': deviation,
                    'abs_deviation': abs(deviation)  # For easier sorting
                })
        
        # Sort by priority
        need_more_weekends.sort(key=lambda x: x['priority'], reverse=True)
        have_excess_weekends.sort(key=lambda x: x['priority'], reverse=True)
        
        # Debug: Log detailed weekend violation info
        logging.info(f"   📅 Weekend need more: {len(need_more_weekends)}, Have excess: {len(have_excess_weekends)}")
        for need in need_more_weekends:
            logging.info(f"      🔴 {need['worker']} needs {need['shortage']} more weekends (deviation: {need['deviation']:.1f}%)")
        for excess in have_excess_weekends:
            logging.info(f"      🔵 {excess['worker']} has {excess['excess']} excess weekends (deviation: {excess['deviation']:.1f}%)")
        
        # Get all weekend dates
        weekend_dates = []
        for date_key in optimized_schedule.keys():
            try:
                # Handle both datetime objects and string dates
                if isinstance(date_key, datetime):
                    date_obj = date_key
                    date_str = date_key.strftime("%Y-%m-%d")
                else:
                    date_obj = datetime.strptime(date_key, "%Y-%m-%d")
                    date_str = date_key
                
                if date_obj.weekday() in [5, 6]:  # Saturday or Sunday
                    weekend_dates.append(date_key)  # Use original key format
            except:
                continue
        
        logging.info(f"   📅 Processing {len(weekend_dates)} weekend dates")
        
        redistributions_made = 0
        # Enhanced weekend redistribution limits
        base_redistributions = len(violations) * 3  # Increased base multiplier
        max_redistributions = min(25, base_redistributions)  # Increased from 15
        
        # Extra aggressiveness for high weekend violation counts
        if len(violations) > 6:
            max_redistributions = min(35, len(violations) * 4)
        
        logging.info(f"   📅 Max weekend redistributions allowed: {max_redistributions}")
        
        # Smart weekend redistribution - more aggressive targeting
        for excess_info in have_excess_weekends:
            if redistributions_made >= max_redistributions:
                break
                
            excess_worker = excess_info['worker']
            
            # Find weekend shifts for this worker
            weekend_shifts = []
            for date_key in weekend_dates:
                if date_key in optimized_schedule:
                    assignments = optimized_schedule[date_key]
                    # Handle different schedule formats
                    if isinstance(assignments, dict):
                        # Format: {date: {'Morning': [workers], 'Afternoon': [workers]}}
                        for shift_type, workers in assignments.items():
                            if excess_worker in workers:
                                weekend_shifts.append((date_key, shift_type, workers))
                    elif isinstance(assignments, list):
                        # Format: {date: [worker1, worker2, worker3]} - positional
                        for post_idx, worker in enumerate(assignments):
                            if worker == excess_worker:
                                weekend_shifts.append((date_key, f"Post_{post_idx}", assignments))
                    else:
                        logging.warning(f"Unknown weekend schedule format for {date_key}: {type(assignments)}")
                        continue
            
            # Redistribute weekend shifts - more aggressive based on deviation
            if excess_info['deviation'] > 25:  # Very high weekend deviation
                shifts_to_redistribute = min(len(weekend_shifts), excess_info['excess'], 4)  # Up to 4
            elif excess_info['deviation'] > 20:
                shifts_to_redistribute = min(len(weekend_shifts), excess_info['excess'], 3)  # Up to 3
            else:
                shifts_to_redistribute = min(len(weekend_shifts), excess_info['excess'], 2)  # Standard 2
            
            for i, (date_key, shift_type, workers) in enumerate(weekend_shifts):
                if i >= shifts_to_redistribute or redistributions_made >= max_redistributions:
                    break
                
                # CRITICAL: Skip mandatory shifts - they cannot be redistributed
                if self._is_mandatory_shift(excess_worker, date_key, workers_data):
                    logging.debug(f"      🔒 SKIPPING mandatory weekend shift for {excess_worker} on {date_key} - cannot redistribute")
                    continue
                
                # Find best weekend recipient
                best_recipient = None
                best_priority = 0
                
                for need_info in need_more_weekends:
                    if need_info['shortage'] <= 0:
                        continue
                        
                    need_worker = need_info['worker']
                    
                    # Check if worker can take this weekend shift
                    if need_worker not in workers and self._can_worker_take_shift(
                        need_worker, date_key, shift_type, optimized_schedule, workers_data
                    ):
                        # Calculate assignment priority
                        assignment_priority = need_info['priority']
                        
                        # Bonus for severe weekend shortages
                        if need_info['deviation'] < -25:
                            assignment_priority *= 2.0
                        
                        # Bonus for balanced weekend distribution
                        if isinstance(date_key, datetime):
                            weekend_day = date_key.weekday()
                        else:
                            weekend_day = datetime.strptime(date_key, "%Y-%m-%d").weekday()
                        
                        if weekend_day == 5:  # Saturday
                            assignment_priority *= 1.1
                        
                        if assignment_priority > best_priority:
                            best_recipient = need_worker
                            best_priority = assignment_priority
                
                # Make the weekend reassignment
                if best_recipient:
                    # Handle both list and dict formats for reassignment
                    if isinstance(workers, list):
                        # Find and replace in the list
                        try:
                            idx = workers.index(excess_worker)
                            workers[idx] = best_recipient
                        except ValueError:
                            logging.warning(f"Weekend worker {excess_worker} not found in list {workers}")
                            continue
                    else:
                        # Dictionary format (original logic)
                        workers.remove(excess_worker)
                        workers.append(best_recipient)
                    
                    # Update tracking
                    for need_info in need_more_weekends:
                        if need_info['worker'] == best_recipient:
                            need_info['shortage'] -= 1
                            break
                    
                    redistributions_made += 1
                    if isinstance(date_key, datetime):
                        day_name = date_key.strftime('%A')
                        date_display = date_key.strftime('%Y-%m-%d')
                    else:
                        day_name = datetime.strptime(date_key, "%Y-%m-%d").strftime('%A')
                        date_display = date_key
                    
                    logging.info(f"      🔄 Weekend: Moved {shift_type} from {excess_worker} to {best_recipient} on {day_name} {date_display}")
        
        logging.info(f"   ✅ Made {redistributions_made} weekend shift redistributions")
        return optimized_schedule
    
    def _apply_weekend_swaps(self, schedule: Dict, validation_report: Dict,
                           workers_data: List[Dict], schedule_config: Dict) -> Dict:
        """Apply direct weekend shift swaps between over-assigned and under-assigned workers."""
        logging.info(f"   🔄 Applying weekend shift swaps for targeted balancing")
        
        optimized_schedule = copy.deepcopy(schedule)
        
        # Extract weekend violations from validation report (try both keys for compatibility)
        weekend_violations = validation_report.get('weekend_shift_violations', [])
        if not weekend_violations:
            weekend_violations = validation_report.get('weekend_violations', [])
        
        if not weekend_violations:
            logging.info(f"   ℹ️ No weekend violations to swap")
            return optimized_schedule
        
        logging.info(f"   📊 Found {len(weekend_violations)} weekend violations to process")
        
        # Separate over and under assigned workers
        over_assigned = []
        under_assigned = []
        
        for violation in weekend_violations:
            worker_name = violation['worker']
            deviation = violation['deviation_percentage']
            
            if deviation > self.tolerance * 100:  # Over-assigned (e.g., +13.3%)
                over_assigned.append({
                    'worker': worker_name,
                    'deviation': deviation,
                    'excess': violation.get('excess', 0)
                })
            elif deviation < -self.tolerance * 100:  # Under-assigned (e.g., -25%, -16.7%)
                under_assigned.append({
                    'worker': worker_name,
                    'deviation': deviation,
                    'shortage': abs(violation.get('shortage', 0))
                })
        
        # Sort by severity (absolute deviation)
        over_assigned.sort(key=lambda x: abs(x['deviation']), reverse=True)
        under_assigned.sort(key=lambda x: abs(x['deviation']), reverse=True)
        
        logging.info(f"   📊 Over-assigned: {len(over_assigned)}, Under-assigned: {len(under_assigned)}")
        for over in over_assigned:
            logging.info(f"      🔵 {over['worker']}: +{over['deviation']:.1f}% ({over['excess']} excess)")
        for under in under_assigned:
            logging.info(f"      🔴 {under['worker']}: {under['deviation']:.1f}% ({under['shortage']} shortage)")
        
        # Get all weekend dates
        weekend_dates = []
        for date_key in optimized_schedule.keys():
            try:
                if isinstance(date_key, datetime):
                    date_obj = date_key
                else:
                    date_obj = datetime.strptime(date_key, "%Y-%m-%d")
                
                if date_obj.weekday() in [5, 6]:  # Saturday or Sunday
                    weekend_dates.append(date_key)
            except:
                continue
        
        logging.info(f"   📅 Processing {len(weekend_dates)} weekend dates for swaps")
        
        swaps_made = 0
        max_swaps = min(20, len(weekend_violations) * 2)  # Allow multiple swaps per worker
        
        # Perform direct swaps between over and under assigned pairs
        for over_info in over_assigned:
            if swaps_made >= max_swaps:
                break
            
            over_worker = over_info['worker']
            
            # Find all weekend shifts for over-assigned worker
            over_weekend_shifts = []
            for date_key in weekend_dates:
                if date_key in optimized_schedule:
                    assignments = optimized_schedule[date_key]
                    
                    if isinstance(assignments, dict):
                        for shift_type, workers in assignments.items():
                            if over_worker in workers:
                                over_weekend_shifts.append({
                                    'date': date_key,
                                    'shift_type': shift_type,
                                    'workers_list': workers
                                })
                    elif isinstance(assignments, list):
                        for post_idx, worker in enumerate(assignments):
                            if worker == over_worker:
                                over_weekend_shifts.append({
                                    'date': date_key,
                                    'shift_type': f"Post_{post_idx}",
                                    'workers_list': assignments,
                                    'post_idx': post_idx
                                })
            
            # Try to swap with under-assigned workers
            attempts = 0
            rejections = {'already_assigned': 0, 'constraint_failed': 0, 'no_shifts_found': 0}
            
            for under_info in under_assigned:
                if under_info['shortage'] <= 0 or swaps_made >= max_swaps:
                    continue
                
                under_worker = under_info['worker']
                
                if not over_weekend_shifts:
                    rejections['no_shifts_found'] += 1
                    logging.debug(f"      ⚠️ No weekend shifts found for over-assigned workers to swap")
                    continue
                
                # Find potential swap opportunities on same dates
                for over_shift in over_weekend_shifts:
                    attempts += 1
                    date_key = over_shift['date']
                    shift_type = over_shift['shift_type']
                    workers_list = over_shift['workers_list']
                    
                    # CRITICAL: Skip mandatory shifts - they cannot be swapped
                    if self._is_mandatory_shift(over_worker, date_key, workers_data):
                        logging.debug(f"      🔒 SKIPPING mandatory shift for {over_worker} on {date_key} - cannot swap")
                        continue
                    
                    # Check if under-assigned worker is already on this shift
                    if under_worker in workers_list:
                        rejections['already_assigned'] += 1
                        continue
                    
                    # Check if under-assigned worker can take this shift
                    if self._can_worker_take_shift(
                        under_worker, date_key, shift_type, optimized_schedule, workers_data
                    ):
                        # Perform the swap
                        if isinstance(workers_list, list):
                            if 'post_idx' in over_shift:
                                # Direct index replacement for list format
                                workers_list[over_shift['post_idx']] = under_worker
                            else:
                                # Find and replace
                                try:
                                    idx = workers_list.index(over_worker)
                                    workers_list[idx] = under_worker
                                except ValueError:
                                    continue
                        else:
                            # Dict format
                            workers_list.remove(over_worker)
                            workers_list.append(under_worker)
                        
                        # Update shortage tracking
                        under_info['shortage'] -= 1
                        over_info['excess'] -= 1
                        swaps_made += 1
                        
                        if isinstance(date_key, datetime):
                            date_display = date_key.strftime('%Y-%m-%d (%A)')
                        else:
                            date_display = f"{date_key} ({datetime.strptime(date_key, '%Y-%m-%d').strftime('%A')})"
                        
                        logging.info(f"      🔄 SWAP: {over_worker} → {under_worker} on {date_display} {shift_type}")
                        
                        # Only do one swap per shift to avoid over-correction
                        break
                    else:
                        rejections['constraint_failed'] += 1
                
                if swaps_made >= max_swaps:
                    break
        
        # Enhanced logging for diagnostics
        if swaps_made == 0 and attempts > 0:
            logging.warning(f"   ⚠️ SWAP DIAGNOSIS: {attempts} attempts, 0 successful")
            logging.warning(f"      - Already assigned: {rejections['already_assigned']}")
            logging.warning(f"      - Constraint failed: {rejections['constraint_failed']}")
            logging.warning(f"      - No shifts found: {rejections['no_shifts_found']}")
            logging.warning(f"      💡 TIP: Constraints may be too strict (gap_between_shifts, 7/14 rule)")
        
        logging.info(f"   ✅ Made {swaps_made} weekend shift swaps (attempts: {attempts})")
        return optimized_schedule
    
    def _apply_random_perturbations(self, schedule: Dict, workers_data: List[Dict], 
                                  schedule_config: Dict, intensity: float = 0.1) -> Dict:
        """Apply random perturbations to escape local optima."""
        logging.info(f"   🎲 Applying random perturbations (intensity: {intensity:.2f})")
        
        optimized_schedule = copy.deepcopy(schedule)
        
        # Extract worker names safely
        worker_names = []
        for i, w in enumerate(workers_data):
            if isinstance(w, dict):
                if 'id' in w:
                    # Handle both string and numeric IDs
                    worker_id = w['id']
                    if isinstance(worker_id, str) and worker_id.startswith('Worker'):
                        worker_names.append(worker_id)  # Already has "Worker" prefix
                    else:
                        worker_names.append(f"Worker {worker_id}")  # Add prefix for numeric
                elif 'name' in w:
                    worker_names.append(w['name'])
                else:
                    worker_names.append(f"Worker {i+1}")  # Fallback
                    logging.warning(f"Worker {i} missing id/name in random perturbations, using fallback")
            else:
                worker_names.append(f"Worker {i+1}")  # Fallback for non-dict
                logging.warning(f"Worker {i} is not a dict in random perturbations: {type(w)}")
        
        logging.info(f"Debug: Extracted {len(worker_names)} worker names for random perturbations")
        
        # Calculate number of swaps based on intensity - handle different schedule formats
        total_assignments = 0
        for assignments in schedule.values():
            if isinstance(assignments, dict):
                # Format: {date: {'Morning': [workers], 'Afternoon': [workers]}}
                for workers in assignments.values():
                    if isinstance(workers, list):
                        total_assignments += len(workers)
            elif isinstance(assignments, list):
                # Format: {date: [worker1, worker2, worker3]} - positional
                total_assignments += len(assignments)
        
        num_swaps = int(total_assignments * intensity)
        logging.info(f"   🎲 Total assignments: {total_assignments}, planned swaps: {num_swaps}")
        
        for swap_attempt in range(num_swaps):
            # Pick random date and shift
            dates = list(optimized_schedule.keys())
            random_date = random.choice(dates)
            
            if not optimized_schedule[random_date]:
                continue
            
            assignments = optimized_schedule[random_date]
            
            # Handle different schedule formats
            if isinstance(assignments, dict):
                # Dictionary format: {'Morning': [workers], 'Afternoon': [workers]}
                shift_types = list(assignments.keys())
                if not shift_types:
                    continue
                random_shift = random.choice(shift_types)
                current_workers = assignments[random_shift]
            elif isinstance(assignments, list):
                # List format: [worker1, worker2, worker3] - positional
                if not assignments:
                    continue
                random_shift = f"Post_{random.randint(0, len(assignments)-1)}"
                current_workers = assignments
            else:
                logging.warning(f"Unknown schedule format for {random_date}: {type(assignments)}")
                continue
            
            # Perform the swap if there are workers available
            if isinstance(current_workers, list) and len(current_workers) > 0:
                # Replace random worker with another random worker
                old_worker = random.choice(current_workers)
                
                # CRITICAL: Skip mandatory shifts - they cannot be perturbed
                if self._is_mandatory_shift(old_worker, random_date, workers_data):
                    logging.debug(f"      🔒 SKIPPING mandatory shift for {old_worker} on {random_date} - cannot perturb")
                    continue
                
                new_worker = random.choice(worker_names)
                
                if new_worker not in current_workers:
                    # CRITICAL: Validate that swap doesn't violate tolerance BEFORE making it
                    # Check if new_worker can take this shift (includes tolerance validation)
                    if not self._can_worker_take_shift(
                        new_worker, random_date, random_shift, optimized_schedule, workers_data
                    ):
                        logging.debug(f"   ❌ Random swap blocked: {new_worker} cannot take shift on {random_date} (tolerance/constraint violation)")
                        continue
                    
                    # Handle different assignment formats
                    if isinstance(assignments, dict):
                        # Dictionary format: modify the specific shift list
                        current_workers.remove(old_worker)
                        current_workers.append(new_worker)
                    elif isinstance(assignments, list):
                        # List format: modify the main assignments list
                        try:
                            idx = assignments.index(old_worker)
                            assignments[idx] = new_worker
                        except ValueError:
                            # Worker not found, skip this swap
                            continue
                    
                    logging.debug(f"   🔄 Random swap: {old_worker} → {new_worker} on {random_date}")
        
        return optimized_schedule
    
    def _apply_forced_redistribution(self, schedule: Dict, violations: List[Dict], 
                                   workers_data: List[Dict], schedule_config: Dict) -> Dict:
        """
        Apply ULTRA AGGRESSIVE forced redistribution when normal strategies fail.
        This method bypasses some constraints to force progress on violations.
        
        CRITICAL: With high violation counts (>20), we MUST be extremely aggressive
        and override pattern constraints that are blocking redistribution.
        """
        logging.info(f"   🚨 Forced redistribution for {len(violations)} violations")
        
        optimized_schedule = copy.deepcopy(schedule)
        
        # Extract worker names safely (reuse existing logic)
        worker_names = []
        for i, w in enumerate(workers_data):
            if isinstance(w, dict):
                if 'id' in w:
                    worker_id = w['id']
                    if isinstance(worker_id, str) and worker_id.startswith('Worker'):
                        worker_names.append(worker_id)
                    else:
                        worker_names.append(f"Worker {worker_id}")
                elif 'name' in w:
                    worker_names.append(w['name'])
                else:
                    worker_names.append(f"Worker {i+1}")
            else:
                worker_names.append(f"Worker {i+1}")
        
        # Group violations by type
        general_violations = [v for v in violations if 'weekend' not in v.get('type', '')]
        weekend_violations = [v for v in violations if 'weekend' in v.get('type', '')]
        
        forced_changes = 0
        
        # ULTRA AGGRESSIVE forced redistribution limit
        # Scale with violation count - we NEED to fix these violations
        max_forced = len(violations) * 10  # 10 redistributions per violation
        if len(violations) > 20:
            max_forced = len(violations) * 15  # Even more aggressive for high counts
            logging.warning(f"⚠️ EXTREME FORCED MODE: {len(violations)} violations, allowing {max_forced} forced redistributions")
        else:
            logging.info(f"   📊 Allowing up to {max_forced} forced redistributions")
        
        # Force general shift redistributions - WITH constraint checking
        for violation in general_violations:
            if forced_changes >= max_forced:
                break
                
            worker = violation['worker']
            deviation = violation.get('deviation_percentage', 0)
            
            if abs(deviation) > 8:  # Reduced from 15 - ANY violation beyond tolerance needs fixing
                logging.info(f"      🚨 FORCING redistribution for {worker} (deviation: {deviation:.1f}%)")
                
                # Find any shift assigned to this worker and try to reassign it
                # TRY MULTIPLE SHIFTS - don't give up after first attempt
                shifts_to_try = []
                for date_key_scan, assignments_scan in optimized_schedule.items():
                    if isinstance(assignments_scan, dict):
                        for shift_type_scan, workers_scan in assignments_scan.items():
                            if worker in workers_scan:
                                # Skip mandatory shifts
                                if not self._is_mandatory_shift(worker, date_key_scan, workers_data):
                                    shifts_to_try.append((date_key_scan, shift_type_scan, 'dict'))
                    elif isinstance(assignments_scan, list):
                        if worker in assignments_scan:
                            # Skip mandatory shifts
                            if not self._is_mandatory_shift(worker, date_key_scan, workers_data):
                                shifts_to_try.append((date_key_scan, None, 'list'))
                
                # Shuffle to avoid always trying the same dates
                import random
                random.shuffle(shifts_to_try)
                
                # Try to redistribute ANY of the shifts
                redistributed_count = 0
                max_per_worker = 5  # Try to redistribute up to 5 shifts per worker with high deviation
                
                for shift_info in shifts_to_try:
                    if redistributed_count >= max_per_worker or forced_changes >= max_forced:
                        break
                    
                    date_key_try, shift_type_try, format_type = shift_info
                    
                    if format_type == 'dict':
                        assignments = optimized_schedule[date_key_try]
                        workers = assignments[shift_type_try]
                        
                        # Try to find valid alternative workers - PRIORITIZE those with DEFICIT
                        valid_alternatives_with_priority = []
                        for candidate in worker_names:
                            if candidate != worker:
                                # Strict constraint check - respect 7/14 pattern
                                if self._can_worker_take_shift(candidate, date_key_try, shift_type_try, optimized_schedule, workers_data):
                                    # Calculate candidate's current deviation to prioritize those with deficit
                                    candidate_data = next((w for w in workers_data if w.get('id') == candidate or f"Worker {w.get('id')}" == candidate), None)
                                    if candidate_data:
                                        target = candidate_data.get('target_shifts', 0)
                                        current = self._count_worker_shifts(candidate, optimized_schedule)
                                        deficit = target - current  # Positive if under target
                                        valid_alternatives_with_priority.append((candidate, deficit))
                        
                        if valid_alternatives_with_priority:
                            # Sort by deficit (descending) - workers with largest deficit first
                            valid_alternatives_with_priority.sort(key=lambda x: x[1], reverse=True)
                            alternative_worker = valid_alternatives_with_priority[0][0]
                            deficit_amount = valid_alternatives_with_priority[0][1]
                            
                            workers.remove(worker)
                            workers.append(alternative_worker)
                            forced_changes += 1
                            redistributed_count += 1
                            logging.info(f"      ✅ FORCED: {shift_type_try} from {worker} to {alternative_worker} (deficit: {deficit_amount}) on {date_key_try}")
                    
                    elif format_type == 'list':
                        assignments = optimized_schedule[date_key_try]
                        
                        # Try constraint-aware replacement - PRIORITIZE workers with DEFICIT
                        valid_alternatives_with_priority = []
                        for candidate in worker_names:
                            if candidate != worker:
                                if self._can_worker_take_shift(candidate, date_key_try, "Position", optimized_schedule, workers_data):
                                    # Calculate candidate's current deviation
                                    candidate_data = next((w for w in workers_data if w.get('id') == candidate or f"Worker {w.get('id')}" == candidate), None)
                                    if candidate_data:
                                        target = candidate_data.get('target_shifts', 0)
                                        current = self._count_worker_shifts(candidate, optimized_schedule)
                                        deficit = target - current
                                        valid_alternatives_with_priority.append((candidate, deficit))
                        
                        if valid_alternatives_with_priority:
                            # Sort by deficit (descending) - prioritize workers furthest below target
                            valid_alternatives_with_priority.sort(key=lambda x: x[1], reverse=True)
                            alternative_worker = valid_alternatives_with_priority[0][0]
                            deficit_amount = valid_alternatives_with_priority[0][1]
                            
                            idx = assignments.index(worker)
                            assignments[idx] = alternative_worker
                            forced_changes += 1
                            redistributed_count += 1
                            logging.info(f"      ✅ FORCED: Position {idx} from {worker} to {alternative_worker} (deficit: {deficit_amount}) on {date_key_try}")
                
                if redistributed_count > 0:
                    logging.info(f"      ✅ Successfully redistributed {redistributed_count} shifts from {worker}")
                else:
                    logging.warning(f"      ⚠️ Could not redistribute any shifts from {worker} - all blocked by constraints")
        
        logging.info(f"   ✅ Made {forced_changes} forced redistributions")
        return optimized_schedule
    
    def get_optimization_summary(self) -> Dict:
        """Get summary of optimization process."""
        if not self.optimization_history:
            return {"message": "No optimization history available"}
        
        return {
            "total_iterations": len(self.optimization_history),
            "initial_violations": self.optimization_history[0]['total_violations'],
            "final_violations": self.optimization_history[-1]['total_violations'],
            "best_violations": min(h['total_violations'] for h in self.optimization_history),
            "improvement": self.optimization_history[0]['total_violations'] - self.optimization_history[-1]['total_violations'],
            "convergence_achieved": self.optimization_history[-1]['total_violations'] == 0,
            "stagnation_counter": self.stagnation_counter,
            "average_improvement_rate": self._calculate_average_improvement(),
            "history": self.optimization_history
        }
    
    def _should_stop_optimization(self, iteration: int, current_violations: int) -> bool:
        """
        Determine if optimization should stop based on convergence criteria.
        
        MODIFIED: Only stop if violations reach 0 - always complete all iterations otherwise
        
        Args:
            iteration: Current iteration number
            current_violations: Current number of violations
            
        Returns:
            bool: True if optimization should stop
        """
        # ONLY stop if we reach 0 violations (perfect schedule)
        if current_violations == 0:
            logging.info(f"   ✅ Perfect schedule achieved - stopping optimization")
            return True
        
        # Otherwise, ALWAYS continue - let all iterations run
        # This ensures maximum optimization effort for difficult schedules
        logging.debug(f"   ⏩ Continuing optimization ({current_violations} violations remaining)")
        return False
    
    def _calculate_average_improvement(self) -> float:
        """Calculate average improvement rate over recent iterations."""
        if len(self.optimization_history) < 2:
            return 0.0
        
        initial_violations = self.optimization_history[0]['total_violations']
        current_violations = self.optimization_history[-1]['total_violations']
        iterations = len(self.optimization_history)
        
        return max(0, (initial_violations - current_violations) / iterations)
    
    def _count_worker_shifts(self, worker_name: str, schedule: Dict) -> int:
        """
        Count total shifts assigned to a worker in the schedule.
        
        Args:
            worker_name: Name of the worker (e.g., "Worker 12" or "12")
            schedule: Schedule dictionary
            
        Returns:
            int: Number of shifts assigned to this worker
        """
        count = 0
        try:
            for date_key, assignments in schedule.items():
                if isinstance(assignments, dict):
                    # Dictionary format: {date: {'Morning': [workers], 'Afternoon': [workers]}}
                    for shift_type, workers in assignments.items():
                        if worker_name in workers:
                            count += 1
                elif isinstance(assignments, list):
                    # List format: {date: [worker1, worker2, worker3]}
                    count += assignments.count(worker_name)
        except Exception as e:
            logging.error(f"Error counting shifts for {worker_name}: {e}")
        
        return count
    
    def _create_validation_report(self, validator, current_schedule: Dict) -> Dict:
        """
        Create a validation report using the existing validator methods.
        
        Args:
            validator: ShiftToleranceValidator instance
            current_schedule: Current schedule to validate
            
        Returns:
            Dict with validation report in expected format
        """
        try:
            # Update validator's schedule reference
            original_schedule = validator.schedule
            validator.schedule = current_schedule
            
            logging.info("Debug: Creating validation report...")
            
            # Get violations using existing methods
            general_violations = []
            weekend_violations = []
            
            # Check all workers for general violations
            general_outside = validator.get_workers_outside_tolerance(is_weekend_only=False)
            logging.info(f"Debug: Found {len(general_outside)} workers outside general tolerance")
            
            for worker_info in general_outside:
                worker_id = worker_info.get('worker_id', 'Unknown')
                worker_name = f"Worker {worker_id}" if str(worker_id).isdigit() else str(worker_id)
                
                # Calculate difference (assigned - target)
                assigned = worker_info.get('assigned_shifts', 0)
                target = worker_info.get('target_shifts', 0)
                difference = assigned - target
                
                general_violations.append({
                    'worker': worker_name,
                    'deviation_percentage': worker_info.get('deviation_percentage', 0),
                    'shortage': max(0, -difference),  # When assigned < target
                    'excess': max(0, difference)      # When assigned > target
                })
            
            # Check all workers for weekend violations  
            weekend_outside = validator.get_workers_outside_tolerance(is_weekend_only=True)
            logging.info(f"Debug: Found {len(weekend_outside)} workers outside weekend tolerance")
            
            for worker_info in weekend_outside:
                worker_id = worker_info.get('worker_id', 'Unknown')
                worker_name = f"Worker {worker_id}" if str(worker_id).isdigit() else str(worker_id)
                
                # Calculate difference (assigned - target)
                assigned = worker_info.get('assigned_shifts', 0)
                target = worker_info.get('target_shifts', 0)
                difference = assigned - target
                
                weekend_violations.append({
                    'worker': worker_name,
                    'deviation_percentage': worker_info.get('deviation_percentage', 0),
                    'shortage': max(0, -difference),  # When assigned < target
                    'excess': max(0, difference)      # When assigned > target
                })
            
            # Restore original schedule
            validator.schedule = original_schedule
            
            report = {
                'general_shift_violations': general_violations,
                'weekend_shift_violations': weekend_violations,
                'total_violations': len(general_violations) + len(weekend_violations)
            }
            
            logging.info(f"Debug: Created validation report with {report['total_violations']} total violations")
            return report
            
        except Exception as e:
            logging.error(f"Error creating validation report: {e}")
            return {
                'general_shift_violations': [],
                'weekend_shift_violations': [],
                'total_violations': 0
            }    
    def _count_empty_slots(self, schedule: Dict) -> int:
        """Count the number of empty slots in the schedule."""
        empty_count = 0
        try:
            for date, assignments in schedule.items():
                if isinstance(assignments, list):
                    empty_count += sum(1 for worker in assignments if worker is None)
                elif isinstance(assignments, dict):
                    for shift_workers in assignments.values():
                        if isinstance(shift_workers, list):
                            empty_count += sum(1 for worker in shift_workers if worker is None)
        except Exception as e:
            logging.error(f"Error counting empty slots: {e}")
        return empty_count
    
    def _greedy_fill_empty_slots(self, schedule: Dict, workers_data: List[Dict],
                                 schedule_config: Dict, scheduler_core) -> Dict:
        """
        Fill empty slots using a greedy algorithm.
        Prioritizes workers with fewer shifts and better availability.
        
        Algorithm:
        1. Find all empty slots
        2. For each empty slot:
           - Rank workers by: (a) deviation from target, (b) constraints satisfaction
           - Assign best available worker
        3. Stop when no more slots can be filled
        """
        logging.info(f"   🎯 GREEDY FILL: Starting empty slot filling")
        
        optimized_schedule = copy.deepcopy(schedule)
        filled_count = 0
        
        try:
            # 1. Find all empty slots
            empty_slots = []
            for date, assignments in optimized_schedule.items():
                if isinstance(assignments, list):
                    for post_idx, worker in enumerate(assignments):
                        if worker is None:
                            empty_slots.append({
                                'date': date,
                                'post': post_idx,
                                'format': 'list'
                            })
                elif isinstance(assignments, dict):
                    for shift_type, shift_workers in assignments.items():
                        if isinstance(shift_workers, list):
                            for idx, worker in enumerate(shift_workers):
                                if worker is None:
                                    empty_slots.append({
                                        'date': date,
                                        'shift_type': shift_type,
                                        'idx': idx,
                                        'format': 'dict'
                                    })
            
            if not empty_slots:
                logging.info(f"   ✅ No empty slots found")
                return optimized_schedule
            
            logging.info(f"   📊 Found {len(empty_slots)} empty slots to fill")
            
            # 2. For each empty slot, find best worker using greedy heuristic
            for slot in empty_slots:
                date = slot['date']
                
                # Get worker statistics for greedy selection
                worker_stats = self._calculate_worker_stats(optimized_schedule, workers_data)
                
                # Rank workers by priority (fewer shifts = higher priority)
                candidates = []
                
                for worker in workers_data:
                    worker_id = worker.get('id')
                    worker_name = f"Worker {worker_id}" if isinstance(worker_id, (int, str)) and str(worker_id).isdigit() else str(worker_id)
                    
                    # Check if worker can take this shift
                    if self._can_worker_take_greedy_shift(
                        worker_name, worker_id, date, slot, optimized_schedule, 
                        workers_data, scheduler_core
                    ):
                        stats = worker_stats.get(worker_name, {})
                        assigned = stats.get('total_shifts', 0)
                        target = worker.get('target_shifts', 0)
                        
                        # Greedy score: prioritize workers below target
                        deviation = assigned - target if target > 0 else assigned
                        priority = -deviation  # Negative deviation = higher priority
                        
                        candidates.append({
                            'worker_name': worker_name,
                            'worker_id': worker_id,
                            'priority': priority,
                            'deviation': deviation,
                            'assigned': assigned
                        })
                
                if not candidates:
                    continue
                
                # Sort by priority (highest first)
                candidates.sort(key=lambda x: x['priority'], reverse=True)
                best_worker = candidates[0]
                
                # Assign the slot
                if slot['format'] == 'list':
                    optimized_schedule[date][slot['post']] = best_worker['worker_name']
                elif slot['format'] == 'dict':
                    optimized_schedule[date][slot['shift_type']][slot['idx']] = best_worker['worker_name']
                
                filled_count += 1
                
                if filled_count <= 5:  # Log first 5 assignments
                    date_str = date.strftime('%Y-%m-%d') if hasattr(date, 'strftime') else str(date)
                    logging.info(f"      ✅ Filled slot on {date_str}: {best_worker['worker_name']} "
                               f"(deviation: {best_worker['deviation']:+d}, assigned: {best_worker['assigned']})")
            
            logging.info(f"   ✅ GREEDY FILL: Filled {filled_count}/{len(empty_slots)} empty slots")
            
        except Exception as e:
            logging.error(f"Error in greedy fill: {e}", exc_info=True)
        
        return optimized_schedule
    
    def _calculate_worker_stats(self, schedule: Dict, workers_data: List[Dict]) -> Dict:
        """Calculate current shift counts for all workers."""
        stats = {}
        
        for worker in workers_data:
            worker_id = worker.get('id')
            worker_name = f"Worker {worker_id}" if isinstance(worker_id, (int, str)) and str(worker_id).isdigit() else str(worker_id)
            stats[worker_name] = {
                'total_shifts': 0,
                'weekend_shifts': 0
            }
        
        # Count assignments
        for date, assignments in schedule.items():
            is_weekend = False
            try:
                if hasattr(date, 'weekday'):
                    is_weekend = date.weekday() in [5, 6]
                elif isinstance(date, str):
                    from datetime import datetime
                    date_obj = datetime.strptime(date, '%Y-%m-%d')
                    is_weekend = date_obj.weekday() in [5, 6]
            except:
                pass
            
            if isinstance(assignments, list):
                for worker in assignments:
                    if worker and worker in stats:
                        stats[worker]['total_shifts'] += 1
                        if is_weekend:
                            stats[worker]['weekend_shifts'] += 1
            elif isinstance(assignments, dict):
                for shift_workers in assignments.values():
                    if isinstance(shift_workers, list):
                        for worker in shift_workers:
                            if worker and worker in stats:
                                stats[worker]['total_shifts'] += 1
                                if is_weekend:
                                    stats[worker]['weekend_shifts'] += 1
        
        return stats
    
    def _can_worker_take_greedy_shift(self, worker_name: str, worker_id, date,
                                      slot: Dict, schedule: Dict, workers_data: List[Dict],
                                      scheduler_core) -> bool:
        """
        Check if worker can take a shift with basic constraint checking.
        Simplified version for greedy algorithm (less strict than full validation).
        
        CRITICAL: This function MUST validate tolerance to prevent violations during optimization.
        """
        try:
            # Check if worker already has a shift on this date
            if isinstance(schedule.get(date), list):
                if worker_name in schedule[date]:
                    return False
            elif isinstance(schedule.get(date), dict):
                for shift_workers in schedule[date].values():
                    if isinstance(shift_workers, list) and worker_name in shift_workers:
                        return False
            
            # CRITICAL: Check tolerance limit (±12% absolute maximum during optimization)
            # This prevents optimization from violating tolerance limits
            if hasattr(scheduler_core, 'builder') and scheduler_core.builder:
                builder = scheduler_core.builder
                
                # Find worker in workers_data to get target_shifts
                worker_data = None
                for w in workers_data:
                    w_id = w.get('id')
                    w_name = f"Worker {w_id}" if isinstance(w_id, (int, str)) and str(w_id).isdigit() else str(w_id)
                    if w_name == worker_name:
                        worker_data = w
                        break
                
                if worker_data:
                    # Count current shifts for this worker
                    # CRITICAL: Count ALL shifts, not just dates - worker can have multiple shifts per date
                    current_shifts = 0
                    for d, assigns in schedule.items():
                        if isinstance(assigns, list):
                            current_shifts += assigns.count(worker_name)
                        elif isinstance(assigns, dict):
                            for shift_workers in assigns.values():
                                if isinstance(shift_workers, list):
                                    current_shifts += shift_workers.count(worker_name)
                    
                    # Use Phase 2 tolerance (12%) during optimization
                    # Part-time workers get adjusted tolerance (minimum 5%)
                    target_shifts = worker_data.get('target_shifts', 0)
                    work_percentage = worker_data.get('work_percentage', 100) / 100.0
                    
                    if target_shifts > 0:
                        base_tolerance = 0.12  # Phase 2: ±12% absolute maximum
                        adjusted_tolerance = max(base_tolerance * work_percentage, 0.05)
                        
                        max_shifts = round(target_shifts * (1 + adjusted_tolerance))
                        
                        # Check if adding this shift would exceed the limit
                        if current_shifts + 1 > max_shifts:
                            logging.debug(f"   ❌ Tolerance violation prevented: {worker_name} "
                                        f"would have {current_shifts + 1}/{target_shifts} shifts "
                                        f"(max: {max_shifts}, tolerance: {adjusted_tolerance*100:.1f}%)")
                            return False
            
            # Check basic gap constraint (simplified - just check adjacent days)
            if hasattr(scheduler_core, 'scheduler') and hasattr(scheduler_core.scheduler, 'gap_between_shifts'):
                gap = scheduler_core.scheduler.gap_between_shifts
                
                # Get worker's assignments
                worker_dates = []
                for d, assigns in schedule.items():
                    if isinstance(assigns, list) and worker_name in assigns:
                        worker_dates.append(d)
                    elif isinstance(assigns, dict):
                        for shift_workers in assigns.values():
                            if isinstance(shift_workers, list) and worker_name in shift_workers:
                                worker_dates.append(d)
                                break
                
                # Check gap with nearest assignments
                for worker_date in worker_dates:
                    try:
                        if hasattr(date, 'date') and hasattr(worker_date, 'date'):
                            days_diff = abs((date - worker_date).days)
                            if days_diff < gap and days_diff > 0:
                                return False
                    except:
                        pass
            
            return True
            
        except Exception as e:
            logging.debug(f"Error checking worker {worker_name} for greedy shift: {e}")
            return False
