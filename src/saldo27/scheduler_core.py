"""
Scheduler Core Module

This module contains the main orchestration logic for the scheduler system,
extracted from the original Scheduler class to improve maintainability and separation of concerns.
"""

import copy
import hashlib
import logging
import math
import random
from datetime import datetime
from typing import Any

from saldo27.adaptive_iterations import AdaptiveIterationManager
from saldo27.advanced_distribution_engine import AdvancedDistributionEngine
from saldo27.exceptions import ConstraintViolationError, SchedulerError
from saldo27.iterative_optimizer import IterativeOptimizer
from saldo27.operation_prioritizer import OperationPrioritizer
from saldo27.optimization_metrics import OptimizationMetrics
from saldo27.progress_monitor import ProgressMonitor
from saldo27.scheduler_config import SchedulerConfig
from saldo27.shift_tolerance_validator import ShiftToleranceValidator
from saldo27.strict_balance_optimizer import StrictBalanceOptimizer


class SchedulerCore:
    """
    Core orchestration class that manages the high-level scheduling workflow.
    This class focuses on coordination between components rather than implementation details.
    """

    def __init__(self, scheduler):
        """
        Initialize the scheduler core with enhanced optimization systems.

        Args:
            scheduler: Reference to the main Scheduler instance
        """
        self.scheduler = scheduler
        self.config = scheduler.config
        self.start_date = scheduler.start_date
        self.end_date = scheduler.end_date
        self.workers_data = scheduler.workers_data
        self._cancelled = False

        # Initialize enhancement systems
        self.metrics = OptimizationMetrics(scheduler)
        self.prioritizer = OperationPrioritizer(scheduler, self.metrics)
        self.progress_monitor = None  # Will be initialized in orchestrate_schedule_generation

        # Initialize tolerance validation and iterative optimization
        self.tolerance_validator = ShiftToleranceValidator(scheduler)
        # Iterative optimizer works with Phase 2 tolerance (±12% absolute limit)
        # Note: Initial distribution uses Phase 1 (±10% objective), optimizer handles both phases
        self.iterative_optimizer = IterativeOptimizer(max_iterations=80, tolerance=0.12)

        # Initialize adaptive iteration manager for intelligent optimization
        self.adaptive_manager = AdaptiveIterationManager(scheduler)

        # Initialize advanced distribution engine
        self.advanced_engine = None  # Will be initialized when schedule_builder is available

        # Initialize strict balance optimizer
        self.balance_optimizer = None  # Will be initialized when schedule_builder is available

        logging.info("SchedulerCore initialized with enhanced optimization systems and tolerance validation")

    def orchestrate_schedule_generation(self, max_improvement_loops: int = 90, max_complete_attempts: int = 1) -> bool:
        """
        Main orchestration method for schedule generation workflow with multiple complete attempts.

        Args:
            max_improvement_loops: Maximum number of improvement iterations per attempt
            max_complete_attempts: Maximum number of complete schedule attempts (default: 1)

        Returns:
            bool: True if schedule generation was successful
        """
        logging.info("Starting schedule generation orchestration...")
        start_time = datetime.now()

        try:
            # Phase 1: Initialize schedule structure
            if not self._initialize_schedule_phase():
                raise ConstraintViolationError("Failed to initialize schedule structure")

            # Phase 2: Assign mandatory shifts
            if not self._assign_mandatory_phase():
                raise ConstraintViolationError("Failed to assign mandatory shifts")

            # Save mandatory state (preserved across all attempts)
            mandatory_backup = copy.deepcopy(self.scheduler.schedule)
            mandatory_assignments = copy.deepcopy(self.scheduler.worker_assignments)
            mandatory_counts = copy.deepcopy(self.scheduler.worker_shift_counts)
            mandatory_weekend_counts = copy.deepcopy(self.scheduler.worker_weekend_counts)
            mandatory_posts = copy.deepcopy(self.scheduler.worker_posts)
            mandatory_locked = copy.deepcopy(self.scheduler.schedule_builder._locked_mandatory)

            # Phase 3: Multiple complete attempts
            logging.info("=" * 80)

            # SIMULATION MODE: Single attempt
            if self.config.get("is_simulation", False):
                logging.info("🧪 SIMULATION MODE: Limiting to 1 complete attempt")
                max_complete_attempts = 1

            logging.info(f"🔄 STARTING {max_complete_attempts} COMPLETE SCHEDULE ATTEMPTS")
            logging.info("   Each attempt will respect Phase 1 (±10% OBJECTIVE) tolerance initially")
            logging.info("   Phase 2 (±12% ABSOLUTE LIMIT) activates if coverage < 95%")
            logging.info("=" * 80)

            complete_attempts = []

            for complete_attempt_num in range(1, max_complete_attempts + 1):
                # Check cancellation flag
                if getattr(self.scheduler, "_cancelled", False):
                    logging.info("🛑 Schedule generation cancelled by user")
                    return False

                logging.info(f"\n{'█' * 80}")
                logging.info(f"🎯 COMPLETE ATTEMPT {complete_attempt_num}/{max_complete_attempts}")
                logging.info(f"{'█' * 80}")

                # Restore mandatory state for this attempt
                self.scheduler.schedule = copy.deepcopy(mandatory_backup)
                self.scheduler.worker_assignments = copy.deepcopy(mandatory_assignments)
                self.scheduler.worker_shift_counts = copy.deepcopy(mandatory_counts)
                self.scheduler.worker_weekend_counts = copy.deepcopy(mandatory_weekend_counts)
                self.scheduler.worker_posts = copy.deepcopy(mandatory_posts)
                # CRITICAL: Sync ALL schedule_builder references to the new deep-copied objects
                self._sync_builder_references()
                self.scheduler.schedule_builder._locked_mandatory = copy.deepcopy(mandatory_locked)

                # Phase 3.1: Multiple initial distribution attempts
                if not self._multiple_initial_distribution_attempts():
                    logging.warning(f"Complete attempt {complete_attempt_num} failed at initial distribution")
                    continue

                # Phase 3.2: Iterative improvement
                if not self._iterative_improvement_phase(max_improvement_loops):
                    logging.warning(f"Complete attempt {complete_attempt_num} failed at iterative improvement")
                    # Don't skip - save what we have

                # Calculate final metrics
                coverage = self._calculate_coverage_percentage()
                empty_shifts = self.metrics.count_empty_shifts()
                score = self.metrics.calculate_overall_schedule_score()
                workload_imbalance = self.metrics.calculate_workload_imbalance()
                weekend_imbalance = self.metrics.calculate_weekend_imbalance()

                logging.info(f"\n📊 Complete Attempt {complete_attempt_num} Final Metrics:")
                logging.info(f"   Coverage: {coverage:.2f}%")
                logging.info(f"   Empty Shifts: {empty_shifts}")
                logging.info(f"   Overall Score: {score:.2f}")
                logging.info(f"   Workload Imbalance: {workload_imbalance:.2f}")
                logging.info(f"   Weekend Imbalance: {weekend_imbalance:.2f}")

                # Save this complete attempt
                complete_attempts.append(
                    {
                        "attempt": complete_attempt_num,
                        "coverage": coverage,
                        "empty_shifts": empty_shifts,
                        "score": score,
                        "workload_imbalance": workload_imbalance,
                        "weekend_imbalance": weekend_imbalance,
                        "schedule": copy.deepcopy(self.scheduler.schedule),
                        "assignments": copy.deepcopy(self.scheduler.worker_assignments),
                        "counts": copy.deepcopy(self.scheduler.worker_shift_counts),
                        "weekend_counts": copy.deepcopy(self.scheduler.worker_weekend_counts),
                        "posts": copy.deepcopy(self.scheduler.worker_posts),
                        "locked_mandatory": copy.deepcopy(self.scheduler.schedule_builder._locked_mandatory),
                    }
                )

                logging.info(f"✅ Complete attempt {complete_attempt_num} saved successfully")

            # Phase 4: Select best complete attempt
            if not complete_attempts:
                raise ConstraintViolationError("All complete attempts failed!")

            best_attempt = self._select_best_complete_attempt(complete_attempts)

            # Apply the best complete attempt
            logging.info(f"\n{'=' * 80}")
            logging.info(f"🏆 SELECTING BEST COMPLETE ATTEMPT #{best_attempt['attempt']}")
            logging.info(f"{'=' * 80}")
            logging.info(f"   Coverage: {best_attempt['coverage']:.2f}%")
            logging.info(f"   Empty Shifts: {best_attempt['empty_shifts']}")
            logging.info(f"   Overall Score: {best_attempt['score']:.2f}")
            logging.info(f"   Workload Imbalance: {best_attempt['workload_imbalance']:.2f}")
            logging.info(f"   Weekend Imbalance: {best_attempt['weekend_imbalance']:.2f}")

            self.scheduler.schedule = best_attempt["schedule"]
            self.scheduler.worker_assignments = best_attempt["assignments"]
            self.scheduler.worker_shift_counts = best_attempt["counts"]
            self.scheduler.worker_weekend_counts = best_attempt["weekend_counts"]
            self.scheduler.worker_posts = best_attempt["posts"]
            # Sync ALL schedule_builder references + restore locked mandatory
            self._sync_builder_references()
            self.scheduler.schedule_builder._locked_mandatory = best_attempt["locked_mandatory"]

            # Phase 5: Finalization
            if not self._finalization_phase():
                raise ConstraintViolationError("Failed to finalize schedule")

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            logging.info(f"Schedule generation orchestration completed successfully in {duration:.2f} seconds.")
            return True

        except Exception as e:
            logging.error(f"Schedule generation orchestration failed: {e!s}", exc_info=True)
            if isinstance(e, SchedulerError):
                raise e
            else:
                raise SchedulerError(f"Orchestration failed: {e!s}")

    def _sync_builder_references(self):
        """
        Synchronize all schedule_builder attribute references with the
        current scheduler objects.  Must be called after any deepcopy
        that replaces scheduler-level dicts/sets so that the builder
        does not keep stale pointers to the old objects.
        """
        sb = self.scheduler.schedule_builder
        sb.schedule = self.scheduler.schedule
        sb.worker_assignments = self.scheduler.worker_assignments
        sb.worker_posts = self.scheduler.worker_posts
        sb.worker_weekdays = self.scheduler.worker_weekdays
        sb.worker_weekends = self.scheduler.worker_weekends
        sb.constraint_skips = self.scheduler.constraint_skips
        sb.last_assigned_date = self.scheduler.last_assignment_date
        sb.consecutive_shifts = self.scheduler.consecutive_shifts

        # Also sync constraint_checker references so it doesn't hold stale dicts
        if hasattr(sb, "constraint_checker") and sb.constraint_checker:
            cc = sb.constraint_checker
            cc.schedule = self.scheduler.schedule
            cc.worker_assignments = self.scheduler.worker_assignments

    def _initialize_schedule_phase(self) -> bool:
        """
        Phase 1: Initialize schedule structure and data.

        Returns:
            bool: True if initialization was successful
        """
        logging.info("Phase 1: Initializing schedule structure...")

        try:
            # Reset scheduler state
            self.scheduler.schedule = {}
            self.scheduler.worker_assignments = {w["id"]: set() for w in self.workers_data}
            self.scheduler.worker_shift_counts = {w["id"]: 0 for w in self.workers_data}
            self.scheduler.worker_weekend_counts = {w["id"]: 0 for w in self.workers_data}
            self.scheduler.worker_posts = {w["id"]: set() for w in self.workers_data}
            self.scheduler.last_assignment_date = {w["id"]: None for w in self.workers_data}
            self.scheduler.consecutive_shifts = {w["id"]: 0 for w in self.workers_data}

            # Initialize schedule with variable shifts
            self.scheduler._initialize_schedule_with_variable_shifts()

            # Create schedule builder
            from saldo27.schedule_builder import ScheduleBuilder

            self.scheduler.schedule_builder = ScheduleBuilder(self.scheduler)

            logging.info(f"Schedule structure initialized with {len(self.scheduler.schedule)} dates")
            return True

        except Exception as e:
            logging.error(f"Failed to initialize schedule phase: {e!s}", exc_info=True)
            return False

    def _assign_mandatory_phase(self) -> bool:
        """
        Phase 2: Assign mandatory shifts and lock them in place.

        Returns:
            bool: True if mandatory assignment was successful
        """
        logging.info("Phase 2: Assigning mandatory shifts...")

        try:
            # Pre-assign mandatory shifts
            self.scheduler.schedule_builder._assign_mandatory_guards()

            # Synchronize tracking data
            self.scheduler.schedule_builder._synchronize_tracking_data()

            # Save initial state as best
            self.scheduler.schedule_builder._save_current_as_best(initial=True)

            # Log summary
            self.scheduler.log_schedule_summary("After Mandatory Assignment")

            logging.info("Mandatory assignment phase completed")
            return True

        except Exception as e:
            logging.error(f"Failed in mandatory assignment phase: {e!s}", exc_info=True)
            return False

    def _multiple_initial_distribution_attempts(self) -> bool:
        """
        Phase 2.5: Perform multiple initial distribution attempts with different strategies
        and select the best one based on quality score.

        This phase uses AdaptiveIterationManager to determine how many attempts to make
        based on problem complexity. Each attempt uses a different strategy:
        - Random seed variation
        - Different worker ordering
        - Different post assignment priorities

        Returns:
            bool: True if at least one attempt was successful
        """
        logging.info("=" * 80)
        logging.info("Phase 2.5: Multiple Initial Distribution Attempts (STRICT MODE)")
        logging.info("=" * 80)

        try:
            # CRITICAL: Enable STRICT MODE for initial distribution
            self.scheduler.schedule_builder.enable_strict_mode()
            logging.info("🔒 STRICT MODE activated for initial distribution phase")

            # Check if this is a simulation run
            is_simulation = self.config.get("is_simulation", False)
            if is_simulation:
                logging.info("🧪 SIMULATION MODE DETECTED: Enabling safety limits")
                # Safety check: If no workers, fail fast
                if not self.workers_data:
                    logging.info("❌ Simulation Aborted: No workers available for simulation.")
                    return False

            logging.info("   - Phase 1 target: ±10% objective (adjusted by work_percentage)")
            logging.info("   - Phase 2 emergency: ±12% ABSOLUTE LIMIT (if needed)")
            logging.info("   - Gap reduction: NOT allowed")
            logging.info("   - Pattern 7/14: Allowed if worker needs 3+ more shifts (prevents blocking)")
            logging.info("   - Mandatory shifts: NEVER modified")
            logging.info("   - Incompatibilities: ALWAYS respected")
            logging.info("   - Days off: NEVER violated")

            # Get adaptive configuration to determine number of attempts
            adaptive_config = self.adaptive_manager.calculate_adaptive_iterations()

            # Determine number of initial attempts based on complexity
            complexity_score = adaptive_config.get("complexity_score", 0)

            # UPDATED: Reduced maximum attempts from 60 to 40 for better performance
            # SIMULATION MODE: Cap attempts to ensure responsiveness
            if is_simulation:
                num_attempts = 5
                logging.info("🧪 Simulation Mode: Capped initial distribution attempts to 5")
            elif complexity_score < 1000:
                num_attempts = 10
            elif complexity_score < 5000:
                num_attempts = 20
            elif complexity_score < 15000:
                num_attempts = 30
            else:
                num_attempts = 40  # Maximum reduced from 60 to 40

            # When prior-period data is loaded the initial worker ordering is already
            # biased by accumulated shift counts, so there is less variance between
            # attempts.  Halve the attempt count to recover most of the extra time
            # cost introduced by the prior-data constraints, while keeping at least 5.
            has_prior = bool(getattr(self.scheduler, "prior_assignments", {}))
            if has_prior and not is_simulation:
                num_attempts = max(5, num_attempts // 2)
                logging.info(f"📅 Prior calendar loaded: reduced initial attempts to {num_attempts}")

            logging.info(f"Problem complexity: {complexity_score:.0f}")
            logging.info(f"Number of initial distribution attempts: {num_attempts}")

            # Save current mandatory state (this must be preserved)
            mandatory_backup = copy.deepcopy(self.scheduler.schedule)
            mandatory_assignments = copy.deepcopy(self.scheduler.worker_assignments)
            mandatory_counts = copy.deepcopy(self.scheduler.worker_shift_counts)
            mandatory_weekend_counts = copy.deepcopy(self.scheduler.worker_weekend_counts)
            mandatory_posts = copy.deepcopy(self.scheduler.worker_posts)
            # CRITICAL: Save locked mandatory shifts to prevent them from being modified
            mandatory_locked = copy.deepcopy(self.scheduler.schedule_builder._locked_mandatory)

            best_attempt = None
            best_score = -1
            # If prior data is loaded, capture the shift counts that already include
            # the prior-period seed (set by _synchronize_tracking_data after mandatory
            # assignment).  We restore these after each attempt so that the ordering
            # bias from the prior period is preserved across resets.
            _prior_seeded_counts = (
                {wid: cnt for wid, cnt in self.scheduler.worker_shift_counts.items()} if has_prior else None
            )

            attempts_results = []
            no_improve_count = 0  # Track consecutive attempts with no score improvement

            # Initialize best-state variables (will be set when first successful attempt beats best_score)
            best_schedule = copy.deepcopy(self.scheduler.schedule)
            best_assignments = copy.deepcopy(self.scheduler.worker_assignments)
            best_counts = copy.deepcopy(self.scheduler.worker_shift_counts)
            best_weekend_counts = copy.deepcopy(self.scheduler.worker_weekend_counts)
            best_posts = copy.deepcopy(self.scheduler.worker_posts)
            best_locked_mandatory = copy.deepcopy(mandatory_locked)

            # Start adaptive iteration manager timer
            self.adaptive_manager.start_time = datetime.now()

            for attempt_num in range(1, num_attempts + 1):
                # Check cancellation flag
                if getattr(self.scheduler, "_cancelled", False):
                    logging.info("🛑 Initial distribution cancelled by user")
                    break

                logging.info(f"\n{'─' * 80}")
                logging.info(f"🔄 Initial Distribution Attempt {attempt_num}/{num_attempts}")
                logging.info(f"{'─' * 80}")

                # Restore mandatory state
                self.scheduler.schedule = copy.deepcopy(mandatory_backup)
                self.scheduler.worker_assignments = copy.deepcopy(mandatory_assignments)
                self.scheduler.worker_shift_counts = copy.deepcopy(mandatory_counts)
                self.scheduler.worker_weekend_counts = copy.deepcopy(mandatory_weekend_counts)
                self.scheduler.worker_posts = copy.deepcopy(mandatory_posts)

                # Re-apply prior-period seed so ordering stays biased correctly
                if _prior_seeded_counts:
                    self.scheduler.worker_shift_counts.update(_prior_seeded_counts)

                # CRITICAL: Sync ALL schedule_builder references to the new deep-copied objects
                if hasattr(self.scheduler, "schedule_builder"):
                    self._sync_builder_references()
                    # CRITICAL: Restore locked mandatory shifts
                    self.scheduler.schedule_builder._locked_mandatory = copy.deepcopy(mandatory_locked)
                    # CRITICAL: Rebuild caches to reflect new state (prevents cache staling between attempts)
                    self.scheduler.schedule_builder._build_optimization_caches()

                logging.info(f"Restored {len(mandatory_locked)} locked mandatory shifts")
                logging.info("Rebuilt schedule builder caches for fresh attempt")

                # Log state before fill
                empty_before = sum(
                    1 for date, shifts in self.scheduler.schedule.items() for worker in shifts if worker is None
                )
                logging.info(f"Empty shifts before fill: {empty_before}")

                # Apply different strategy for each attempt
                strategy = self._select_distribution_strategy(attempt_num, num_attempts)
                logging.info(f"Strategy for attempt {attempt_num}: {strategy['name']}")

                # Perform initial fill with this strategy
                success = self._perform_initial_fill_with_strategy(strategy)

                # Log state after fill
                empty_after = sum(
                    1 for date, shifts in self.scheduler.schedule.items() for worker in shifts if worker is None
                )
                filled_count = empty_before - empty_after
                logging.info(f"Filled {filled_count} shifts (empty after: {empty_after})")

                if not success:
                    logging.warning(f"Attempt {attempt_num} failed to fill schedule")
                    attempts_results.append(
                        {"attempt": attempt_num, "strategy": strategy["name"], "success": False, "score": 0}
                    )
                    continue

                # Calculate quality score for this attempt
                score = self.metrics.calculate_overall_schedule_score()

                # Get detailed metrics
                empty_shifts = self.metrics.count_empty_shifts()
                workload_imbalance = self.metrics.calculate_workload_imbalance()
                weekend_imbalance = self.metrics.calculate_weekend_imbalance()

                logging.info(f"📊 Attempt {attempt_num} Results:")
                logging.info(f"   Overall Score: {score:.2f}")
                logging.info(f"   Empty Shifts: {empty_shifts}")
                logging.info(f"   Workload Imbalance: {workload_imbalance:.2f}")
                logging.info(f"   Weekend Imbalance: {weekend_imbalance:.2f}")

                # Record this attempt
                attempts_results.append(
                    {
                        "attempt": attempt_num,
                        "strategy": strategy["name"],
                        "success": True,
                        "score": score,
                        "empty_shifts": empty_shifts,
                        "workload_imbalance": workload_imbalance,
                        "weekend_imbalance": weekend_imbalance,
                    }
                )

                # Check if this is the best so far
                if score > best_score:
                    best_score = score
                    best_attempt = attempt_num
                    no_improve_count = 0  # Reset early-stop counter on improvement
                    # Save this as the best attempt
                    best_schedule = copy.deepcopy(self.scheduler.schedule)
                    best_assignments = copy.deepcopy(self.scheduler.worker_assignments)
                    best_counts = copy.deepcopy(self.scheduler.worker_shift_counts)
                    best_weekend_counts = copy.deepcopy(self.scheduler.worker_weekend_counts)
                    best_posts = copy.deepcopy(self.scheduler.worker_posts)
                    # CRITICAL: Save locked mandatory from best attempt
                    best_locked_mandatory = copy.deepcopy(self.scheduler.schedule_builder._locked_mandatory)

                    logging.info(f"✨ New best attempt! Score: {score:.2f}")
                else:
                    no_improve_count += 1

                # Early stop: if no improvement for 8 consecutive attempts, stop
                if no_improve_count >= 8 and attempt_num >= 10:
                    logging.info(
                        f"⏩ Early stop: no improvement in {no_improve_count} consecutive attempts "
                        f"(best score: {best_score:.2f} from attempt #{best_attempt})"
                    )
                    break

            # Summary of all attempts
            logging.info(f"\n{'=' * 80}")
            logging.info("📈 INITIAL DISTRIBUTION ATTEMPTS SUMMARY")
            logging.info(f"{'=' * 80}")

            successful_attempts = [r for r in attempts_results if r["success"]]

            if not successful_attempts:
                logging.error("❌ All initial distribution attempts failed!")
                return False

            logging.info(f"Successful attempts: {len(successful_attempts)}/{num_attempts}")

            # Display results table
            logging.info(
                f"\n{'Attempt':<10} {'Strategy':<25} {'Score':<10} {'Empty':<8} {'Work Imb':<10} {'Weekend Imb':<12}"
            )
            logging.info("─" * 90)

            for result in attempts_results:
                if result["success"]:
                    marker = "👑" if result["attempt"] == best_attempt else "  "
                    logging.info(
                        f"{marker} {result['attempt']:<8} {result['strategy']:<25} "
                        f"{result['score']:<10.2f} {result['empty_shifts']:<8} "
                        f"{result['workload_imbalance']:<10.2f} {result['weekend_imbalance']:<12.2f}"
                    )
                else:
                    logging.info(f"  {result['attempt']:<8} {result['strategy']:<25} FAILED")

            # Apply the best attempt
            logging.info(f"\n🏆 Applying best attempt #{best_attempt} with score {best_score:.2f}")

            self.scheduler.schedule = best_schedule
            self.scheduler.worker_assignments = best_assignments
            self.scheduler.worker_shift_counts = best_counts
            self.scheduler.worker_weekend_counts = best_weekend_counts
            self.scheduler.worker_posts = best_posts
            # CRITICAL: Sync ALL schedule_builder references + restore locked mandatory
            self._sync_builder_references()
            self.scheduler.schedule_builder._locked_mandatory = best_locked_mandatory

            logging.info(f"Restored {len(best_locked_mandatory)} locked mandatory shifts from best attempt")

            # Synchronize tracking data
            self.scheduler.schedule_builder._synchronize_tracking_data()

            # Save as current best
            self.scheduler.schedule_builder._save_current_as_best(initial=False)

            # Export initial calendar PDF before optimization
            self._export_initial_calendar_pdf()

            logging.info("=" * 80)
            logging.info("✅ Multiple initial distribution phase completed successfully")
            logging.info("=" * 80)

            return True

        except Exception as e:
            logging.error(f"Error during multiple initial distribution attempts: {e!s}", exc_info=True)
            return False

    def _iterative_improvement_phase(self, max_improvement_loops: int) -> bool:
        """
        Phase 3: Enhanced iterative improvement with smart optimization.

        Args:
            max_improvement_loops: Maximum number of improvement iterations

        Returns:
            bool: True if improvement phase completed successfully
        """
        logging.info("Phase 3: Starting enhanced iterative improvement...")

        # Initialize advanced distribution engine if not already done
        if self.advanced_engine is None and hasattr(self.scheduler, "schedule_builder"):
            self.advanced_engine = AdvancedDistributionEngine(self.scheduler, self.scheduler.schedule_builder)
            logging.info("✅ Advanced Distribution Engine initialized")

        # Initialize strict balance optimizer if not already done
        if self.balance_optimizer is None and hasattr(self.scheduler, "schedule_builder"):
            self.balance_optimizer = StrictBalanceOptimizer(self.scheduler, self.scheduler.schedule_builder)
            logging.info("✅ Strict Balance Optimizer initialized")

        # Initialize progress monitor
        self.progress_monitor = ProgressMonitor(self.scheduler, max_improvement_loops)
        self.progress_monitor.start_monitoring()

        improvement_loop_count = 0
        overall_improvement_made = True

        try:
            # Aumentar el número de ciclos por defecto si no se especifica, pero limitar en simulación
            if self.config.get("is_simulation", False):
                max_improvement_loops = min(max_improvement_loops, 120)
                logging.info(f"🧪 Simulation Mode: Capped improvement loops to {max_improvement_loops}")
            elif max_improvement_loops < 120:
                max_improvement_loops = 120

            # Calculate initial score for comparison
            current_overall_score = self.metrics.calculate_overall_schedule_score(log_components=True)
            logging.info(f"Score inicial: {current_overall_score:.2f}")

            # Checkpoint: track best score seen during the loop so we can rollback
            best_loop_score = current_overall_score
            best_loop_state = None

            # Track operations that were reverted so we skip them in later loops
            reverted_ops: set[str] = set()

            # Simulated Annealing parameters
            sa_temperature_start = 1.0
            sa_cooling_rate = 0.7
            sa_max_drop = -2.0  # never accept a drop worse than this

            while overall_improvement_made and improvement_loop_count < max_improvement_loops:
                loop_start_time = datetime.now()
                improvement_loop_count += 1

                logging.info(f"--- Starting Enhanced Improvement Loop {improvement_loop_count} ---")

                # Get current state for decision making
                current_state = {
                    "empty_shifts_count": self.metrics.count_empty_shifts(),
                    "workload_imbalance": self.metrics.calculate_workload_imbalance(),
                    "weekend_imbalance": self.metrics.calculate_weekend_imbalance(),
                }

                # Record score at the start of this cycle for aggregate comparison
                cycle_start_score = self.metrics.calculate_overall_schedule_score()

                # Get dynamically prioritized operations
                prioritized_operations = self.prioritizer.prioritize_operations_dynamically()

                # Execute operations with enhanced tracking
                operation_results = {}
                cycle_improvement_made = False

                for operation_name, operation_func, priority in prioritized_operations:
                    try:
                        # Skip operations that were reverted in a previous loop
                        if operation_name in reverted_ops:
                            logging.debug(f"Skipping {operation_name}: reverted in previous loop")
                            operation_results[operation_name] = {
                                "improved": False,
                                "skipped": True,
                                "reason": "reverted in previous loop",
                            }
                            continue

                        # Check if operation should be skipped
                        should_skip, skip_reason = self.prioritizer.should_skip_operation(operation_name, current_state)

                        if should_skip:
                            logging.debug(f"Skipping {operation_name}: {skip_reason}")
                            operation_results[operation_name] = {
                                "improved": False,
                                "skipped": True,
                                "reason": skip_reason,
                            }
                            continue

                        # Per-operation checkpoint: save state before non-trivial operations
                        if operation_name != "synchronize_tracking_data":
                            op_checkpoint = {
                                "schedule": copy.deepcopy(self.scheduler.schedule),
                                "assignments": copy.deepcopy(self.scheduler.worker_assignments),
                                "counts": copy.deepcopy(self.scheduler.worker_shift_counts),
                                "weekend_counts": copy.deepcopy(self.scheduler.worker_weekend_counts),
                                "posts": copy.deepcopy(self.scheduler.worker_posts),
                                "locked_mandatory": copy.deepcopy(self.scheduler.schedule_builder._locked_mandatory),
                            }
                        else:
                            op_checkpoint = None

                        # Measure performance of operation
                        before_score = self.metrics.calculate_overall_schedule_score()
                        operation_start_time = datetime.now()

                        # Execute operation
                        if operation_name == "synchronize_tracking_data":
                            operation_func()
                            operation_made_change = True  # This operation always "succeeds"
                        else:
                            operation_made_change = operation_func()

                        operation_end_time = datetime.now()
                        execution_time = (operation_end_time - operation_start_time).total_seconds()

                        # Evaluate improvement quality
                        after_score = self.metrics.calculate_overall_schedule_score()

                        # Per-operation rollback with Simulated Annealing
                        if op_checkpoint and operation_made_change and after_score < before_score - 0.001:
                            delta = after_score - before_score  # negative
                            sa_temp = sa_temperature_start * (sa_cooling_rate ** (improvement_loop_count - 1))
                            # SA: probabilistically accept small drops to escape local optima
                            sa_accept = (
                                delta >= sa_max_drop and sa_temp > 0.01 and random.random() < math.exp(delta / sa_temp)
                            )
                            if sa_accept:
                                logging.info(
                                    f"🎲 SA accepted {operation_name}: "
                                    f"{before_score:.2f} → {after_score:.2f} "
                                    f"(Δ={delta:.2f}, T={sa_temp:.3f})"
                                )
                                # Don't add to reverted_ops — allow re-execution
                                cycle_improvement_made = True  # keep loop alive
                                operation_results[operation_name] = {
                                    "improved": False,
                                    "sa_accepted": True,
                                }
                                continue
                            # Revert: SA rejected or drop too large
                            self.scheduler.schedule = op_checkpoint["schedule"]
                            self.scheduler.worker_assignments = op_checkpoint["assignments"]
                            self.scheduler.worker_shift_counts = op_checkpoint["counts"]
                            self.scheduler.worker_weekend_counts = op_checkpoint["weekend_counts"]
                            self.scheduler.worker_posts = op_checkpoint["posts"]
                            self.scheduler.schedule_builder._locked_mandatory = op_checkpoint["locked_mandatory"]
                            self._sync_builder_references()
                            logging.info(f"↩️  {operation_name}: revertido ({before_score:.2f} → {after_score:.2f})")
                            operation_results[operation_name] = {
                                "improved": False,
                                "reverted": True,
                            }
                            reverted_ops.add(operation_name)
                            continue

                        if operation_made_change and operation_name != "synchronize_tracking_data":
                            is_significant, improvement_ratio = self.metrics.evaluate_improvement_quality(
                                before_score, after_score, operation_name
                            )

                            if is_significant:
                                logging.info(
                                    f"✅ {operation_name}: mejora significativa "
                                    f"({improvement_ratio:.4f}, +{after_score - before_score:.2f})"
                                )
                                cycle_improvement_made = True
                                # Clear reverted memory: state changed, operations may work now
                                reverted_ops.clear()
                            else:
                                logging.debug(f"⚠️  {operation_name}: mejora marginal ({improvement_ratio:.4f})")

                        # Record operation results
                        operation_results[operation_name] = self.prioritizer.analyze_operation_effectiveness(
                            operation_name, before_score, after_score, execution_time
                        )

                    except Exception as e:
                        logging.warning(f"Operation {operation_name} failed: {e!s}")
                        operation_results[operation_name] = {"improved": False, "error": str(e)}

                # Update current score after all operations
                current_overall_score = self.metrics.calculate_overall_schedule_score(log_components=True)

                # Track iteration progress with enhanced monitoring
                progress_data = self.progress_monitor.track_iteration_progress(
                    improvement_loop_count, operation_results, current_overall_score, current_state
                )

                # Record iteration for trend analysis
                sa_accepts_in_loop = sum(
                    1 for r in operation_results.values() if isinstance(r, dict) and r.get("sa_accepted")
                )
                self.metrics.record_iteration_result(
                    improvement_loop_count,
                    operation_results,
                    current_overall_score,
                    sa_accepts=sa_accepts_in_loop,
                    best_score=best_loop_score,
                )

                # Check if should continue with smart early stopping
                should_continue, reason = self.metrics.should_continue_optimization(improvement_loop_count)
                if not should_continue:
                    logging.info(f"🛑 Parada temprana activada: {reason}")
                    break

                # Use aggregate cycle score improvement OR per-operation significance as loop guard
                cycle_end_score = self.metrics.calculate_overall_schedule_score()
                cycle_delta = cycle_end_score - cycle_start_score
                overall_improvement_made = cycle_improvement_made or cycle_delta > 0.01

                # Checkpoint: save state if this is the best score so far
                if cycle_end_score > best_loop_score + 0.001:
                    best_loop_score = cycle_end_score
                    best_loop_state = {
                        "schedule": copy.deepcopy(self.scheduler.schedule),
                        "assignments": copy.deepcopy(self.scheduler.worker_assignments),
                        "counts": copy.deepcopy(self.scheduler.worker_shift_counts),
                        "weekend_counts": copy.deepcopy(self.scheduler.worker_weekend_counts),
                        "posts": copy.deepcopy(self.scheduler.worker_posts),
                        "locked_mandatory": copy.deepcopy(self.scheduler.schedule_builder._locked_mandatory),
                    }
                    logging.info(f"💾 Checkpoint guardado: score {best_loop_score:.2f}")

                if cycle_delta > 0.01 and not cycle_improvement_made:
                    logging.info(
                        f"📈 Aggregate cycle improvement: +{cycle_delta:.2f} "
                        f"({cycle_start_score:.2f} → {cycle_end_score:.2f})"
                    )
                elif cycle_improvement_made and cycle_delta <= 0.01:
                    logging.info(
                        f"📈 Operations improved components but aggregate score flat/down "
                        f"({cycle_start_score:.2f} → {cycle_end_score:.2f}), continuing..."
                    )

                # Log cycle summary
                loop_end_time = datetime.now()
                loop_duration = (loop_end_time - loop_start_time).total_seconds()
                successful_operations = sum(
                    1
                    for result in operation_results.values()
                    if isinstance(result, dict) and result.get("improved", False)
                )

                logging.info(
                    f"--- Loop {improvement_loop_count} completado en {loop_duration:.2f}s. "
                    f"Operaciones exitosas: {successful_operations}/{len(operation_results)} ---"
                )

                if not overall_improvement_made:
                    logging.info("No se detectaron mejoras adicionales. Finalizando fase de mejora.")

            # Rollback to best checkpoint if final state is worse
            final_score = self.metrics.calculate_overall_schedule_score()
            if best_loop_state and final_score < best_loop_score - 0.001:
                logging.info(
                    f"🔄 Restaurando mejor checkpoint: {final_score:.2f} → {best_loop_score:.2f} "
                    f"(+{best_loop_score - final_score:.2f})"
                )
                self.scheduler.schedule = best_loop_state["schedule"]
                self.scheduler.worker_assignments = best_loop_state["assignments"]
                self.scheduler.worker_shift_counts = best_loop_state["counts"]
                self.scheduler.worker_weekend_counts = best_loop_state["weekend_counts"]
                self.scheduler.worker_posts = best_loop_state["posts"]
                self.scheduler.schedule_builder._locked_mandatory = best_loop_state["locked_mandatory"]
                self._sync_builder_references()

            # Phase 3.5: Advanced distribution engine as final push
            logging.info("\n" + "=" * 80)
            logging.info("Phase 3.5: Advanced Distribution Engine - Final Push")
            logging.info("=" * 80)

            if self.advanced_engine:
                try:
                    empty_before_advanced = self.metrics.count_empty_shifts()
                    logging.info(f"Empty slots before advanced engine: {empty_before_advanced}")

                    # Run advanced distribution engine
                    self.advanced_engine.enhanced_fill_schedule(max_iterations=150)

                    empty_after_advanced = self.metrics.count_empty_shifts()
                    improvement = empty_before_advanced - empty_after_advanced

                    logging.info(f"Empty slots after advanced engine: {empty_after_advanced}")
                    logging.info(f"Improvement from advanced engine: +{improvement} slots filled")

                except Exception as e:
                    logging.error(f"Error during advanced distribution engine phase: {e}", exc_info=True)
            else:
                logging.warning("Advanced Distribution Engine not available")

            # Phase 3.6: Strict Balance Optimization
            logging.info("\n" + "=" * 80)
            logging.info("Phase 3.6: Strict Balance Optimization")
            logging.info("=" * 80)

            if self.balance_optimizer:
                try:
                    # Aplicar optimización de balance estricto
                    # Primero intentamos con tolerancia ±1
                    balance_achieved = self.balance_optimizer.optimize_balance(
                        max_iterations=300,
                        target_tolerance=1,  # ±1 turno máximo
                    )

                    # Si no se logró, intentar con múltiples pasadas
                    if not balance_achieved:
                        logging.info("🔄 Running second balance pass with relaxed constraints...")
                        balance_achieved = self.balance_optimizer.optimize_balance(
                            max_iterations=250,
                            target_tolerance=2,  # Aceptar ±2 temporalmente
                        )
                        # Luego intentar ajustar a ±1 de nuevo
                        if balance_achieved:
                            self.balance_optimizer.optimize_balance(max_iterations=200, target_tolerance=1)

                    if balance_achieved:
                        logging.info("✅ Perfect balance achieved: All workers within ±1 shift of target")
                    else:
                        logging.warning("⚠️ Some workers still outside ±1 tolerance")

                except Exception as e:
                    logging.error(f"Error during strict balance optimization: {e}", exc_info=True)
            else:
                logging.warning("Strict Balance Optimizer not available")

            # Final summary
            if improvement_loop_count >= max_improvement_loops:
                termination_reason = f"Límite de iteraciones alcanzado ({max_improvement_loops})"
                logging.warning(termination_reason)
            else:
                termination_reason = "Convergencia alcanzada"

            # Display final optimization summary
            self.progress_monitor.display_optimization_summary(
                improvement_loop_count, current_overall_score, termination_reason
            )

            # Get performance insights for logging
            insights = self.progress_monitor.get_performance_insights()
            if not insights.get("error"):
                logging.info(
                    f"📊 Insights finales: {insights['significant_improvements']} mejoras significativas, "
                    f"tasa de éxito: {insights['average_operations_success_rate']:.2f}"
                )

            return True

        except Exception as e:
            logging.error(f"Error during enhanced iterative improvement phase: {e!s}", exc_info=True)
            return False

    def _finalization_phase(self) -> bool:
        """
        Phase 4: Finalize the schedule and perform final optimizations.

        IMPORTANT: This phase now saves the pre-finalization state and compares
        the result to avoid degradation (Fase 1 vs Fase 2 problem).

        Returns:
            bool: True if finalization was successful
        """
        logging.info("Phase 4: Finalizing schedule...")

        try:
            # ================================================================
            # CRITICAL: Save pre-finalization state to compare later
            # This addresses the issue where Phase 2 was degrading results
            # ================================================================
            pre_finalization_state = {
                "schedule": copy.deepcopy(self.scheduler.schedule),
                "assignments": copy.deepcopy(self.scheduler.worker_assignments),
                "counts": copy.deepcopy(self.scheduler.worker_shift_counts),
                "weekend_counts": copy.deepcopy(self.scheduler.worker_weekend_counts),
                "posts": copy.deepcopy(self.scheduler.worker_posts),
                "locked_mandatory": copy.deepcopy(self.scheduler.schedule_builder._locked_mandatory),
            }

            # Calculate pre-finalization metrics
            pre_score = self.metrics.calculate_overall_schedule_score()
            pre_workload_imbalance = self.metrics.calculate_workload_imbalance()
            pre_weekend_imbalance = self.metrics.calculate_weekend_imbalance()

            logging.info("📊 Pre-finalization metrics:")
            logging.info(f"   Score: {pre_score:.2f}")
            logging.info(f"   Workload Imbalance: {pre_workload_imbalance:.2f}")
            logging.info(f"   Weekend Imbalance: {pre_weekend_imbalance:.2f}")

            # Final adjustment of last post distribution
            logging.info("Performing final last post distribution adjustment...")
            max_iterations = self.config.get(
                "last_post_adjustment_max_iterations", SchedulerConfig.DEFAULT_LAST_POST_ADJUSTMENT_ITERATIONS
            )

            if self.scheduler.schedule_builder._adjust_last_post_distribution(
                balance_tolerance=1.0, max_iterations=max_iterations
            ):
                logging.info("Final last post distribution adjustment completed.")

            # PASADA EXTRA: Llenar huecos y balancear tras el ajuste final
            logging.info("Extra pass: Filling empty shifts and balancing workloads after last post adjustment...")
            self.scheduler.schedule_builder._try_fill_empty_shifts()
            self.scheduler.schedule_builder._balance_workloads()
            self.scheduler.schedule_builder._balance_weekday_distribution()

            # Iterar hasta que todos los trabajadores estén dentro de la tolerancia ±1 en turnos y last posts
            max_final_balance_loops = 40
            for i in range(max_final_balance_loops):
                logging.info(f"Final strict balance loop {i + 1}/{max_final_balance_loops}")
                changed1 = self.scheduler.schedule_builder._balance_workloads()
                changed2 = self.scheduler.schedule_builder._adjust_last_post_distribution(
                    balance_tolerance=1.0, max_iterations=20
                )
                changed3 = self.scheduler.schedule_builder._balance_weekday_distribution()
                changed4 = self.scheduler.schedule_builder._balance_monthly_distribution()
                # Rebalance weekend distribution — corrects imbalances introduced by workload/monthly swaps.
                changed5 = False
                try:
                    changed5 = self.scheduler.schedule_builder._improve_weekend_distribution()
                except Exception as _we:
                    logging.debug(f"Weekend balance in loop {i + 1} skipped: {_we}")
                # Also rebalance bridge distribution each loop iteration so that
                # workload/monthly changes don't accumulate bridge imbalance.
                changed6 = False
                try:
                    if (
                        hasattr(self.scheduler, "bridge_periods")
                        and self.scheduler.bridge_periods
                        and hasattr(self.scheduler, "schedule_builder")
                    ):
                        changed6 = self.scheduler.schedule_builder._distribute_bridge_shifts_proportionally()
                except Exception as _be:
                    logging.debug(f"Bridge balance in loop {i + 1} skipped: {_be}")
                if not changed1 and not changed2 and not changed3 and not changed4 and not changed5 and not changed6:
                    logging.info(f"Balance achieved after {i + 1} iterations")
                    break
            else:
                logging.warning(f"Max balance iterations ({max_final_balance_loops}) reached")

            # FASE FINAL: Balance estricto de turnos con el optimizador avanzado
            if self.balance_optimizer:
                logging.info("\n" + "=" * 80)
                logging.info("FINAL PHASE: Strict Balance Optimization (Post-Finalization)")
                logging.info("=" * 80)
                try:
                    # Ejecutar balance estricto final con más iteraciones
                    final_balance = self.balance_optimizer.optimize_balance(max_iterations=250, target_tolerance=1)
                    if final_balance:
                        logging.info("✅ Final strict balance achieved")
                    else:
                        # Intentar con tolerancia más flexible
                        logging.info("🔄 Retrying with tolerance ±2...")
                        self.balance_optimizer.optimize_balance(max_iterations=180, target_tolerance=2)
                except Exception as e:
                    logging.error(f"Error in final balance optimization: {e}", exc_info=True)

                # CRITICAL: Rebalance last posts after balance optimization
                # Balance optimizer swaps can disrupt last post distribution
                logging.info("Rebalancing last post distribution after balance optimization...")
                self.scheduler.schedule_builder._adjust_last_post_distribution(balance_tolerance=1.0, max_iterations=30)

            # NOTE: Monthly balance pass removed here — it now runs as the
            # ABSOLUTE LAST STEP after bridge rebalancing (see below).

            # ================================================================
            # CRITICAL: Compare post-finalization with pre-finalization
            # If finalization degraded the schedule, RESTORE pre-finalization state
            # ================================================================
            post_score = self.metrics.calculate_overall_schedule_score()
            post_workload_imbalance = self.metrics.calculate_workload_imbalance()
            post_weekend_imbalance = self.metrics.calculate_weekend_imbalance()

            logging.info("\n📊 Post-finalization metrics:")
            logging.info(f"   Score: {post_score:.2f} (was {pre_score:.2f}, diff: {post_score - pre_score:+.2f})")
            logging.info(f"   Workload Imbalance: {post_workload_imbalance:.2f} (was {pre_workload_imbalance:.2f})")
            logging.info(f"   Weekend Imbalance: {post_weekend_imbalance:.2f} (was {pre_weekend_imbalance:.2f})")

            # Decision: Use post-finalization ONLY if it's better or equal
            # Weight: workload imbalance is critical, weekend imbalance secondary
            pre_composite = pre_workload_imbalance + (pre_weekend_imbalance * 0.5)
            post_composite = post_workload_imbalance + (post_weekend_imbalance * 0.5)

            # Allow small degradation (0.5) but reject significant degradation
            if post_composite > pre_composite + 0.5:
                logging.warning("⚠️ FINALIZATION DEGRADED THE SCHEDULE!")
                logging.warning(f"   Pre-finalization composite: {pre_composite:.2f}")
                logging.warning(f"   Post-finalization composite: {post_composite:.2f}")
                logging.warning("   RESTORING pre-finalization state...")

                # Restore pre-finalization state
                self.scheduler.schedule = pre_finalization_state["schedule"]
                self.scheduler.worker_assignments = pre_finalization_state["assignments"]
                self.scheduler.worker_shift_counts = pre_finalization_state["counts"]
                self.scheduler.worker_weekend_counts = pre_finalization_state["weekend_counts"]
                self.scheduler.worker_posts = pre_finalization_state["posts"]
                self.scheduler.schedule_builder._locked_mandatory = pre_finalization_state["locked_mandatory"]
                # Sync ALL schedule_builder AND constraint_checker references
                self._sync_builder_references()

                logging.info("✅ Pre-finalization state restored successfully")
            else:
                logging.info("✅ Finalization improved or maintained schedule quality")

            # Get the best schedule
            final_schedule_data = self.scheduler.schedule_builder.get_best_schedule()

            if not final_schedule_data or not final_schedule_data.get("schedule"):
                logging.error("No best schedule data available for finalization.")
                return self._handle_fallback_finalization()

            # Update scheduler state with final schedule
            self._apply_final_schedule(final_schedule_data)

            # --- Post-finalization monthly distribution balance ---
            # Must run BEFORE bridge so that the bridge rebalancing (below) can
            # correct any disruption that the monthly pass introduces.
            logging.info("Running FINAL monthly distribution balance pass...")
            for _pass in range(5):
                if not self.scheduler.schedule_builder._balance_monthly_distribution():
                    logging.info(f"Monthly distribution converged after {_pass + 1} pass(es)")
                    break

            # --- Bridge rebalancing (after monthly) ---
            try:
                if (
                    hasattr(self.scheduler, "bridge_periods")
                    and self.scheduler.bridge_periods
                    and hasattr(self.scheduler, "schedule_builder")
                ):
                    logging.info("🌉 Running FINAL bridge rebalancing...")
                    for pass_num in range(5):
                        changed = self.scheduler.schedule_builder._distribute_bridge_shifts_proportionally()
                        if not changed:
                            logging.info(f"🌉 Bridge rebalancing converged after {pass_num + 1} pass(es)")
                            break
                    else:
                        logging.info("🌉 Bridge rebalancing completed all 5 passes")
            except Exception as e:
                logging.warning(f"Bridge rebalancing (final) failed: {e}")

            # --- ABSOLUTE LAST STEP: Weekend rebalancing ---
            # Must run AFTER monthly and bridge — both can disrupt weekend balance
            # with no subsequent correction otherwise.
            logging.info("🏖️ Running FINAL weekend distribution balance (absolute last step)...")
            try:
                for _wpass in range(5):
                    if not self.scheduler.schedule_builder._improve_weekend_distribution():
                        logging.info(f"🏖️ Weekend distribution converged after {_wpass + 1} pass(es)")
                        break
                else:
                    logging.info("🏖️ Weekend distribution completed all 5 passes")
            except Exception as e:
                logging.warning(f"Weekend rebalancing (final) failed: {e}")

            # Final validation y logging
            self._perform_final_validation()

            # --- ABSOLUTE LAST STEP: Targeted weekend strategies 5 & 6 ---
            # _perform_final_validation → _apply_tolerance_optimization can
            # degrade weekend balance.  This corrective pass applies
            # weekend↔weekday rotations and 3-way chain swaps targeting the
            # workers with the highest weekend deviation, with full rollback
            # if the overall weekend balance worsens.
            try:
                self._targeted_weekend_balance_pass()
            except Exception as e:
                logging.warning(f"Targeted weekend balance pass failed: {e}")

            logging.info("Schedule finalization phase completed successfully.")
            return True

        except Exception as e:
            logging.error(f"Error during finalization phase: {e!s}", exc_info=True)
            return False

    def _handle_fallback_finalization(self) -> bool:
        """
        Handle fallback finalization when no best schedule is available.

        Returns:
            bool: True if fallback was successful
        """
        logging.warning("Using current schedule state as fallback for finalization.")

        if not self.scheduler.schedule or all(
            all(p is None for p in posts) for posts in self.scheduler.schedule.values()
        ):
            logging.error("Current schedule state is also empty. Cannot finalize.")
            return False

        # Use current state as final schedule
        final_schedule_data = {
            "schedule": self.scheduler.schedule,
            "worker_assignments": self.scheduler.worker_assignments,
            "worker_shift_counts": self.scheduler.worker_shift_counts,
            "worker_weekend_counts": self.scheduler.worker_weekend_counts,
            "worker_posts": self.scheduler.worker_posts,
            "last_assignment_date": self.scheduler.last_assignment_date,
            "consecutive_shifts": self.scheduler.consecutive_shifts,
            "score": self.scheduler.calculate_score(),
        }

        return self._apply_final_schedule(final_schedule_data)

    def _apply_final_schedule(self, final_schedule_data: dict[str, Any]) -> bool:
        """
        Apply the final schedule data to the scheduler state.

        Args:
            final_schedule_data: Dictionary containing the final schedule data

        Returns:
            bool: True if application was successful
        """
        try:
            logging.info("Applying final schedule data to scheduler state...")

            self.scheduler.schedule = final_schedule_data["schedule"]
            self.scheduler.worker_assignments = final_schedule_data["worker_assignments"]
            self.scheduler.worker_shift_counts = final_schedule_data["worker_shift_counts"]
            self.scheduler.worker_weekend_counts = final_schedule_data.get("worker_weekend_counts", {})
            self.scheduler.worker_posts = final_schedule_data["worker_posts"]
            self.scheduler.last_assignment_date = final_schedule_data["last_assignment_date"]
            self.scheduler.consecutive_shifts = final_schedule_data["consecutive_shifts"]

            final_score = final_schedule_data.get("score", float("-inf"))
            logging.info(f"Final schedule applied with score: {final_score:.2f}")

            # CRITICAL: Sync builder references so schedule_builder and
            # constraint_checker point to the new objects (not stale pre-apply ones)
            self._sync_builder_references()

            return True

        except Exception as e:
            logging.error(f"Error applying final schedule data: {e!s}", exc_info=True)
            return False

    def _perform_final_validation(self) -> bool:
        """
        Perform final validation and logging of the schedule.

        Returns:
            bool: True if validation passed
        """
        try:
            # Calculate final statistics
            total_slots_final = sum(len(slots) for slots in self.scheduler.schedule.values())
            total_assignments_final = sum(
                1 for slots in self.scheduler.schedule.values() for worker_id in slots if worker_id is not None
            )

            empty_shifts_final = [
                (date, post_idx)
                for date, posts in self.scheduler.schedule.items()
                for post_idx, worker_id in enumerate(posts)
                if worker_id is None
            ]

            # Validate schedule integrity
            if total_slots_final == 0:
                schedule_duration_days = (self.end_date - self.start_date).days + 1
                if schedule_duration_days > 0:
                    logging.error(
                        f"Final schedule has 0 total slots despite valid date range ({schedule_duration_days} days)."
                    )
                    return False

            if total_assignments_final == 0 and total_slots_final > 0:
                logging.warning(f"Final schedule has {total_slots_final} slots but contains ZERO assignments.")

            if empty_shifts_final:
                empty_percentage = (len(empty_shifts_final) / total_slots_final) * 100
                logging.warning(
                    f"Final schedule has {len(empty_shifts_final)} empty shifts ({empty_percentage:.1f}%) out of {total_slots_final} total slots."
                )

            # Log final summary
            self.scheduler.log_schedule_summary("Final Generated Schedule")

            # Apply iterative tolerance optimization to meet ±10% objective requirements
            logging.info("=" * 80)
            logging.info("APPLYING ITERATIVE TOLERANCE OPTIMIZATION")
            logging.info("=" * 80)
            self._apply_tolerance_optimization()

            return True

        except Exception as e:
            logging.error(f"Error during final validation: {e!s}", exc_info=True)
            return False

    def _apply_tolerance_optimization(self):
        """
        Apply iterative optimization to meet ±10% objective tolerance requirements.

        This method uses the IterativeOptimizer to automatically correct tolerance violations
        by redistributing shifts between workers who are over/under their target allocations.
        """
        try:
            # CRITICAL: Ensure validator's schedule reference is current before counting
            self.tolerance_validator.schedule = self.scheduler.schedule

            # Check initial tolerance violations
            outside_general = self.tolerance_validator.get_workers_outside_tolerance(is_weekend_only=False)
            outside_weekend = self.tolerance_validator.get_workers_outside_tolerance(is_weekend_only=True)

            total_violations = len(outside_general) + len(outside_weekend)

            logging.info("Initial tolerance check:")
            logging.info(f"  Workers outside ±10% objective (general): {len(outside_general)}")
            logging.info(f"  Workers outside ±10% objective (weekend): {len(outside_weekend)}")
            logging.info(f"  Total violations: {total_violations}")

            if total_violations == 0:
                logging.info("✅ All workers already within ±10% objective!")
                return

            # Log violations details
            if outside_general:
                logging.info("General shift violations:")
                for violation in outside_general[:5]:  # Show first 5
                    logging.info(
                        f"  - {violation['worker_id']}: {violation['assigned_shifts']} assigned "
                        f"(target: {violation['target_shifts']}, "
                        f"deviation: {violation['deviation_percentage']:+.1f}%)"
                    )

            if outside_weekend:
                logging.info("Weekend shift violations:")
                for violation in outside_weekend[:5]:  # Show first 5
                    logging.info(
                        f"  - {violation['worker_id']}: {violation['assigned_shifts']} assigned "
                        f"(target: {violation['target_shifts']}, "
                        f"deviation: {violation['deviation_percentage']:+.1f}%)"
                    )

            # CRITICAL: Switch to RELAXED MODE for iterative optimization
            self.scheduler.schedule_builder.enable_relaxed_mode()
            logging.info("🔓 RELAXED MODE activated for iterative optimization phase")
            logging.info("   - Target tolerance: ±10% objective (Phase 1) or ±12% limit (Phase 2 if needed)")
            logging.info("   - ABSOLUTE LIMIT: ±12% NEVER exceeded")
            logging.info("   - Gap reduction: -1 ONLY (with deficit ≥3)")
            logging.info("   - Pattern 7/14: Allows violation if deficit >10% of target")
            logging.info("   - Balance tolerance: ±10% for guardias/mes, weekends")
            logging.info("   - Mandatory/Incompatibilities/Days off: ALWAYS respected")

            # Apply iterative optimization
            logging.info(
                f"Starting iterative optimization (max {self.iterative_optimizer.max_iterations} iterations)..."
            )

            optimization_result = self.iterative_optimizer.optimize_schedule(
                scheduler_core=self,
                schedule=self.scheduler.schedule,
                workers_data=self.workers_data,
                schedule_config=self.config,
            )

            # Check if optimization improved the schedule
            # CRITICAL: Always try the best result, not just if "success"
            if optimization_result.schedule:
                # Apply optimized schedule temporarily to check
                original_schedule = self.scheduler.schedule
                self.scheduler.schedule = optimization_result.schedule

                # CRITICAL FIX: Sync tracking data AND update validator schedule
                # reference BEFORE validation. Without this, the validator reads
                # stale worker_assignments from the original schedule and always
                # reports the same violation count, causing the code to blindly
                # keep the optimizer's (possibly worse) result.
                self.scheduler._synchronize_tracking_data()
                self.tolerance_validator.schedule = self.scheduler.schedule

                # Verify tolerance after optimization
                new_outside_general = self.tolerance_validator.get_workers_outside_tolerance(is_weekend_only=False)
                new_outside_weekend = self.tolerance_validator.get_workers_outside_tolerance(is_weekend_only=True)

                new_total_violations = len(new_outside_general) + len(new_outside_weekend)

                logging.info(f"Optimization completed after {optimization_result.iteration} iterations")
                logging.info(f"Result: {total_violations} → {new_total_violations} violations")

                if new_total_violations < total_violations:
                    improvement = total_violations - new_total_violations
                    improvement_pct = (improvement / total_violations * 100) if total_violations > 0 else 0
                    logging.info(
                        f"✅ Optimization IMPROVED schedule! Reduced {improvement} violations ({improvement_pct:.1f}% improvement)"
                    )
                    logging.info(f"  General: {len(outside_general)} → {len(new_outside_general)}")
                    logging.info(f"  Weekend: {len(outside_weekend)} → {len(new_outside_weekend)}")

                    # Keep optimized schedule (already applied and synced)
                    logging.info("Optimized schedule applied successfully")

                elif new_total_violations == total_violations:
                    logging.info(f"📊 Optimization maintained violations at {total_violations} (no change)")
                    # Keep it anyway - at least redistributions may have balanced better
                    logging.info("Keeping optimized schedule (same violations but may be better balanced)")

                else:
                    regression = new_total_violations - total_violations
                    logging.warning(
                        f"⚠️ Optimization WORSENED schedule: +{regression} violations ({total_violations} → {new_total_violations})"
                    )
                    logging.info("Reverting to original schedule")
                    self.scheduler.schedule = original_schedule
                    # CRITICAL FIX: Re-sync tracking data after revert
                    self.scheduler._synchronize_tracking_data()
                    self.tolerance_validator.schedule = self.scheduler.schedule
            else:
                logging.warning("No optimized schedule available from optimizer")
                logging.info("Keeping original schedule")

            # Final tolerance report
            final_outside_general = self.tolerance_validator.get_workers_outside_tolerance(is_weekend_only=False)
            final_outside_weekend = self.tolerance_validator.get_workers_outside_tolerance(is_weekend_only=True)

            final_total = len(final_outside_general) + len(final_outside_weekend)

            logging.info("=" * 80)
            logging.info("TOLERANCE OPTIMIZATION COMPLETE")
            logging.info(f"Final violations: {final_total}")
            logging.info(f"  General: {len(final_outside_general)}")
            logging.info(f"  Weekend: {len(final_outside_weekend)}")

            if final_total == 0:
                logging.info("🎯 SUCCESS: All workers within ±10% objective!")
            else:
                logging.warning(f"⚠️  {final_total} workers still outside tolerance")
            logging.info("=" * 80)

        except Exception as e:
            logging.error(f"Error during tolerance optimization: {e}", exc_info=True)
            logging.info("Continuing with original schedule")

    def _targeted_weekend_balance_pass(self) -> bool:
        """Apply strategies 5 (weekend↔weekday rotation) and 6 (3-way chain)
        from the IterativeOptimizer as a final corrective pass, targeting
        the workers with the highest weekend deviation.

        This runs AFTER ``_perform_final_validation`` (which may degrade
        weekend balance via ``_apply_tolerance_optimization``) and corrects
        any residual weekend imbalance without touching total shift counts.

        Returns:
            True if any improvement was made, False otherwise.
        """
        try:
            logging.info("=" * 80)
            logging.info("🏖️ TARGETED WEEKEND BALANCE PASS (strategies 5 & 6)")
            logging.info("=" * 80)

            # --- snapshot pre-pass weekend violations for rollback ---
            self.tolerance_validator.schedule = self.scheduler.schedule
            pre_violations = self.tolerance_validator.get_workers_outside_tolerance(is_weekend_only=True)
            pre_total_dev = sum(abs(v.get("deviation_percentage", 0)) for v in pre_violations)
            pre_count = len(pre_violations)
            logging.info(f"Pre-pass weekend state: {pre_count} violations, total |deviation|={pre_total_dev:.1f}%")

            if pre_count == 0:
                logging.info("✅ No weekend violations — skipping targeted pass")
                return False

            # Save a deep-copy checkpoint for rollback
            checkpoint_schedule = copy.deepcopy(self.scheduler.schedule)

            any_improvement = False
            max_iterations = 8

            for iteration in range(max_iterations):
                # Build validation_report in the format strategies 5 & 6 expect
                self.tolerance_validator.schedule = self.scheduler.schedule
                weekend_outside = self.tolerance_validator.get_workers_outside_tolerance(is_weekend_only=True)

                if not weekend_outside:
                    logging.info(f"🏖️ All weekend violations resolved after {iteration} iteration(s)")
                    break

                # Sort by absolute deviation descending (worst offenders first)
                weekend_outside.sort(
                    key=lambda v: abs(v.get("deviation_percentage", 0)),
                    reverse=True,
                )

                violations_list = []
                for w in weekend_outside:
                    wid = w.get("worker_id", "Unknown")
                    assigned = w.get("assigned_shifts", 0)
                    target = w.get("target_shifts", 0)
                    diff = assigned - target
                    violations_list.append(
                        {
                            "worker": str(wid),
                            "deviation_percentage": w.get("deviation_percentage", 0),
                            "shortage": max(0, -diff),
                            "excess": max(0, diff),
                        }
                    )

                validation_report = {
                    "weekend_shift_violations": violations_list,
                    "general_shift_violations": [],
                    "total_violations": len(violations_list),
                }

                logging.info(f"🏖️ Iteration {iteration + 1}/{max_iterations}: {len(violations_list)} weekend violations")

                schedule_before = copy.deepcopy(self.scheduler.schedule)

                # --- Strategy 5: weekend↔weekday rotation ---
                modified_schedule = self.iterative_optimizer._apply_weekend_weekday_rotation(
                    copy.deepcopy(self.scheduler.schedule),
                    validation_report,
                    self.workers_data,
                    self.config,
                )

                # Rebuild report after strategy 5 for strategy 6
                self.scheduler.schedule = modified_schedule
                self.scheduler._synchronize_tracking_data()
                self.tolerance_validator.schedule = self.scheduler.schedule

                weekend_outside_mid = self.tolerance_validator.get_workers_outside_tolerance(is_weekend_only=True)
                mid_violations = []
                for w in weekend_outside_mid:
                    wid = w.get("worker_id", "Unknown")
                    assigned = w.get("assigned_shifts", 0)
                    target = w.get("target_shifts", 0)
                    diff = assigned - target
                    mid_violations.append(
                        {
                            "worker": str(wid),
                            "deviation_percentage": w.get("deviation_percentage", 0),
                            "shortage": max(0, -diff),
                            "excess": max(0, diff),
                        }
                    )
                mid_report = {
                    "weekend_shift_violations": mid_violations,
                    "general_shift_violations": [],
                    "total_violations": len(mid_violations),
                }

                # --- Strategy 6: 3-way chain rotation ---
                if mid_violations:
                    modified_schedule = self.iterative_optimizer._apply_chain_weekend_rotation(
                        copy.deepcopy(self.scheduler.schedule),
                        mid_report,
                        self.workers_data,
                        self.config,
                    )
                    self.scheduler.schedule = modified_schedule
                    self.scheduler._synchronize_tracking_data()

                # --- Strategy 7+: Weekend pull for one-sided under-only ---
                # When only UNDER workers exist (no OVER), strategies 5 & 6
                # can't help.  Pull a weekend from any non-violating donor
                # who can absorb a weekday in return.
                self.tolerance_validator.schedule = self.scheduler.schedule
                remaining_outside = self.tolerance_validator.get_workers_outside_tolerance(is_weekend_only=True)
                under_only = [v for v in remaining_outside if v.get("deviation_percentage", 0) < 0]
                over_any = [v for v in remaining_outside if v.get("deviation_percentage", 0) > 0]
                if under_only and not over_any:
                    modified_schedule = self._weekend_pull_from_donors(self.scheduler.schedule, under_only)
                    self.scheduler.schedule = modified_schedule
                    self.scheduler._synchronize_tracking_data()

                # Check if this iteration improved anything
                if self.scheduler.schedule == schedule_before:
                    logging.info(f"🏖️ No changes in iteration {iteration + 1} — converged")
                    # Diagnose remaining blockers if violations persist
                    self.tolerance_validator.schedule = self.scheduler.schedule
                    remaining = self.tolerance_validator.get_workers_outside_tolerance(is_weekend_only=True)
                    if remaining:
                        self._diagnose_weekend_swap_blockers()
                    break

                any_improvement = True

            # --- Post-pass validation: rollback if weekend balance worsened ---
            self.tolerance_validator.schedule = self.scheduler.schedule
            post_violations = self.tolerance_validator.get_workers_outside_tolerance(is_weekend_only=True)
            post_total_dev = sum(abs(v.get("deviation_percentage", 0)) for v in post_violations)
            post_count = len(post_violations)

            logging.info(f"Post-pass weekend state: {post_count} violations, total |deviation|={post_total_dev:.1f}%")

            if post_total_dev > pre_total_dev:
                logging.warning("⚠️ Targeted weekend pass WORSENED balance — rolling back")
                self.scheduler.schedule = checkpoint_schedule
                self.scheduler._synchronize_tracking_data()
                self._sync_builder_references()
                return False

            if any_improvement:
                self._sync_builder_references()
                logging.info(
                    f"✅ Targeted weekend pass: {pre_count}→{post_count} violations, "
                    f"|dev| {pre_total_dev:.1f}%→{post_total_dev:.1f}%"
                )

            logging.info("=" * 80)
            return any_improvement

        except Exception as e:
            logging.error(f"Error during targeted weekend balance pass: {e}", exc_info=True)
            return False

    # ------------------------------------------------------------------
    # Diagnostic helper for the targeted weekend balance pass
    # ------------------------------------------------------------------
    def _diagnose_weekend_swap_blockers(self) -> None:
        """Log why strategies 5 & 6 couldn't resolve remaining weekend violations.

        For each over↔under pair, counts candidate shift slots and checks
        how many combinations pass ``_can_worker_take_shift`` (with the
        vacated-slot fix applied).  Logs a summary per pair.
        """
        try:
            from datetime import timedelta

            schedule = self.scheduler.schedule
            workers_data = self.workers_data
            holidays = set(getattr(self.scheduler, "holidays", [])) if self.scheduler else set()

            violations = self.tolerance_validator.get_workers_outside_tolerance(is_weekend_only=True)
            if not violations:
                return

            over_workers = [v for v in violations if v.get("deviation_percentage", 0) > 0]
            under_workers = [v for v in violations if v.get("deviation_percentage", 0) < 0]

            logging.info("  📊 SWAP BLOCKER DIAGNOSTIC")
            for v in over_workers:
                logging.info(
                    f"     OVER  {v.get('worker_id')}: "
                    f"assigned={v.get('assigned_shifts')}, "
                    f"target={v.get('target_shifts')}, "
                    f"dev={v.get('deviation_percentage', 0):+.1f}%"
                )
            for v in under_workers:
                logging.info(
                    f"     UNDER {v.get('worker_id')}: "
                    f"assigned={v.get('assigned_shifts')}, "
                    f"target={v.get('target_shifts')}, "
                    f"dev={v.get('deviation_percentage', 0):+.1f}%"
                )

            if not over_workers or not under_workers:
                logging.info("     ⚠️  One-sided violations only — Strategy 5 requires both over & under workers")
                return

            # Classify dates
            weekend_dates: set = set()
            weekday_dates: set = set()
            for dk in schedule:
                try:
                    d = dk if isinstance(dk, datetime) else datetime.strptime(dk, "%Y-%m-%d")
                    if d.weekday() >= 4 or d in holidays or (d + timedelta(days=1)) in holidays:
                        weekend_dates.add(dk)
                    else:
                        weekday_dates.add(dk)
                except (ValueError, AttributeError):
                    continue

            opt = self.iterative_optimizer
            can_check = opt._can_worker_take_shift
            is_mandatory = opt._is_mandatory_shift

            for over_v in over_workers:
                over_w = str(over_v.get("worker_id", ""))
                we_slots = []
                for dk in weekend_dates:
                    assigns = schedule.get(dk, [])
                    if isinstance(assigns, list):
                        for idx, w in enumerate(assigns):
                            if w == over_w and not is_mandatory(over_w, dk, workers_data):
                                we_slots.append((dk, idx))

                logging.info(f"     {over_w}: {len(we_slots)} swappable weekend slots")

                for under_v in under_workers:
                    under_w = str(under_v.get("worker_id", ""))
                    wd_slots = []
                    for dk in weekday_dates:
                        assigns = schedule.get(dk, [])
                        if isinstance(assigns, list):
                            for idx, w in enumerate(assigns):
                                if w == under_w and not is_mandatory(under_w, dk, workers_data):
                                    wd_slots.append((dk, idx))

                    total = len(we_slots) * len(wd_slots)
                    both_ok = 0
                    over_blocked = 0
                    under_blocked = 0
                    both_blocked = 0
                    checked = 0
                    max_check = 300

                    for we_dk, we_idx in we_slots:
                        if checked >= max_check:
                            break
                        for wd_dk, wd_idx in wd_slots:
                            if checked >= max_check:
                                break

                            # Vacate both slots (mirrors the fix in strategy 5)
                            orig_we = schedule[we_dk][we_idx]
                            orig_wd = schedule[wd_dk][wd_idx]
                            schedule[we_dk][we_idx] = None
                            schedule[wd_dk][wd_idx] = None

                            ok_over = can_check(
                                over_w,
                                wd_dk,
                                f"Post_{wd_idx}",
                                schedule,
                                workers_data,
                            )
                            ok_under = can_check(
                                under_w,
                                we_dk,
                                f"Post_{we_idx}",
                                schedule,
                                workers_data,
                            )

                            schedule[we_dk][we_idx] = orig_we
                            schedule[wd_dk][wd_idx] = orig_wd

                            if ok_over and ok_under:
                                both_ok += 1
                            elif not ok_over and not ok_under:
                                both_blocked += 1
                            elif not ok_over:
                                over_blocked += 1
                            else:
                                under_blocked += 1
                            checked += 1

                    logging.info(
                        f"       ↔ {under_w}: {len(wd_slots)} wd-slots, "
                        f"{checked}/{total} combos checked | "
                        f"both_ok={both_ok}, "
                        f"over_blocked={over_blocked}, "
                        f"under_blocked={under_blocked}, "
                        f"both_blocked={both_blocked}"
                    )

        except Exception as e:
            logging.error(f"Error in swap blocker diagnostic: {e}", exc_info=True)

    # ------------------------------------------------------------------
    # Weekend pull from non-violating donors (one-sided under-only fix)
    # ------------------------------------------------------------------
    def _weekend_pull_from_donors(self, schedule: dict, under_violations: list[dict]) -> dict:
        """Swap a weekend shift from a non-violating donor to an under-assigned
        worker, giving the donor the under-worker's weekday shift in return.

        This handles the case where only UNDER workers exist — no one is OVER
        tolerance, so strategies 5/6 can't pair them.  Any worker whose weekend
        count is above their target (even within tolerance) is a candidate donor.

        The swap is structurally identical to Strategy 5 but the donor is chosen
        from the general pool, not from violators.
        """
        from datetime import timedelta

        logging.info("   🎯 Weekend pull: recruiting non-violating donors")

        holidays = set(getattr(self.scheduler, "holidays", [])) if self.scheduler else set()
        opt = self.iterative_optimizer
        workers_data = self.workers_data
        can_check = opt._can_worker_take_shift
        is_mandatory = opt._is_mandatory_shift

        optimized = copy.deepcopy(schedule)

        # Classify dates
        weekend_dates: set = set()
        weekday_dates: set = set()
        for dk in optimized:
            try:
                d = dk if isinstance(dk, datetime) else datetime.strptime(dk, "%Y-%m-%d")
                if d.weekday() >= 4 or d in holidays or (d + timedelta(days=1)) in holidays:
                    weekend_dates.add(dk)
                else:
                    weekday_dates.add(dk)
            except (ValueError, AttributeError):
                continue

        # Build per-worker weekend counts from current schedule
        worker_we_counts: dict[str, int] = {}
        for dk in weekend_dates:
            assigns = optimized.get(dk, [])
            if isinstance(assigns, list):
                for w in assigns:
                    if w is not None:
                        worker_we_counts[w] = worker_we_counts.get(w, 0) + 1

        # Weekend targets per worker (proportional)
        total_days = len(optimized)
        total_we_days = len(weekend_dates)
        we_ratio = total_we_days / total_days if total_days else 0.0

        worker_we_target: dict[str, float] = {}
        for wd in workers_data:
            wname = str(wd.get("id", wd.get("name", "")))
            raw_target = wd.get("_raw_target", wd.get("target_shifts", 0))
            worker_we_target[wname] = raw_target * we_ratio

        # Find donors: workers NOT in violation whose weekend count > their target
        violating_ids = {str(v.get("worker_id", "")) for v in under_violations}
        donors = []
        for wname, we_count in worker_we_counts.items():
            if wname in violating_ids:
                continue
            target = worker_we_target.get(wname, 0)
            if we_count > target:
                donors.append((wname, we_count - target))

        # Sort donors by surplus descending
        donors.sort(key=lambda x: x[1], reverse=True)

        if not donors:
            logging.info("      ℹ️  No donors with weekend surplus found")
            return optimized

        logging.info(f"      Found {len(donors)} potential donors (top surplus: {donors[0][0]} +{donors[0][1]:.1f})")

        swaps_made = 0
        max_swaps = 20

        for v in under_violations:
            if swaps_made >= max_swaps:
                break
            under_w = str(v.get("worker_id", ""))
            shortage = v.get("target_shifts", 0) - v.get("assigned_shifts", 0)
            if shortage <= 0:
                continue

            # Collect under_worker's non-mandatory weekday shifts
            under_wd_slots = []
            for dk in weekday_dates:
                assigns = optimized.get(dk, [])
                if isinstance(assigns, list):
                    for idx, w in enumerate(assigns):
                        if w == under_w and not is_mandatory(under_w, dk, workers_data):
                            under_wd_slots.append((dk, idx))

            if not under_wd_slots:
                logging.info(f"      {under_w}: no weekday slots to offer")
                continue

            import random as _rnd

            _rnd.shuffle(under_wd_slots)

            for donor_name, _ in donors:
                if shortage <= 0 or swaps_made >= max_swaps:
                    break

                # Collect donor's non-mandatory weekend shifts
                donor_we_slots = []
                for dk in weekend_dates:
                    assigns = optimized.get(dk, [])
                    if isinstance(assigns, list):
                        for idx, w in enumerate(assigns):
                            if w == donor_name and not is_mandatory(donor_name, dk, workers_data):
                                donor_we_slots.append((dk, idx))

                if not donor_we_slots:
                    continue
                _rnd.shuffle(donor_we_slots)

                swapped = False
                for we_dk, we_idx in donor_we_slots:
                    if swapped:
                        break
                    for wd_dk, wd_idx in under_wd_slots:
                        # Vacate both slots before constraint check
                        orig_we = optimized[we_dk][we_idx]
                        orig_wd = optimized[wd_dk][wd_idx]
                        optimized[we_dk][we_idx] = None
                        optimized[wd_dk][wd_idx] = None

                        ok_donor = can_check(
                            donor_name,
                            wd_dk,
                            f"Post_{wd_idx}",
                            optimized,
                            workers_data,
                        )
                        ok_under = can_check(
                            under_w,
                            we_dk,
                            f"Post_{we_idx}",
                            optimized,
                            workers_data,
                        )

                        optimized[we_dk][we_idx] = orig_we
                        optimized[wd_dk][wd_idx] = orig_wd

                        if not ok_donor or not ok_under:
                            continue

                        # Execute swap
                        optimized[we_dk][we_idx] = under_w
                        optimized[wd_dk][wd_idx] = donor_name
                        swaps_made += 1
                        shortage -= 1

                        we_disp = we_dk.strftime("%Y-%m-%d") if isinstance(we_dk, datetime) else we_dk
                        wd_disp = wd_dk.strftime("%Y-%m-%d") if isinstance(wd_dk, datetime) else wd_dk
                        logging.info(f"      🎯 PULL: {donor_name}(we {we_disp})↔{under_w}(wd {wd_disp})")
                        swapped = True
                        break

        logging.info(f"   ✅ Weekend pull: {swaps_made} donor swaps")
        return optimized

    def _export_initial_calendar_pdf(self) -> None:
        """
        Export the initial calendar (before iterative optimization) to PDF.
        Creates one PDF with all months, each month on a separate landscape A4 page.
        """
        try:
            logging.info("\n" + "=" * 80)
            logging.info("📄 GENERATING INITIAL CALENDAR PDF")
            logging.info("=" * 80)

            # Count and log schedule stats before export
            total_shifts = sum(len(shifts) for shifts in self.scheduler.schedule.values())
            filled_shifts = sum(
                1 for shifts in self.scheduler.schedule.values() for worker in shifts if worker is not None
            )
            empty_shifts = total_shifts - filled_shifts

            logging.info("Schedule statistics at PDF export:")
            logging.info(f"  Total shifts: {total_shifts}")
            logging.info(f"  Filled shifts: {filled_shifts}")
            logging.info(f"  Empty shifts: {empty_shifts}")
            logging.info(f"  Fill rate: {(filled_shifts / total_shifts * 100):.1f}%")

            # Import PDF exporter
            from saldo27.pdf_exporter import PDFExporter

            # Prepare configuration for PDF exporter
            schedule_config = {
                "schedule": self.scheduler.schedule,
                "workers_data": self.scheduler.workers_data,
                "num_shifts": self.scheduler.num_shifts,
                "holidays": self.scheduler.holidays,
            }

            # Create exporter instance
            pdf_exporter = PDFExporter(schedule_config)

            # Generate filename with timestamp and "INITIAL" marker
            start_date = self.scheduler.start_date
            end_date = self.scheduler.end_date
            period_str = f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"schedule_INITIAL_{period_str}_{timestamp}.pdf"

            # Export all months to single PDF (landscape A4, one month per page)
            result_file = pdf_exporter.export_all_months_calendar(filename=filename)

            if result_file:
                logging.info(f"✅ Initial calendar PDF generated successfully: {result_file}")
                logging.info("   Format: Landscape A4, one month per page")
                logging.info(f"   Period: {start_date.strftime('%d-%m-%Y')} to {end_date.strftime('%d-%m-%Y')}")
            else:
                logging.warning("⚠️  Initial calendar PDF generation returned no file")

            logging.info("=" * 80)

        except ImportError as e:
            logging.error(f"❌ Could not import PDF exporter: {e}")
            logging.info("Continuing without initial PDF export")
        except Exception as e:
            logging.error(f"❌ Error generating initial calendar PDF: {e!s}", exc_info=True)
            logging.info("Continuing without initial PDF export")

    @staticmethod
    def _make_hash_seed(attempt_num: int, tag: str) -> int:
        """Generate a well-distributed 32-bit seed using SHA-256.

        Unlike linear arithmetic (1000+n, 2000+n*11 …), consecutive attempt
        numbers produce seeds with no pairwise correlation, giving the
        Mersenne Twister truly independent starting states per attempt.
        """
        key = f"{attempt_num}:{tag}".encode()
        return int(hashlib.sha256(key).hexdigest()[:8], 16) % (2**32)

    def _select_distribution_strategy(self, attempt_num: int, total_attempts: int) -> dict[str, Any]:
        """
        Select a distribution strategy for this attempt with TRUE variance.

        CRITICAL: Each strategy must produce MATERIALLY DIFFERENT initial schedules.
        Strategies include:
        - Different random seeds (ensures different slot choices)
        - Different worker ordering (changes priority of workers)
        - Combination approaches

        Args:
            attempt_num: Current attempt number (1-indexed)
            total_attempts: Total number of attempts

        Returns:
            Dict with strategy configuration
        """
        # NOTE on list_position tiebreaker:
        # At Phase 1 fill time all workers have 0 shifts, so they all land in the
        # same shift-count tier and the primary sort leaves them in alphabetical order.
        # Sequential/alternating lists therefore still start with worker A, making
        # list_position collapse to the same result as alphabetical_asc.
        # The only tiebreaker that produces genuine diversity is 'random' (GRASP-RCL),
        # which picks randomly from the top-20% score band.
        # Strategy design: keep exactly ONE pure A-Z and ONE pure Z-A as deterministic
        # baselines; all other 10 slots use GRASP-RCL with differing worker orderings
        # and distinct hash seeds so each attempt produces a unique schedule.
        strategies = [
            # ── Deterministic baselines ──────────────────────────────────────────
            {
                "name": "Alphabetical A-Z Tiebreaker",
                "worker_order": "balanced",
                "randomize": False,
                "seed": None,
                "tiebreaker": "alphabetical_asc",
                "description": "Deterministic baseline: A-Z wins every tie",
            },
            {
                "name": "Alphabetical Z-A Tiebreaker",
                "worker_order": "balanced",
                "randomize": False,
                "seed": None,
                "tiebreaker": "alphabetical_desc",
                "description": "Deterministic baseline: Z-A wins every tie",
            },
            # ── GRASP-RCL strategies (genuinely diverse) ─────────────────────────
            {
                "name": f"Balanced + GRASP (Seed {self._make_hash_seed(attempt_num, 'rt')})",
                "worker_order": "balanced",
                "randomize": True,
                "seed": self._make_hash_seed(attempt_num, "rt"),
                "tiebreaker": "random",
                "description": "Balanced order, GRASP-RCL selects randomly from top-20% score band",
            },
            {
                "name": f"Sequential + GRASP (Seed {self._make_hash_seed(attempt_num, 'sg')})",
                "worker_order": "sequential",
                "randomize": True,
                "seed": self._make_hash_seed(attempt_num, "sg"),
                "tiebreaker": "random",
                "description": "A-Z secondary order with GRASP-RCL tiebreaker for genuine diversity",
            },
            {
                "name": f"Reverse + GRASP (Seed {self._make_hash_seed(attempt_num, 'rg')})",
                "worker_order": "reverse",
                "randomize": True,
                "seed": self._make_hash_seed(attempt_num, "rg"),
                "tiebreaker": "random",
                "description": "Z-A secondary order with GRASP-RCL tiebreaker",
            },
            {
                "name": f"Workload Priority + GRASP (Seed {self._make_hash_seed(attempt_num, 'wp')})",
                "worker_order": "workload",
                "randomize": True,
                "seed": self._make_hash_seed(attempt_num, "wp"),
                "tiebreaker": "random",
                "description": "Workload-deficit ordering with GRASP-RCL tiebreaker",
            },
            {
                "name": f"Alternating + GRASP (Seed {self._make_hash_seed(attempt_num, 'ag')})",
                "worker_order": "alternating",
                "randomize": True,
                "seed": self._make_hash_seed(attempt_num, "ag"),
                "tiebreaker": "random",
                "description": "Low/high alternating order with GRASP-RCL tiebreaker",
            },
            {
                "name": f"Random Order + GRASP (Seed {self._make_hash_seed(attempt_num, 'rr')})",
                "worker_order": "random",
                "randomize": True,
                "seed": self._make_hash_seed(attempt_num, "rr"),
                "tiebreaker": "random",
                "description": "Fully randomized worker order with GRASP-RCL",
            },
            {
                "name": f"Workload Deficit + GRASP (Seed {self._make_hash_seed(attempt_num, 'dg')})",
                "worker_order": "workload",
                "randomize": True,
                "seed": self._make_hash_seed(attempt_num, "dg"),
                "tiebreaker": "random",
                "description": "Workload-deficit priority with GRASP, distinct seed sequence",
            },
            {
                "name": f"Random Order + GRASP Alt (Seed {self._make_hash_seed(attempt_num, 'ra')})",
                "worker_order": "random",
                "randomize": True,
                "seed": self._make_hash_seed(attempt_num, "ra"),
                "tiebreaker": "random",
                "description": "Random order with GRASP, alternate seed sequence",
            },
            {
                "name": f"Quasi-Random PCG64 + GRASP (Seed {self._make_hash_seed(attempt_num, 'qr')})",
                "worker_order": "quasi_random",
                "randomize": True,
                "seed": self._make_hash_seed(attempt_num, "qr"),
                "tiebreaker": "random",
                "description": "NumPy PCG64 permutation within shift-count groups + GRASP-RCL",
            },
            {
                "name": f"Quasi-Random PCG64 + GRASP Alt (Seed {self._make_hash_seed(attempt_num, 'qa')})",
                "worker_order": "quasi_random",
                "randomize": True,
                "seed": self._make_hash_seed(attempt_num, "qa"),
                "tiebreaker": "random",
                "description": "PCG64 permutation with alternate GRASP seed for maximum diversity",
            },
        ]

        # Select strategy based on attempt number.
        # The two deterministic baselines (A-Z and Z-A, slots 0 and 1) always
        # produce the same schedule regardless of attempt_num, so we only run
        # them once — on the very first cycle (attempts 1 and 2).
        # From the second cycle onward those slots are replaced by two extra
        # GRASP-RCL variants with unique seeds so no attempt is wasted.
        num_strategies = len(strategies)
        strategy_index = (attempt_num - 1) % num_strategies
        if strategy_index < 2 and attempt_num > num_strategies:
            # Second cycle or later: replace deterministic baselines with GRASP variants
            if strategy_index == 0:
                chosen = {
                    "name": f"Balanced + GRASP Cycle {(attempt_num - 1) // num_strategies + 1} (Seed {self._make_hash_seed(attempt_num, 'bc')})",
                    "worker_order": "balanced",
                    "randomize": True,
                    "seed": self._make_hash_seed(attempt_num, "bc"),
                    "tiebreaker": "random",
                    "description": "Extra GRASP cycle replacing A-Z deterministic baseline",
                }
            else:  # strategy_index == 1
                chosen = {
                    "name": f"Workload + GRASP Cycle {(attempt_num - 1) // num_strategies + 1} (Seed {self._make_hash_seed(attempt_num, 'wc')})",
                    "worker_order": "workload",
                    "randomize": True,
                    "seed": self._make_hash_seed(attempt_num, "wc"),
                    "tiebreaker": "random",
                    "description": "Extra GRASP cycle replacing Z-A deterministic baseline",
                }
        else:
            chosen = strategies[strategy_index]
        # Embed attempt_num so _perform_initial_fill_with_strategy can use it for hash seeds
        chosen["attempt_num"] = attempt_num
        return chosen

    def _perform_initial_fill_with_strategy(self, strategy: dict[str, Any]) -> bool:
        """
        Perform initial schedule fill using the specified strategy.

        Args:
            strategy: Strategy configuration dictionary including tiebreaker strategy

        Returns:
            bool: True if fill was successful
        """
        try:
            # Set tiebreaker strategy in schedule_builder
            tiebreaker = strategy.get("tiebreaker", "alphabetical_asc")
            self.scheduler.schedule_builder.tiebreaker_strategy = tiebreaker
            logging.info(f"🎯 Strategy: {strategy['name']}")
            logging.info(f"📊 Worker Order: {strategy['worker_order']}")
            logging.info(f"🔀 Tiebreaker Strategy: {tiebreaker}")

            # Set random seed FIRST and MULTIPLE TIMES to ensure complete randomness reset
            # This prevents stale random state from previous attempts
            if strategy.get("seed") is not None:
                seed_value = strategy["seed"]
                random.seed(seed_value)
                logging.info(f"🔢 Random Seed: {seed_value}")
            else:
                # For deterministic strategies, derive a hash-based seed so that
                # consecutive attempts have uncorrelated random state even when
                # no explicit seed is configured in the strategy.
                _attempt = strategy.get("attempt_num", 0)
                seed_value = self._make_hash_seed(_attempt, strategy["name"])
                random.seed(seed_value)
                logging.info(f"Hash-derived seed {seed_value} for strategy '{strategy['name']}'")
            # Get worker list based on strategy
            workers_list = self._get_ordered_workers_list(strategy["worker_order"])

            logging.info(f"Filling schedule with {len(workers_list)} workers using '{strategy['name']}' strategy")

            # Perform initial fill
            # Use adaptive iteration config to determine fill attempts
            adaptive_config = self.adaptive_manager.calculate_adaptive_iterations()
            fill_attempts = adaptive_config.get("fill_attempts", 16)

            logging.info(f"Using {fill_attempts} fill attempts based on adaptive configuration")

            # Call schedule builder's fill method with custom worker ordering
            success = self.scheduler.schedule_builder._try_fill_empty_shifts_with_worker_order(
                workers_list, max_attempts=fill_attempts
            )

            if success:
                logging.info(f"✅ Initial fill successful with '{strategy['name']}' strategy")
            else:
                logging.warning(f"⚠️  Initial fill had issues with '{strategy['name']}' strategy")

            return success

        except AttributeError:
            # Fallback if custom worker order method doesn't exist
            logging.warning("Custom worker order method not available, using standard fill")
            return self.scheduler.schedule_builder._try_fill_empty_shifts()

        except Exception as e:
            logging.error(f"Error during initial fill with strategy '{strategy['name']}': {e}", exc_info=True)
            return False

    def _get_ordered_workers_list(self, order_type: str) -> list[dict]:
        """
        Get workers list ordered according to specified type.

        CRITICAL CHANGE: ALWAYS prioritizes workers with fewer assigned shifts first,
        then applies secondary ordering strategy. This ensures fair distribution.

        Args:
            order_type: Type of ordering ('balanced', 'random', 'sequential', 'reverse',
                       'workload', 'alternating', 'quasi_random')

        Returns:
            List of worker dictionaries in specified order, with workers having
            fewer shifts always getting priority
        """
        workers = list(self.workers_data)

        # PRIMARY SORT: Always by current assignment count (fewer shifts first)
        # This ensures workers with 0 shifts get priority over workers with 3+ shifts
        workers.sort(key=lambda w: self.scheduler.worker_shift_counts.get(w["id"], 0))

        # SECONDARY SORT: Apply strategy-specific ordering within groups of same shift count
        if order_type == "random":
            # Group by shift count, then randomize within each group
            from itertools import groupby

            result = []
            for shift_count, group in groupby(
                workers, key=lambda w: self.scheduler.worker_shift_counts.get(w["id"], 0)
            ):
                group_list = list(group)
                random.shuffle(group_list)
                result.extend(group_list)
            workers = result

        elif order_type == "sequential":
            # Secondary sort by ID, but keeping shift count priority
            workers.sort(key=lambda w: (self.scheduler.worker_shift_counts.get(w["id"], 0), w["id"]))

        elif order_type == "reverse":
            # Ascending shift count, descending ID (Z→A) as tiebreaker.
            # Use two stable sorts: first by ID desc, then by shift count asc.
            workers.sort(key=lambda w: w["id"], reverse=True)
            workers.sort(key=lambda w: self.scheduler.worker_shift_counts.get(w["id"], 0))

        elif order_type == "balanced":
            # Already sorted by shift count, keep as is
            pass

        elif order_type == "workload":
            # Order by deficit percentage, but still prioritize by absolute shift count first
            def get_sort_key(worker):
                worker_id = worker["id"]
                current = self.scheduler.worker_shift_counts.get(worker_id, 0)
                target = worker.get("target_shifts", 0)
                if target == 0:
                    deficit_pct = 0
                else:
                    deficit = target - current
                    deficit_pct = (deficit / target) * 100
                # Return tuple: (current_shifts, -deficit_percentage)
                # Lower current shifts come first, then higher deficit % within same shift count
                return (current, -deficit_pct)

            workers.sort(key=get_sort_key)

        elif order_type == "alternating":
            # Already sorted by shift count
            # Alternate between lowest and highest within each shift count group
            alternated = []
            low_idx = 0
            high_idx = len(workers) - 1
            while low_idx <= high_idx:
                if low_idx == high_idx:
                    alternated.append(workers[low_idx])
                else:
                    alternated.append(workers[low_idx])
                    alternated.append(workers[high_idx])
                low_idx += 1
                high_idx -= 1
            workers = alternated

        elif order_type == "quasi_random":
            # Use numpy's PCG64 generator (better statistical properties than MT19937)
            # to produce permutations within each shift-count group.
            # The seed is drawn from the already-seeded Python random state so results
            # are fully reproducible given the same hash-based seed per attempt.
            from itertools import groupby as _groupby

            import numpy as np

            _seed_val = random.getrandbits(64)
            _rng = np.random.default_rng(_seed_val)
            result = []
            for _, _grp in _groupby(workers, key=lambda w: self.scheduler.worker_shift_counts.get(w["id"], 0)):
                _grp_list = list(_grp)
                _perm = _rng.permutation(len(_grp_list))
                result.extend(_grp_list[int(i)] for i in _perm)
            workers = result

        # Log first few workers to verify ordering
        if len(workers) > 0:
            first_5 = [(w["id"], self.scheduler.worker_shift_counts.get(w["id"], 0)) for w in workers[:5]]
            logging.debug(f"Worker order ({order_type}), first 5: {first_5}")

        return workers

    def _calculate_coverage_percentage(self) -> float:
        """
        Calculate the percentage of shifts that are filled.

        Returns:
            float: Coverage percentage (0-100)
        """
        total_shifts = 0
        filled_shifts = 0

        for date, shifts in self.scheduler.schedule.items():
            total_shifts += len(shifts)
            filled_shifts += sum(1 for worker in shifts if worker is not None)

        if total_shifts == 0:
            return 0.0

        return (filled_shifts / total_shifts) * 100.0

    def _select_best_complete_attempt(self, complete_attempts: list[dict]) -> dict:
        """
        Select the best complete attempt based on multiple criteria.

        Priority:
        1. Highest coverage
        2. If coverage equal, lowest workload imbalance
        3. If still tied, lowest weekend imbalance
        4. If still tied, highest overall score

        Args:
            complete_attempts: List of complete attempt results

        Returns:
            Dict: Best complete attempt data
        """
        logging.info(f"\n{'=' * 80}")
        logging.info("📊 COMPARING ALL COMPLETE ATTEMPTS")
        logging.info(f"{'=' * 80}")

        # Display comparison table
        logging.info(f"\n{'Att':<5} {'Coverage':<12} {'Empty':<8} {'Score':<10} {'Work Imb':<12} {'Weekend Imb':<12}")
        logging.info("─" * 70)

        for attempt in complete_attempts:
            logging.info(
                f"{attempt['attempt']:<5} "
                f"{attempt['coverage']:>10.2f}%  "
                f"{attempt['empty_shifts']:<8} "
                f"{attempt['score']:<10.2f} "
                f"{attempt['workload_imbalance']:<12.2f} "
                f"{attempt['weekend_imbalance']:<12.2f}"
            )

        # Sort by: coverage (desc), workload_imbalance (asc), weekend_imbalance (asc), score (desc)
        sorted_attempts = sorted(
            complete_attempts,
            key=lambda x: (
                -x["coverage"],  # Higher coverage is better (negative for desc)
                x["workload_imbalance"],  # Lower imbalance is better
                x["weekend_imbalance"],  # Lower imbalance is better
                -x["score"],  # Higher score is better (negative for desc)
            ),
        )

        best = sorted_attempts[0]

        logging.info(f"\n🏆 Best attempt: #{best['attempt']}")
        logging.info(
            f"   Reason: Coverage={best['coverage']:.2f}%, "
            f"Workload Imb={best['workload_imbalance']:.2f}, "
            f"Weekend Imb={best['weekend_imbalance']:.2f}"
        )

        return best
