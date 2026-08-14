from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from saldo27.scheduler import Scheduler


class SchedulerTrackingState:
    """Owns mutable tracking dictionaries and their repair/validation logic."""

    def __init__(self, scheduler: Scheduler) -> None:
        self.scheduler = scheduler

    def initialize(self) -> None:
        scheduler = self.scheduler
        scheduler.schedule = {}
        scheduler.schedule_builder = None
        scheduler.worker_assignments = {worker["id"]: set() for worker in scheduler.workers_data}
        scheduler.worker_posts = {worker["id"]: set() for worker in scheduler.workers_data}
        scheduler.worker_weekdays = {worker["id"]: {day: 0 for day in range(7)} for worker in scheduler.workers_data}
        scheduler.worker_weekends = {worker["id"]: [] for worker in scheduler.workers_data}

        self.initialize_schedule_with_variable_shifts()

        scheduler.worker_shift_counts = {worker["id"]: 0 for worker in scheduler.workers_data}
        scheduler.worker_weekend_counts = {worker["id"]: 0 for worker in scheduler.workers_data}
        scheduler.worker_post_counts = {
            worker["id"]: {post: 0 for post in range(scheduler.num_shifts)} for worker in scheduler.workers_data
        }
        scheduler.worker_weekday_counts = {
            worker["id"]: {day: 0 for day in range(7)} for worker in scheduler.workers_data
        }
        scheduler.worker_holiday_counts = {worker["id"]: 0 for worker in scheduler.workers_data}
        scheduler.last_assignment_date = {worker["id"]: None for worker in scheduler.workers_data}
        scheduler.consecutive_shifts = {worker["id"]: 0 for worker in scheduler.workers_data}

        for worker in scheduler.workers_data:
            if "target_shifts" not in worker:
                worker["target_shifts"] = 0

        total_days = (scheduler.end_date - scheduler.start_date).days + 1
        total_shifts_possible = total_days * scheduler.num_shifts
        num_workers = len(scheduler.workers_data)
        scheduler.max_shifts_per_worker = (
            (total_shifts_possible // num_workers) + 2 if num_workers > 0 else total_shifts_possible
        )
        scheduler.constraint_skips = {
            worker["id"]: {"gap": [], "incompatibility": [], "reduced_gap": []} for worker in scheduler.workers_data
        }

    def initialize_schedule_with_variable_shifts(self) -> None:
        scheduler = self.scheduler
        current_date = scheduler.start_date
        var_cfgs = [(cfg["start_date"], cfg["end_date"], cfg["shifts"]) for cfg in scheduler.variable_shifts]
        while current_date <= scheduler.end_date:
            shifts_for_date = scheduler.num_shifts
            for start, end, count in var_cfgs:
                if start <= current_date <= end:
                    shifts_for_date = count
                    logging.info(
                        f"Variable shifts applied for {current_date}: {count} shifts (default is {scheduler.num_shifts})"
                    )
                    break
            scheduler.schedule[current_date] = [None] * shifts_for_date
            current_date += timedelta(days=1)

    def reset(self) -> None:
        scheduler = self.scheduler
        scheduler.schedule = {}
        scheduler.worker_assignments = {worker["id"]: set() for worker in scheduler.workers_data}
        scheduler.worker_posts = {worker["id"]: set() for worker in scheduler.workers_data}
        scheduler.worker_weekdays = {worker["id"]: {day: 0 for day in range(7)} for worker in scheduler.workers_data}
        scheduler.worker_weekends = {worker["id"]: [] for worker in scheduler.workers_data}
        scheduler.constraint_skips = {
            worker["id"]: {"gap": [], "incompatibility": [], "reduced_gap": []} for worker in scheduler.workers_data
        }

    def ensure_data_integrity(self) -> bool:
        scheduler = self.scheduler
        logging.info("Ensuring data integrity...")

        for worker in scheduler.workers_data:
            worker_id = worker["id"]
            if worker_id not in scheduler.worker_assignments:
                scheduler.worker_assignments[worker_id] = set()
            if worker_id not in scheduler.worker_posts:
                scheduler.worker_posts[worker_id] = set()
            if worker_id not in scheduler.worker_weekdays:
                scheduler.worker_weekdays[worker_id] = {day: 0 for day in range(7)}
            if worker_id not in scheduler.worker_weekends:
                scheduler.worker_weekends[worker_id] = []

        for current_date in scheduler._get_date_range(scheduler.start_date, scheduler.end_date):
            expected = scheduler._get_shifts_for_date(current_date)
            if current_date not in scheduler.schedule:
                scheduler.schedule[current_date] = [None] * expected
            else:
                actual = len(scheduler.schedule[current_date])
                if actual < expected:
                    scheduler.schedule[current_date].extend([None] * (expected - actual))
                elif actual > expected:
                    scheduler.schedule[current_date] = scheduler.schedule[current_date][:expected]

        logging.info("Data integrity check completed")
        return True

    def synchronize(self) -> bool:
        scheduler = self.scheduler
        try:
            logging.info("Synchronizing tracking data structures...")
            scheduler._clear_cache()

            for worker_id in (worker["id"] for worker in scheduler.workers_data):
                scheduler.worker_assignments[worker_id].clear()
                if not isinstance(scheduler.worker_posts[worker_id], set):
                    logging.warning(
                        f"Correcting worker_posts[{worker_id}] from {type(scheduler.worker_posts[worker_id])} to set"
                    )
                    scheduler.worker_posts[worker_id] = set()
                else:
                    scheduler.worker_posts[worker_id].clear()
                scheduler.worker_weekends[worker_id].clear()
                for day in range(7):
                    scheduler.worker_weekdays[worker_id][day] = 0
                scheduler.worker_shift_counts[worker_id] = 0
                scheduler.worker_weekend_counts[worker_id] = 0

            for date, shifts in scheduler.schedule.items():
                weekday = date.weekday()
                is_weekend_or_holiday = (
                    weekday >= 4 or date in scheduler.holidays or (date + timedelta(days=1)) in scheduler.holidays
                )
                for post_idx, worker_id in enumerate(shifts):
                    if worker_id is None:
                        continue
                    scheduler.worker_assignments[worker_id].add(date)
                    scheduler.worker_posts[worker_id].add(post_idx)
                    scheduler.worker_weekdays[worker_id][weekday] += 1
                    if is_weekend_or_holiday:
                        scheduler.worker_weekends[worker_id].append(date)
                        scheduler.worker_weekend_counts[worker_id] += 1
                    scheduler.worker_shift_counts[worker_id] += 1

            for worker_id, weekends in scheduler.worker_weekends.items():
                if weekends:
                    weekends.sort()

            if scheduler.prior_shift_counts:
                for worker_id, prior_count in scheduler.prior_shift_counts.items():
                    if worker_id in scheduler.worker_shift_counts:
                        scheduler.worker_shift_counts[worker_id] += prior_count

            logging.info("Tracking data synchronization complete.")
            return True
        except Exception as exc:
            logging.error(f"Error synchronizing tracking data: {exc!s}", exc_info=True)
            return False

    def validate_synchronization(self) -> tuple[bool, dict[str, Any]]:
        scheduler = self.scheduler
        logging.debug("Validating data synchronization between worker_assignments and schedule...")

        try:
            validation_report: dict[str, Any] = {
                "is_synchronized": True,
                "discrepancies": [],
                "summary": {
                    "total_workers": len(scheduler.workers_data),
                    "workers_with_issues": 0,
                    "total_assignments_schedule": 0,
                    "total_assignments_tracking": 0,
                    "missing_from_tracking": 0,
                    "extra_in_tracking": 0,
                },
            }

            schedule_assignments = {worker["id"]: set() for worker in scheduler.workers_data}
            for date, shifts in scheduler.schedule.items():
                validation_report["summary"]["total_assignments_schedule"] += len(
                    [shift for shift in shifts if shift is not None]
                )
                for worker_id in shifts:
                    if worker_id is not None:
                        schedule_assignments.setdefault(worker_id, set()).add(date)

            for worker_id, assignments in scheduler.worker_assignments.items():
                validation_report["summary"]["total_assignments_tracking"] += len(assignments)

            for worker_id in set(scheduler.worker_assignments) | set(schedule_assignments):
                tracking_assignments = scheduler.worker_assignments.get(worker_id, set())
                schedule_worker_assignments = schedule_assignments.get(worker_id, set())
                missing_from_tracking = schedule_worker_assignments - tracking_assignments
                extra_in_tracking = tracking_assignments - schedule_worker_assignments

                if not missing_from_tracking and not extra_in_tracking:
                    continue

                validation_report["is_synchronized"] = False
                validation_report["summary"]["workers_with_issues"] += 1
                validation_report["summary"]["missing_from_tracking"] += len(missing_from_tracking)
                validation_report["summary"]["extra_in_tracking"] += len(extra_in_tracking)
                validation_report["discrepancies"].append(
                    {
                        "worker_id": worker_id,
                        "missing_from_tracking": sorted(date.strftime("%Y-%m-%d") for date in missing_from_tracking),
                        "extra_in_tracking": sorted(date.strftime("%Y-%m-%d") for date in extra_in_tracking),
                        "tracking_count": len(tracking_assignments),
                        "schedule_count": len(schedule_worker_assignments),
                    }
                )

            if validation_report["is_synchronized"]:
                logging.debug("✓ Data synchronization validation passed: worker_assignments and schedule are synchronized")
            else:
                logging.warning(
                    f"✗ Data synchronization issues detected: {len(validation_report['discrepancies'])} workers affected"
                )
                for discrepancy in validation_report["discrepancies"][:3]:
                    logging.warning(
                        f"  Worker {discrepancy['worker_id']}: {len(discrepancy['missing_from_tracking'])} missing, "
                        f"{len(discrepancy['extra_in_tracking'])} extra"
                    )

            return validation_report["is_synchronized"], validation_report
        except Exception as exc:
            logging.error(f"Error validating data synchronization: {exc!s}", exc_info=True)
            return False, {"error": str(exc), "is_synchronized": False}

    def repair_synchronization(self, validation_report: dict[str, Any] | None = None) -> bool:
        scheduler = self.scheduler
        logging.info("Repairing data synchronization issues...")

        try:
            if validation_report is None:
                is_synchronized, validation_report = self.validate_synchronization()
                if is_synchronized:
                    logging.info("No repair needed: data is already synchronized")
                    return True

            corrected_assignments = {worker["id"]: set() for worker in scheduler.workers_data}
            for date, shifts in scheduler.schedule.items():
                for worker_id in shifts:
                    if worker_id is not None:
                        corrected_assignments.setdefault(worker_id, set()).add(date)

            scheduler.worker_assignments.clear()
            scheduler.worker_assignments.update(corrected_assignments)

            is_synchronized_after, _ = self.validate_synchronization()
            if is_synchronized_after:
                total_fixes = validation_report.get("summary", {}).get("missing_from_tracking", 0) + validation_report.get(
                    "summary", {}
                ).get("extra_in_tracking", 0)
                logging.info(f"✓ Data synchronization repair successful: Fixed {total_fixes} inconsistencies")
                return True

            logging.error("✗ Data synchronization repair failed: Issues still persist")
            return False
        except Exception as exc:
            logging.error(f"Error repairing data synchronization: {exc!s}", exc_info=True)
            return False

    def ensure_synchronization(self) -> bool:
        try:
            is_synchronized, validation_report = self.validate_synchronization()
            if not is_synchronized:
                logging.warning("Data synchronization issues detected, attempting repair...")
                return self.repair_synchronization(validation_report)
            return True
        except Exception as exc:
            logging.error(f"Error ensuring data synchronization: {exc!s}", exc_info=True)
            return False

    def reconcile(self) -> bool:
        logging.info("Reconciling worker assignments tracking with schedule...")
        try:
            is_synchronized = self.ensure_synchronization()
            if is_synchronized:
                logging.info("Reconciliation complete: Data structures are synchronized")
                return True
            logging.error("Reconciliation failed: Unable to synchronize data structures")
            return False
        except Exception as exc:
            logging.error(f"Error reconciling schedule tracking: {exc!s}", exc_info=True)
            return False

    def update_assignment(self, worker_id: str, date: datetime, post: int, *, removing: bool = False) -> None:
        scheduler = self.scheduler
        try:
            if worker_id not in scheduler.worker_assignments:
                scheduler.worker_assignments[worker_id] = set()
            if worker_id not in scheduler.worker_posts or not isinstance(scheduler.worker_posts.get(worker_id), set):
                logging.warning(f"Re-initializing scheduler.worker_posts[{worker_id}] as a set due to incorrect type.")
                scheduler.worker_posts[worker_id] = set()
            if worker_id not in scheduler.worker_weekdays:
                scheduler.worker_weekdays[worker_id] = {day: 0 for day in range(7)}
            if worker_id not in scheduler.worker_weekends:
                scheduler.worker_weekends[worker_id] = []

            if removing:
                if date in scheduler.worker_assignments.get(worker_id, set()):
                    scheduler.worker_assignments[worker_id].remove(date)
                if scheduler.worker_shift_counts.get(worker_id, 0) > 0:
                    scheduler.worker_shift_counts[worker_id] -= 1
                if post in scheduler.worker_posts.get(worker_id, set()):
                    still_works_at_post = any(
                        scheduler._get_worker_assigned_to_post(check_date, post) == worker_id
                        for check_date in scheduler.worker_assignments.get(worker_id, set())
                    )
                    if not still_works_at_post:
                        scheduler.worker_posts[worker_id].discard(post)

                weekday = date.weekday()
                if weekday in scheduler.worker_weekdays.get(worker_id, {}):
                    if scheduler.worker_weekdays[worker_id][weekday] > 0:
                        scheduler.worker_weekdays[worker_id][weekday] -= 1
                else:
                    logging.warning(
                        f"Weekday {weekday} not found in self.worker_weekdays for worker {worker_id} during removal."
                    )

                if scheduler.date_utils.is_weekend_day(date, scheduler.holidays):
                    current_weekends = scheduler.worker_weekends.get(worker_id)
                    if current_weekends is not None and date in current_weekends:
                        current_weekends.remove(date)
                    if scheduler.worker_weekend_counts.get(worker_id, 0) > 0:
                        scheduler.worker_weekend_counts[worker_id] -= 1

                bridge_period = scheduler.date_utils.get_bridge_period_for_date(date, scheduler.bridge_periods)
                if bridge_period:
                    has_other_assignment = any(
                        check_date != date and check_date in scheduler.worker_assignments.get(worker_id, set())
                        for check_date in scheduler._get_dates_in_bridge(bridge_period)
                    )
                    if not has_other_assignment and worker_id in scheduler.worker_bridge_counts:
                        scheduler.worker_bridge_counts[worker_id].discard(bridge_period["id"])
            else:
                scheduler.worker_assignments[worker_id].add(date)
                scheduler.worker_posts[worker_id].add(post)
                scheduler.worker_shift_counts[worker_id] = scheduler.worker_shift_counts.get(worker_id, 0) + 1

                bridge_period = scheduler.date_utils.get_bridge_period_for_date(date, scheduler.bridge_periods)
                if bridge_period:
                    scheduler.worker_bridge_counts.setdefault(worker_id, set()).add(bridge_period["id"])

                weekday = date.weekday()
                scheduler.worker_weekdays[worker_id][weekday] = scheduler.worker_weekdays[worker_id].get(weekday, 0) + 1

                if scheduler.date_utils.is_weekend_day(date, scheduler.holidays):
                    current_weekends = scheduler.worker_weekends.setdefault(worker_id, [])
                    if date not in current_weekends:
                        current_weekends.append(date)
                        current_weekends.sort()
                    scheduler.worker_weekend_counts[worker_id] = scheduler.worker_weekend_counts.get(worker_id, 0) + 1

            if hasattr(scheduler, "eligibility_tracker") and scheduler.eligibility_tracker:
                if removing:
                    scheduler.eligibility_tracker.remove_worker_assignment(worker_id, date)
                else:
                    scheduler.eligibility_tracker.update_worker_status(worker_id, date)

            if hasattr(scheduler, "_validate_assignment_consistency") and not scheduler._validate_assignment_consistency(
                worker_id, date, removing
            ):
                logging.error(
                    f"SYNC ERROR: Data synchronization issue detected after "
                    f"{'removing' if removing else 'adding'} worker {worker_id} on {date.strftime('%Y-%m-%d')}"
                )
                if hasattr(scheduler, "_ensure_data_synchronization"):
                    scheduler._ensure_data_synchronization()

            logging.debug(
                f"{'Removed' if removing else 'Added'} assignment and updated tracking for worker "
                f"{worker_id} on {date.strftime('%Y-%m-%d')}, post {post}"
            )
        except Exception as exc:
            logging.error(
                f"Error in _update_tracking_data for worker {worker_id}, date {date}, post {post}, "
                f"removing={removing}: {exc!s}",
                exc_info=True,
            )
            raise

    def validate_assignment_consistency(self, worker_id: str, date: datetime, *, removing: bool = False) -> bool:
        scheduler = self.scheduler
        try:
            is_in_schedule = date in scheduler.schedule and worker_id in scheduler.schedule.get(date, [])
            is_in_tracking = worker_id in scheduler.worker_assignments and date in scheduler.worker_assignments.get(
                worker_id, set()
            )

            if removing:
                if is_in_schedule or is_in_tracking:
                    logging.debug(
                        f"Inconsistency after removal: worker {worker_id} still found in "
                        f"{'schedule' if is_in_schedule else 'tracking'} for {date.strftime('%Y-%m-%d')}"
                    )
                    return False
            elif is_in_schedule != is_in_tracking:
                logging.debug(
                    f"Inconsistency after addition: worker {worker_id} found in "
                    f"{'schedule' if is_in_schedule else 'tracking'} but not "
                    f"{'tracking' if is_in_schedule else 'schedule'} for {date.strftime('%Y-%m-%d')}"
                )
                return False

            return True
        except Exception as exc:
            logging.error(f"Error validating assignment consistency: {exc!s}", exc_info=True)
            return False
