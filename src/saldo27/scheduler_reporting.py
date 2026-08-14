from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from saldo27.scheduler import Scheduler


class SchedulerReportingService:
    """Owns scoring, summary, and export/report generation concerns."""

    def __init__(self, scheduler: Scheduler) -> None:
        self.scheduler = scheduler

    def calculate_score(
        self,
        schedule_to_score: dict[datetime, list[str | None]] | None = None,
        assignments_to_score: dict[str, set[datetime]] | None = None,
    ) -> float:
        del assignments_to_score
        scheduler = self.scheduler
        logging.debug("Scheduler.calculate_score called")

        current_schedule = schedule_to_score if schedule_to_score is not None else scheduler.schedule
        if not current_schedule:
            return float("-inf")

        filled_shifts = sum(1 for shifts in current_schedule.values() for worker_id in shifts if worker_id is not None)
        total_possible_shifts = sum(len(shifts) for shifts in current_schedule.values())
        score = (filled_shifts / total_possible_shifts) * 100 if total_possible_shifts > 0 else 0

        logging.debug(f"Calculated score (placeholder): {score}")
        return score

    def calculate_coverage(self) -> float:
        scheduler = self.scheduler
        try:
            total_shifts = sum(
                scheduler._get_shifts_for_date(date)
                for date in scheduler._get_date_range(scheduler.start_date, scheduler.end_date)
            )
            filled_shifts = sum(1 for shifts in scheduler.schedule.values() for worker in shifts if worker is not None)

            logging.info(f"Coverage calculation: {filled_shifts} filled out of {total_shifts} total shifts")
            logging.debug(f"Schedule contains {len(scheduler.schedule)} dates with shifts")

            sample_size = min(3, len(scheduler.schedule))
            if sample_size > 0:
                for date in list(scheduler.schedule)[:sample_size]:
                    logging.debug(f"Sample date {date.strftime('%d-%m-%Y')}: {scheduler.schedule[date]}")

            if total_shifts > 0:
                return (filled_shifts / total_shifts) * 100
            return 0
        except Exception as exc:
            logging.error(f"Error calculating coverage: {exc!s}", exc_info=True)
            return 0

    def calculate_post_rotation(self) -> dict[str, float]:
        try:
            rotation_data = self.calculate_post_rotation_coverage()
            if isinstance(rotation_data, dict) and "uniformity" in rotation_data and "avg_worker" in rotation_data:
                return rotation_data

            overall_score = rotation_data if isinstance(rotation_data, (int, float)) else 40.0
            return {
                "overall_score": overall_score,
                "uniformity": 0.0,
                "avg_worker": 100.0,
            }
        except Exception as exc:
            logging.error(f"Error in calculating post rotation: {exc!s}")
            return {"overall_score": 40.0, "uniformity": 0.0, "avg_worker": 100.0}

    def calculate_post_rotation_coverage(self) -> dict[str, Any]:
        scheduler = self.scheduler
        logging.info("Calculating post rotation coverage...")

        metrics: dict[str, Any] = {"overall_score": 0, "worker_scores": {}, "post_distribution": {}}
        post_counts = {post: 0 for post in range(scheduler.num_shifts)}
        total_assignments = 0

        for shifts in scheduler.schedule.values():
            for post, worker in enumerate(shifts):
                if worker is not None:
                    post_counts[post] = post_counts.get(post, 0) + 1
                    total_assignments += 1

        if total_assignments > 0:
            expected_per_post = total_assignments / scheduler.num_shifts
            post_deviation = 0

            for post, count in post_counts.items():
                metrics["post_distribution"][post] = {
                    "count": count,
                    "percentage": (count / total_assignments * 100) if total_assignments > 0 else 0,
                }
                post_deviation += abs(count - expected_per_post)

            post_uniformity = max(0, 100 - (post_deviation / total_assignments * 100))
        else:
            post_uniformity = 0

        worker_scores = {}
        overall_worker_deviation = 0

        for worker in scheduler.workers_data:
            worker_id = worker["id"]
            worker_assignments = len(scheduler.worker_assignments.get(worker_id, []))
            if worker_assignments < 2:
                worker_scores[worker_id] = 100
                continue

            worker_post_counts = {post: 0 for post in range(scheduler.num_shifts)}
            for shifts in scheduler.schedule.values():
                for post, assigned_worker in enumerate(shifts):
                    if assigned_worker == worker_id:
                        worker_post_counts[post] = worker_post_counts.get(post, 0) + 1

            expected_per_post_for_worker = worker_assignments / scheduler.num_shifts
            worker_deviation = sum(abs(count - expected_per_post_for_worker) for count in worker_post_counts.values())

            if worker_assignments > 0:
                worker_score = max(0, 100 - (worker_deviation / worker_assignments * 100))
                normalized_worker_deviation = worker_deviation / worker_assignments
            else:
                worker_score = 100
                normalized_worker_deviation = 0

            worker_scores[worker_id] = worker_score
            overall_worker_deviation += normalized_worker_deviation

        avg_worker_score = sum(worker_scores.values()) / len(scheduler.workers_data) if scheduler.workers_data else 0
        metrics["overall_score"] = (post_uniformity * 0.6) + (avg_worker_score * 0.4)
        metrics["post_uniformity"] = post_uniformity
        metrics["avg_worker_score"] = avg_worker_score
        metrics["worker_scores"] = worker_scores

        logging.info(f"Post rotation overall score: {metrics['overall_score']:.2f}%")
        logging.info(f"Post uniformity: {post_uniformity:.2f}%, Avg worker score: {avg_worker_score:.2f}%")

        return metrics

    def export_schedule(self, output_format: str = "txt") -> str:
        scheduler = self.scheduler
        timestamp = datetime.now().strftime("%d%m%Y_%H%M%S")
        filename = f"schedule_{timestamp}.{output_format}"

        if output_format == "txt":
            worker_names = {worker["id"]: worker["name"] for worker in scheduler.workers_data}
            with open(filename, "w", encoding="utf-8") as file_obj:
                file_obj.write("=" * 60 + "\n")
                file_obj.write("HORARIO GENERADO\n")
                file_obj.write("=" * 60 + "\n")
                file_obj.write(f"Fecha de generación: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n")
                file_obj.write(
                    f"Período: {scheduler.start_date.strftime('%d/%m/%Y')} - {scheduler.end_date.strftime('%d/%m/%Y')}\n"
                )
                file_obj.write(f"Trabajadores: {len(scheduler.workers_data)}\n")
                file_obj.write(f"Turnos por día: {scheduler.num_shifts}\n\n")

                current_date = scheduler.start_date
                while current_date <= scheduler.end_date:
                    if current_date in scheduler.schedule:
                        day_name = [
                            "Lunes",
                            "Martes",
                            "Miércoles",
                            "Jueves",
                            "Viernes",
                            "Sábado",
                            "Domingo",
                        ][current_date.weekday()]
                        file_obj.write(f"{day_name} {current_date.strftime('%d/%m/%Y')}\n")

                        for post_idx, worker_id in enumerate(scheduler.schedule[current_date]):
                            if worker_id:
                                worker_name = worker_names.get(worker_id, worker_id)
                                file_obj.write(f"  Turno {post_idx + 1}: {worker_name} ({worker_id})\n")
                            else:
                                file_obj.write(f"  Turno {post_idx + 1}: [VACANTE]\n")
                        file_obj.write("\n")
                    current_date += timedelta(days=1)

                file_obj.write("\n" + "=" * 60 + "\n")
                file_obj.write("RESUMEN\n")
                file_obj.write("=" * 60 + "\n")
                for worker in scheduler.workers_data:
                    worker_id = worker["id"]
                    shift_count = len(scheduler.worker_assignments.get(worker_id, []))
                    weekend_count = len(
                        [
                            date
                            for date in scheduler.worker_assignments.get(worker_id, [])
                            if scheduler.date_utils.is_weekend_day(date, scheduler.holidays)
                        ]
                    )
                    file_obj.write(
                        f"{worker['name']} ({worker_id}): {shift_count} turnos, {weekend_count} fines de semana\n"
                    )

        logging.info(f"Schedule exported to {filename}")
        return filename

    def export_schedule_json(self, filename: str | None = None) -> str:
        scheduler = self.scheduler
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"schedule_complete_{timestamp}.json"

        schedule_serializable = {date.strftime("%Y-%m-%d"): workers for date, workers in scheduler.schedule.items()}
        worker_assignments_serializable = {
            worker_id: [date.strftime("%Y-%m-%d") for date in sorted(dates)]
            for worker_id, dates in scheduler.worker_assignments.items()
        }

        data = {
            "metadata": {
                "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "period_start": scheduler.start_date.strftime("%Y-%m-%d"),
                "period_end": scheduler.end_date.strftime("%Y-%m-%d"),
                "total_days": (scheduler.end_date - scheduler.start_date).days + 1,
                "num_shifts_per_day": scheduler.num_shifts,
                "total_workers": len(scheduler.workers_data),
            },
            "schedule": schedule_serializable,
            "worker_assignments": worker_assignments_serializable,
            "workers_data": scheduler.workers_data,
            "config": {
                "start_date": scheduler.start_date.strftime("%Y-%m-%d"),
                "end_date": scheduler.end_date.strftime("%Y-%m-%d"),
                "num_shifts": scheduler.num_shifts,
                "gap_between_shifts": scheduler.gap_between_shifts,
                "max_consecutive_weekends": scheduler.max_consecutive_weekends,
            },
        }

        with open(filename, "w", encoding="utf-8") as file_obj:
            json.dump(data, file_obj, indent=2, ensure_ascii=False)

        logging.info(f"Complete schedule exported to JSON: {filename}")
        return filename

    def generate_worker_report(self, worker_id: str, *, save_to_file: bool = False) -> str:
        scheduler = self.scheduler
        try:
            report = scheduler.stats.generate_worker_report(worker_id)
            if save_to_file:
                filename = f"worker_{worker_id}_report.txt"
                with open(filename, "w", encoding="utf-8") as file_obj:
                    file_obj.write(report)
                logging.info(f"Worker report saved to {filename}")
            return report
        except Exception as exc:
            logging.error(f"Error generating worker report: {exc!s}")
            return f"Error generating report: {exc!s}"

    def generate_all_worker_reports(self, output_directory: str | None = None) -> int:
        scheduler = self.scheduler
        count = 0
        for worker in scheduler.workers_data:
            worker_id = worker["id"]
            try:
                report = scheduler.stats.generate_worker_report(worker_id)
                filename = f"worker_{worker_id}_report.txt"
                if output_directory:
                    os.makedirs(output_directory, exist_ok=True)
                    filename = os.path.join(output_directory, filename)
                with open(filename, "w", encoding="utf-8") as file_obj:
                    file_obj.write(report)
                count += 1
                logging.info(f"Generated report for worker {worker_id}")
            except Exception as exc:
                logging.error(f"Failed to generate report for worker {worker_id}: {exc!s}")

        logging.info(f"Generated {count} worker reports")
        return count

    def log_schedule_summary(self, title: str = "Schedule Summary") -> None:
        scheduler = self.scheduler
        logging.info(f"--- {title} ---")
        try:
            total_shifts_assigned = sum(len(assignments) for assignments in scheduler.worker_assignments.values())
            logging.info(f"Total shifts assigned: {total_shifts_assigned}")

            total_slots = sum(len(posts) for posts in scheduler.schedule.values())
            empty_shifts = sum(posts.count(None) for posts in scheduler.schedule.values())
            logging.info(f"Total slots: {total_slots}, Empty slots: {empty_shifts}")

            logging.info("Shift Counts per Worker:")
            for worker_id, count in sorted(scheduler.worker_shift_counts.items()):
                logging.info(f"  Worker {worker_id}: {count} shifts")

            logging.info("Weekend Shifts per Worker:")
            for worker_id, count in sorted(scheduler.worker_weekend_counts.items()):
                logging.info(f"  Worker {worker_id}: {count} weekend shifts")

            logging.info("Post Assignments per Worker:")
            for worker_id in sorted(scheduler.worker_posts):
                posts_set = scheduler.worker_posts[worker_id]
                if not posts_set:
                    continue
                posts_list = sorted(posts_set)
                post_counts: dict[int, int] = {}
                for shifts in scheduler.schedule.values():
                    for post_idx, assigned_worker in enumerate(shifts):
                        if assigned_worker == worker_id:
                            post_counts[post_idx] = post_counts.get(post_idx, 0) + 1
                post_details = [f"P{post}({post_counts.get(post, 0)})" for post in posts_list]
                logging.info(f"  Worker {worker_id}: {', '.join(post_details)}")

            logging.info(f"Current Schedule Score: {self.calculate_score()}")
        except Exception as exc:
            logging.error(f"Error generating schedule summary: {exc}")
        logging.info(f"--- End {title} ---")
