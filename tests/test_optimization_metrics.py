"""Tests for OptimizationMetrics score-component deadzones and exemptions."""

from datetime import datetime

from saldo27.optimization_metrics import OptimizationMetrics
from saldo27.scheduler import Scheduler


def _build_scheduler(workers_data, num_shifts=2):
    return Scheduler(
        {
            "start_date": datetime(2026, 3, 1),
            "end_date": datetime(2026, 3, 31),
            "num_shifts": num_shifts,
            "workers_data": workers_data,
            "holidays": [],
            "variable_shifts": [],
            "gap_between_shifts": 1,
            "max_consecutive_weekends": 5,
        }
    )


def test_workload_balance_score_no_penalty_within_10_percent():
    """Deviations of <= 10% from target_shifts must not penalize the score."""
    workers = [
        {
            "id": "A",
            "target_shifts": 10,
            "work_percentage": 100,
            "work_periods": "",
            "mandatory_days": "",
            "days_off": "",
        },
        {
            "id": "B",
            "target_shifts": 10,
            "work_percentage": 100,
            "work_periods": "",
            "mandatory_days": "",
            "days_off": "",
        },
    ]
    scheduler = _build_scheduler(workers)
    # TargetCalculator recalculates target_shifts based on availability; force
    # the fixed values under test after construction.
    scheduler.workers_data[0]["target_shifts"] = 10
    scheduler.workers_data[1]["target_shifts"] = 10
    metrics = OptimizationMetrics(scheduler)

    # A is exactly 10% below target (9/10); B is exactly on target.
    scheduler.worker_assignments["A"] = {datetime(2026, 3, i) for i in range(1, 10)}
    scheduler.worker_assignments["B"] = {datetime(2026, 3, i) for i in range(10, 20)}

    assert metrics._calculate_workload_balance_score() == 100.0


def test_workload_balance_score_penalizes_beyond_10_percent():
    """Deviations exceeding the 10% deadzone must still reduce the score."""
    workers = [
        {
            "id": "A",
            "target_shifts": 10,
            "work_percentage": 100,
            "work_periods": "",
            "mandatory_days": "",
            "days_off": "",
        },
    ]
    scheduler = _build_scheduler(workers)
    scheduler.workers_data[0]["target_shifts"] = 10
    metrics = OptimizationMetrics(scheduler)

    # 30% deviation (7/10) -> 20% excess beyond the 10% deadzone.
    scheduler.worker_assignments["A"] = {datetime(2026, 3, i) for i in range(1, 8)}

    assert metrics._calculate_workload_balance_score() < 100.0


def test_workload_balance_score_excludes_mandatory_shifts():
    """target_shifts excludes mandatory shifts: 4 free + 2 mandatory with target 4 is on target."""
    workers = [
        {
            "id": "A",
            "target_shifts": 4,
            "work_percentage": 100,
            "work_periods": "",
            "mandatory_days": "",
            "days_off": "",
        },
    ]
    scheduler = _build_scheduler(workers)
    scheduler.workers_data[0]["target_shifts"] = 4
    scheduler.workers_data[0]["mandatory_days"] = "05-03-2026;10-03-2026"
    metrics = OptimizationMetrics(scheduler)

    free_dates = {datetime(2026, 3, d) for d in (1, 15, 20, 25)}
    scheduler.worker_assignments["A"] = free_dates | {datetime(2026, 3, 5), datetime(2026, 3, 10)}

    assert metrics._calculate_workload_balance_score() == 100.0


def _weekend_worker_scheduler():
    """March 2026 has 13 weekend days of 31 (fixed posts), so an objective of 31 shifts
    means 13 weekend shifts."""
    scheduler = _build_scheduler(
        [
            {
                "id": "A",
                "target_shifts": 10,
                "work_percentage": 100,
                "work_periods": "",
                "mandatory_days": "",
                "days_off": "",
            },
        ]
    )
    scheduler.workers_data[0]["_raw_target"] = 31
    weekend_dates = sorted(d for d in scheduler.schedule if scheduler.date_utils.is_weekend_day(d, set()))
    assert len(weekend_dates) == 13
    return scheduler, weekend_dates


def test_weekend_balance_score_counts_mandatory_weekends_as_assigned():
    """Mandatory weekends count in the actual weekend total like any other weekend."""
    scheduler, weekend_dates = _weekend_worker_scheduler()
    scheduler.worker_assignments["A"] = set(weekend_dates)
    scheduler.workers_data[0]["mandatory_days"] = ";".join(d.strftime("%d-%m-%Y") for d in weekend_dates[:3])

    assert OptimizationMetrics(scheduler)._calculate_weekend_balance_score() == 100.0


def test_weekend_balance_score_no_penalty_within_15_percent():
    """12 weekends against an objective of 13 (7.7%) must not penalize."""
    scheduler, weekend_dates = _weekend_worker_scheduler()
    scheduler.worker_assignments["A"] = set(weekend_dates[:12])

    assert OptimizationMetrics(scheduler)._calculate_weekend_balance_score() == 100.0


def test_weekend_balance_score_penalizes_beyond_15_percent():
    """9 weekends against an objective of 13 (31%) is beyond the deadzone."""
    scheduler, weekend_dates = _weekend_worker_scheduler()
    scheduler.worker_assignments["A"] = set(weekend_dates[:9])

    assert OptimizationMetrics(scheduler)._calculate_weekend_balance_score() < 100.0


def test_post_rotation_score_no_last_post_worker_treated_as_no_deviation():
    """Workers with no_last_post must be exempt from post-rotation penalties."""
    workers = [
        {
            "id": "A",
            "target_shifts": 10,
            "work_percentage": 100,
            "work_periods": "",
            "mandatory_days": "",
            "days_off": "",
            "no_last_post": True,
        },
    ]
    scheduler = _build_scheduler(workers, num_shifts=3)
    metrics = OptimizationMetrics(scheduler)

    # A is always assigned to post 0 and never to post 2 (last post) - a
    # structurally "unbalanced" post distribution that must not be penalized.
    dates = [datetime(2026, 3, i) for i in range(1, 11)]
    scheduler.worker_assignments["A"] = set(dates)
    for d in dates:
        scheduler.schedule[d][0] = "A"

    assert metrics._calculate_post_rotation_score() == 100.0


def test_post_rotation_score_only_last_post_worker_treated_as_no_deviation():
    """Workers with only_last_post must be exempt from post-rotation penalties."""
    workers = [
        {
            "id": "A",
            "target_shifts": 10,
            "work_percentage": 100,
            "work_periods": "",
            "mandatory_days": "",
            "days_off": "",
            "only_last_post": True,
        },
    ]
    scheduler = _build_scheduler(workers, num_shifts=3)
    metrics = OptimizationMetrics(scheduler)

    dates = [datetime(2026, 3, i) for i in range(1, 11)]
    scheduler.worker_assignments["A"] = set(dates)
    for d in dates:
        scheduler.schedule[d][2] = "A"

    assert metrics._calculate_post_rotation_score() == 100.0


def test_post_rotation_score_no_penalty_within_20_percent():
    """Regular workers (no post restriction) tolerate <= 20% post-distribution deviation."""
    workers = [
        {
            "id": "A",
            "target_shifts": 10,
            "work_percentage": 100,
            "work_periods": "",
            "mandatory_days": "",
            "days_off": "",
        },
    ]
    scheduler = _build_scheduler(workers, num_shifts=2)
    metrics = OptimizationMetrics(scheduler)

    # 10 shifts split 5.5/4.5 avg -> use 6/4 split: expected=5 per post,
    # mard = (|6-5| + |4-5|) / 2 / 5 = 0.2 (exactly 20%, at the deadzone boundary).
    dates = [datetime(2026, 3, i) for i in range(1, 11)]
    scheduler.worker_assignments["A"] = set(dates)
    for i, d in enumerate(dates):
        post = 0 if i < 6 else 1
        scheduler.schedule[d][post] = "A"

    assert metrics._calculate_post_rotation_score() == 100.0


def test_post_rotation_score_penalizes_beyond_20_percent():
    """Regular workers must still be penalized once deviation exceeds 20%."""
    workers = [
        {
            "id": "A",
            "target_shifts": 10,
            "work_percentage": 100,
            "work_periods": "",
            "mandatory_days": "",
            "days_off": "",
        },
    ]
    scheduler = _build_scheduler(workers, num_shifts=2)
    metrics = OptimizationMetrics(scheduler)

    # 8/2 split: expected=5 per post, mard = (|8-5| + |2-5|) / 2 / 5 = 0.6 (60%).
    dates = [datetime(2026, 3, i) for i in range(1, 11)]
    scheduler.worker_assignments["A"] = set(dates)
    for i, d in enumerate(dates):
        post = 0 if i < 8 else 1
        scheduler.schedule[d][post] = "A"

    assert metrics._calculate_post_rotation_score() < 100.0


def _worker(worker_id: str, **overrides):
    worker = {
        "id": worker_id,
        "name": worker_id,
        "target_shifts": 10,
        "_raw_target": 10,
        "work_percentage": 100,
        "work_periods": "",
        "mandatory_days": "",
        "days_off": "",
        "incompatible_with": [],
        "auto_calculate_shifts": True,
        "has_cadence": False,
        "no_last_post": False,
        "only_last_post": False,
    }
    worker.update(overrides)
    return worker


def test_workload_score_uses_clique_scaled_target():
    """Incompat clique capacity caps the scoring target so structural shortfall is neutral."""
    from saldo27.schedule_builder import ScheduleBuilder
    from saldo27.structural_score import StructuralScoreBaselines

    workers = [
        _worker("A", incompatible_with=["B", "C"], target_shifts=20, _raw_target=20),
        _worker("B", incompatible_with=["A", "C"], target_shifts=20, _raw_target=20),
        _worker("C", incompatible_with=["A", "B"], target_shifts=20, _raw_target=20),
    ]
    scheduler = _build_scheduler(workers, num_shifts=2)
    scheduler.schedule_builder = ScheduleBuilder(scheduler)
    for w in scheduler.workers_data:
        w["target_shifts"] = 20
        w["_raw_target"] = 20

    baselines = StructuralScoreBaselines(scheduler)
    # 10-day March slice in _build_scheduler is full month — capacity large.
    # Force a short horizon by using only early March days for capacity via end_date...
    # With full March (~31 days) and targets 20*3=60 > 31, scaled < 20.
    scaled = {w["id"]: baselines.scoring_shift_target(w) for w in scheduler.workers_data}
    assert all(v < 20 for v in scaled.values())

    days = list(scheduler._get_date_range(scheduler.start_date, scheduler.end_date))
    cursor = 0
    packed = {}
    for wid, target in scaled.items():
        n = round(target)
        packed[wid] = set(days[cursor : cursor + n])
        cursor += n
    scheduler.worker_assignments = packed

    assert OptimizationMetrics(scheduler)._calculate_workload_balance_score() == 100.0


def test_post_rotation_spillover_rosell_from_no_last_not_penalized():
    """A free worker taking more Rosell because peers are no_last must not be penalised."""
    from saldo27.schedule_builder import ScheduleBuilder

    workers = [
        _worker("FREE", target_shifts=8, _raw_target=8),
        _worker("NL1", no_last_post=True, target_shifts=8, _raw_target=8),
        _worker("NL2", no_last_post=True, target_shifts=8, _raw_target=8),
    ]
    scheduler = _build_scheduler(workers, num_shifts=2)
    scheduler.schedule_builder = ScheduleBuilder(scheduler)
    for w in scheduler.workers_data:
        w["target_shifts"] = 8
        w["_raw_target"] = 8

    # FREE always on last post; NL1/NL2 on first — structural spillover pattern.
    days = [datetime(2026, 3, i) for i in range(1, 9)]
    for i, day in enumerate(days):
        nl = "NL1" if i % 2 == 0 else "NL2"
        scheduler.schedule[day] = [nl, "FREE"]
    scheduler._synchronize_tracking_data()

    assert OptimizationMetrics(scheduler)._calculate_post_rotation_score() == 100.0


def test_fill_rate_structural_empty_counts_as_filled():
    """A hole nobody can take (all no_last on Rosell) must not reduce fill score."""
    from saldo27.schedule_builder import ScheduleBuilder

    workers = [
        _worker("A", no_last_post=True),
        _worker("B", no_last_post=True),
    ]
    scheduler = _build_scheduler(workers, num_shifts=2)
    scheduler.schedule_builder = ScheduleBuilder(scheduler)

    day = datetime(2026, 3, 1)
    # Post 0 filled; last post empty and unfillable (both no_last).
    scheduler.schedule[day] = ["A", None]
    scheduler.worker_assignments = {"A": {day}, "B": set()}
    # B also assigned elsewhere so not available? Actually B could take post 0 only.
    # Last post: both A (already on day) and B (no_last) cannot → structural.

    metrics = OptimizationMetrics(scheduler)
    # Only score the one day for clarity: clear other days' empties by removing them.
    only = {day: scheduler.schedule[day]}
    scheduler.schedule = only
    assert metrics._calculate_fill_rate_score() == 100.0


def test_fill_rate_fillable_empty_still_penalizes():
    """An empty slot a free worker could take must still reduce fill score."""
    from saldo27.schedule_builder import ScheduleBuilder

    workers = [
        _worker("A"),
        _worker("B"),
    ]
    scheduler = _build_scheduler(workers, num_shifts=2)
    scheduler.schedule_builder = ScheduleBuilder(scheduler)

    day = datetime(2026, 3, 1)
    scheduler.schedule = {day: ["A", None]}
    scheduler.worker_assignments = {"A": {day}, "B": set()}

    assert OptimizationMetrics(scheduler)._calculate_fill_rate_score() < 100.0


def test_get_score_breakdown_matches_overall_score():
    """Breakdown overall must match calculate_overall_schedule_score and expose weights."""
    workers = [
        _worker("A"),
        _worker("B"),
    ]
    scheduler = _build_scheduler(workers, num_shifts=2)
    day = datetime(2026, 3, 2)
    scheduler.schedule = {day: ["A", "B"]}
    scheduler.worker_assignments = {"A": {day}, "B": {day}}
    scheduler.workers_data[0]["target_shifts"] = 1
    scheduler.workers_data[1]["target_shifts"] = 1

    metrics = OptimizationMetrics(scheduler)
    breakdown = metrics.get_score_breakdown()

    assert set(breakdown["components"]) == set(OptimizationMetrics.SCORE_WEIGHTS)
    assert set(breakdown["weights"]) == set(OptimizationMetrics.SCORE_WEIGHTS)
    assert 0.0 <= breakdown["overall"] <= 100.0
    assert breakdown["overall"] == metrics.calculate_overall_schedule_score()


def _assign_days(scheduler, worker_id: str, days: list[datetime], post: int = 0) -> None:
    from saldo27.schedule_builder import ScheduleBuilder

    if getattr(scheduler, "schedule_builder", None) is None:
        scheduler.schedule_builder = ScheduleBuilder(scheduler)
    for day in list(scheduler.schedule):
        slots = list(scheduler.schedule[day])
        while len(slots) < scheduler.num_shifts:
            slots.append(None)
        scheduler.schedule[day] = slots
    for day in days:
        slots = list(scheduler.schedule.get(day) or [None] * scheduler.num_shifts)
        slots[post] = worker_id
        scheduler.schedule[day] = slots
    scheduler._synchronize_tracking_data()


def test_constraint_penalty_skips_dual_mandatory_gap_and_pattern():
    """Gap and 7/14 pairs forced by two mandatory dates must not penalise the score."""
    workers = [
        _worker(
            "A",
            mandatory_days="01-03-2026;02-03-2026;08-03-2026",
            target_shifts=0,
            _raw_target=3,
        )
    ]
    scheduler = _build_scheduler(workers)
    scheduler.gap_between_shifts = 3
    _assign_days(
        scheduler,
        "A",
        [datetime(2026, 3, 1), datetime(2026, 3, 2), datetime(2026, 3, 8)],
    )

    assert OptimizationMetrics(scheduler)._calculate_constraint_violations_penalty() == 0.0


def test_constraint_penalty_skips_fixed_cadence_spacing():
    """Fixed-cadence workers are outside spacing limits for scoring."""
    workers = [
        _worker(
            "A",
            has_cadence=True,
            cadence_days=2,
            cadence_start_date="01-03-2026",
            target_shifts=0,
            _raw_target=5,
        )
    ]
    scheduler = _build_scheduler(workers, num_shifts=2)
    # Init expands cadence into mandatory_days; assign those dates.
    cadence_days = list(scheduler.date_utils.parse_dates(scheduler.workers_data[0]["mandatory_days"]))
    assert len(cadence_days) >= 2
    _assign_days(scheduler, "A", cadence_days[:4])

    assert OptimizationMetrics(scheduler)._calculate_constraint_violations_penalty() == 0.0


def test_constraint_penalty_skips_dual_mandatory_incompatibility():
    """Incompatible workers both mandatory on the same day: configuration, no penalty."""
    from saldo27.schedule_builder import ScheduleBuilder

    workers = [
        _worker("A", mandatory_days="01-03-2026", incompatible_with=["B"], target_shifts=0, _raw_target=1),
        _worker("B", mandatory_days="01-03-2026", incompatible_with=["A"], target_shifts=0, _raw_target=1),
    ]
    scheduler = _build_scheduler(workers)
    scheduler.schedule_builder = ScheduleBuilder(scheduler)
    day = datetime(2026, 3, 1)
    scheduler.schedule[day] = ["A", "B"]
    scheduler._synchronize_tracking_data()

    assert OptimizationMetrics(scheduler)._calculate_constraint_violations_penalty() == 0.0
    # Verificación must use the same exemption (locked_mandatory may be empty).
    from saldo27.application.generation_flow import collect_constraint_violations

    assert collect_constraint_violations(scheduler).get("incompatibilidades") == []


def test_workload_and_weekend_survive_incomplete_calendar_vs_full_objectives():
    """Revisión-like: few detected shifts vs full-period live targets must not floor at 0.

    Scale only rescues proportional underfill; keep weekend mass balanced so the
    fixture asserts the incomplete-calendar path (not genuine imbalance).
    """
    workers = [
        _worker("A", target_shifts=8, _raw_target=8),
        _worker("B", target_shifts=8, _raw_target=8),
    ]
    scheduler = _build_scheduler(workers)
    # Sparse calendar (~25% of shift objectives); one Sunday each + one weekday each.
    _assign_days(scheduler, "A", [datetime(2026, 3, 1), datetime(2026, 3, 3)])  # Sun + Tue
    _assign_days(scheduler, "B", [datetime(2026, 3, 8), datetime(2026, 3, 10)])  # Sun + Tue
    for worker in scheduler.workers_data:
        worker["target_shifts"] = 8
        worker["_raw_target"] = 8

    metrics = OptimizationMetrics(scheduler)
    workload = metrics._calculate_workload_balance_score()
    weekend = metrics._calculate_weekend_balance_score()
    assert workload > 0.0
    assert weekend > 0.0
    breakdown = metrics.get_score_breakdown()
    assert breakdown["components"]["workload_balance"] > 0.0
    assert breakdown["components"]["weekend_balance"] > 0.0


def test_incomplete_calendar_scale_still_exposes_weekend_imbalance():
    """Underfill scale must not hide real weekend skew between workers."""
    workers = [
        _worker("A", target_shifts=8, _raw_target=8),
        _worker("B", target_shifts=8, _raw_target=8),
    ]
    scheduler = _build_scheduler(workers)
    _assign_days(scheduler, "A", [datetime(2026, 3, 1), datetime(2026, 3, 8)])  # both Sundays
    _assign_days(scheduler, "B", [datetime(2026, 3, 2), datetime(2026, 3, 9)])  # weekdays
    for worker in scheduler.workers_data:
        worker["target_shifts"] = 8
        worker["_raw_target"] = 8

    assert OptimizationMetrics(scheduler)._calculate_weekend_balance_score() == 0.0


def test_constraint_penalty_respects_variable_gap_periods():
    """Pairs inside a relaxed gap_periods window must not be scored as gap violations."""
    from saldo27.utilities import normalize_gap_periods

    workers = [_worker("A", target_shifts=2, _raw_target=2)]
    periods = normalize_gap_periods([{"start_date": datetime(2026, 3, 1), "end_date": datetime(2026, 3, 15), "gap": 2}])
    scheduler = _build_scheduler(workers)
    scheduler.gap_between_shifts = 4
    scheduler.gap_periods = periods
    # Mon–Wed (2 days): illegal under global gap 4, legal under period gap 2.
    # Avoid Thu–Sat so the gap-2 weekend bridge rule is not in play.
    _assign_days(scheduler, "A", [datetime(2026, 3, 2), datetime(2026, 3, 4)])

    assert OptimizationMetrics(scheduler)._calculate_constraint_violations_penalty() == 0.0

    scheduler.gap_periods = []
    assert OptimizationMetrics(scheduler)._calculate_constraint_violations_penalty() == 1.0


def test_constraint_penalty_same_spacing_ok_in_one_period_violation_in_another():
    """Same calendar spacing can be legal in a relaxed period and illegal outside it."""
    from saldo27.utilities import normalize_gap_periods

    workers = [_worker("A", target_shifts=4, _raw_target=4)]
    periods = normalize_gap_periods([{"start_date": datetime(2026, 3, 1), "end_date": datetime(2026, 3, 15), "gap": 2}])
    scheduler = _build_scheduler(workers)
    scheduler.gap_between_shifts = 4
    scheduler.gap_periods = periods
    # Spacing of 2 days twice: Mar 2–4 inside relaxed period (OK), Mar 20–22 outside (violation).
    _assign_days(
        scheduler,
        "A",
        [datetime(2026, 3, 2), datetime(2026, 3, 4), datetime(2026, 3, 20), datetime(2026, 3, 22)],
    )

    assert OptimizationMetrics(scheduler)._calculate_constraint_violations_penalty() == 1.0

    # Without periods both pairs violate the global floor of 4.
    scheduler.gap_periods = []
    assert OptimizationMetrics(scheduler)._calculate_constraint_violations_penalty() == 2.0


def test_constraint_penalty_stricter_gap_period_penalises_spacing_allowed_globally():
    """A period with a higher gap than the global floor must raise the scored threshold."""
    from saldo27.utilities import normalize_gap_periods

    workers = [_worker("A", target_shifts=2, _raw_target=2)]
    periods = normalize_gap_periods([{"start_date": datetime(2026, 3, 1), "end_date": datetime(2026, 3, 31), "gap": 5}])
    scheduler = _build_scheduler(workers)
    scheduler.gap_between_shifts = 2
    scheduler.gap_periods = periods
    # 3 days apart: OK under global gap 2, illegal under period gap 5.
    _assign_days(scheduler, "A", [datetime(2026, 3, 2), datetime(2026, 3, 5)])

    assert OptimizationMetrics(scheduler)._calculate_constraint_violations_penalty() == 1.0

    scheduler.gap_periods = []
    assert OptimizationMetrics(scheduler)._calculate_constraint_violations_penalty() == 0.0


def test_constraint_penalty_counts_gap2_weekend_when_period_sets_gap_two():
    """When gap_periods make the effective floor 2, the gap-2 weekend bridge is scored."""
    from saldo27.utilities import normalize_gap_periods

    workers = [_worker("A", target_shifts=2, _raw_target=2)]
    periods = normalize_gap_periods([{"start_date": datetime(2026, 3, 1), "end_date": datetime(2026, 3, 15), "gap": 2}])
    scheduler = _build_scheduler(workers)
    scheduler.gap_between_shifts = 4
    scheduler.gap_periods = periods
    # Thu 5 – Sat 7: spacing equals period gap 2 but bridges the weekend.
    _assign_days(scheduler, "A", [datetime(2026, 3, 5), datetime(2026, 3, 7)])

    assert OptimizationMetrics(scheduler)._calculate_constraint_violations_penalty() == 1.0


def test_constraint_penalty_counts_repairable_single_mandatory_gap():
    """One mandatory + one free day that violate gap still penalise (repair moves the free day)."""
    workers = [_worker("A", mandatory_days="01-03-2026", target_shifts=1, _raw_target=2)]
    scheduler = _build_scheduler(workers)
    scheduler.gap_between_shifts = 3
    _assign_days(scheduler, "A", [datetime(2026, 3, 1), datetime(2026, 3, 2)])

    assert OptimizationMetrics(scheduler)._calculate_constraint_violations_penalty() == 1.0


def test_apply_scoring_objectives_aligns_revision_display_with_generation():
    """Display targets (= detected counts) must not change the scored workload vs generation."""
    from saldo27.optimization_metrics import apply_scoring_objectives, restore_scoring_objectives

    gen_workers = [_worker("A", name="Ana", mandatory_days="01-03-2026;15-03-2026", target_shifts=4, _raw_target=6)]
    gen = _build_scheduler(gen_workers)
    days = [datetime(2026, 3, d) for d in (1, 8, 15, 22, 25, 28)]
    _assign_days(gen, "A", days)
    gen.workers_data[0]["target_shifts"] = 4
    gen.workers_data[0]["_raw_target"] = 6
    gen_score = OptimizationMetrics(gen).get_score_breakdown()

    # Simulate Revisión display: calendar id = name, targets overwritten to counts.
    rev_workers = [
        _worker(
            "Ana",
            name="Ana",
            mandatory_days="01-03-2026;15-03-2026",
            target_shifts=6,
            _raw_target=6,
            _score_target_shifts=4,
            _score_raw_target=6,
        )
    ]
    rev = _build_scheduler(rev_workers)
    _assign_days(rev, "Ana", days)
    rev.workers_data[0]["target_shifts"] = 6
    rev.workers_data[0]["_raw_target"] = 6
    rev.workers_data[0]["_score_target_shifts"] = 4
    rev.workers_data[0]["_score_raw_target"] = 6

    naive = OptimizationMetrics(rev).get_score_breakdown()
    assert naive["components"]["workload_balance"] != gen_score["components"]["workload_balance"]

    snapshot = apply_scoring_objectives(rev, gen.workers_data)
    aligned = OptimizationMetrics(rev).get_score_breakdown()
    restore_scoring_objectives(snapshot)

    assert aligned["components"]["workload_balance"] == gen_score["components"]["workload_balance"]
    assert aligned["overall"] == gen_score["overall"]
    # Display targets restored for Estadísticas.
    assert rev.workers_data[0]["target_shifts"] == 6


def test_apply_scoring_objectives_uses_stamped_score_fields_without_source():
    """When no live workers are passed, ``_score_*`` stamps still drive scoring."""
    from saldo27.optimization_metrics import apply_scoring_objectives, restore_scoring_objectives

    workers = [
        _worker(
            "A",
            target_shifts=6,
            _raw_target=6,
            _score_target_shifts=4,
            _score_raw_target=6,
            mandatory_days="01-03-2026;15-03-2026",
        )
    ]
    scheduler = _build_scheduler(workers)
    days = [datetime(2026, 3, d) for d in (1, 8, 15, 22, 25, 28)]
    _assign_days(scheduler, "A", days)
    scheduler.workers_data[0]["target_shifts"] = 6
    scheduler.workers_data[0]["_raw_target"] = 6
    scheduler.workers_data[0]["_score_target_shifts"] = 4
    scheduler.workers_data[0]["_score_raw_target"] = 6

    snapshot = apply_scoring_objectives(scheduler, [])
    assert scheduler.workers_data[0]["target_shifts"] == 4
    assert scheduler.workers_data[0]["_raw_target"] == 6
    restore_scoring_objectives(snapshot)
    assert scheduler.workers_data[0]["target_shifts"] == 6
