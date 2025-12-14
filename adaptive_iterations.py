import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
from collections import defaultdict
import statistics

class AdaptiveIterationManager:
    """Manages iteration counts for scheduling optimization based on problem complexity"""
    
    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.start_time = None
        self.convergence_threshold = 5  # Stop if no improvement for 5 iterations
        self.max_time_minutes = 6  # Maximum optimization time in minutes
        
        # Historical optimization data for learning
        self.optimization_history: List[Dict] = []
        self.convergence_patterns: Dict[str, List[float]] = defaultdict(list)
        self.quality_metrics: Dict[str, float] = {}
        
        # Enhanced thresholds - now adaptive
        self.base_thresholds = {
            'excellent_score': 95.0,
            'good_score': 85.0,
            'acceptable_score': 75.0,
            'improvement_threshold': 0.1
        }
        
    def calculate_base_iterations(self):
        """Calculate base iteration count based on problem complexity"""
        num_workers = len(self.scheduler.workers_data)
        shifts_per_day = self.scheduler.num_shifts
        total_days = (self.scheduler.end_date - self.scheduler.start_date).days + 1
        
        # Calculate complexity factors
        base_complexity = num_workers * shifts_per_day * total_days
        
        # Count constraints that add complexity
        constraint_complexity = 0
        
        # Variable shifts add complexity
        if hasattr(self.scheduler, 'variable_shifts') and self.scheduler.variable_shifts:
            constraint_complexity += len(self.scheduler.variable_shifts) * 0.1
        
        # Incompatible workers add complexity
        incompatible_count = sum(1 for w in self.scheduler.workers_data 
                               if w.get('is_incompatible', False))
        constraint_complexity += incompatible_count * 0.2
        
        # Part-time workers add complexity
        part_time_count = sum(1 for w in self.scheduler.workers_data 
                            if w.get('work_percentage', 100) < 70)
        constraint_complexity += part_time_count * 0.15
        
        # Days off and mandatory days add complexity
        complex_schedules = sum(1 for w in self.scheduler.workers_data 
                              if w.get('days_off', '') or w.get('mandatory_days', ''))
        constraint_complexity += complex_schedules * 0.1
        
        # Calculate final complexity score
        total_complexity = base_complexity * (1 + constraint_complexity)
        
        logging.info(f"Complexity calculation: base={base_complexity}, "
                    f"constraint_factor={constraint_complexity:.2f}, "
                    f"total={total_complexity:.0f}")
        
        return total_complexity
    
    def calculate_adaptive_iterations(self):
        """Calculate adaptive iteration counts for different optimization phases with historical learning"""
        complexity = self.calculate_base_iterations()
        
        # Analyze historical convergence patterns to adjust iterations
        complexity_multiplier = self._calculate_complexity_multiplier()
        quality_factor = self._calculate_quality_adjustment_factor()
        
        # Base iteration calculations with enhanced logic
        if complexity < 1000:
            base_config = {
                'main_loops': 10,
                'fill_attempts': 8,
                'balance_iterations': 5,
                'weekend_passes': 5,
                'post_adjustment_iterations': 6
            }
        elif complexity < 5000:
            base_config = {
                'main_loops': 20,
                'fill_attempts': 16,
                'balance_iterations': 10,
                'weekend_passes': 10,
                'post_adjustment_iterations': 10
            }
        elif complexity < 15000:
            base_config = {
                'main_loops': 75,
                'fill_attempts': 60,
                'balance_iterations': 40,
                'weekend_passes': 30,
                'post_adjustment_iterations': 30
            }
        else:
            # Enhanced: Apply dynamic multipliers for complex schedules
            base_config = {
                'main_loops': 100,
                'fill_attempts': 80,
                'balance_iterations': 50,
                'weekend_passes': 40,
                'post_adjustment_iterations': 35
            }
        
        # Apply historical learning adjustments
        adjusted_config = self._apply_historical_adjustments(base_config, complexity_multiplier, quality_factor)
        
        # Adjust based on worker count with more nuanced scaling
        num_workers = len(self.scheduler.workers_data)
        worker_adjustment = self._calculate_worker_count_adjustment(num_workers)
        
        final_config = {}
        for key, value in adjusted_config.items():
            if key in ['main_loops', 'balance_iterations']:
                final_config[key] = max(2, int(value * worker_adjustment))
            else:
                final_config[key] = max(1, int(value * worker_adjustment))
        
        # Add enhanced configuration parameters
        final_config.update({
            'convergence_threshold': self._adaptive_convergence_threshold(),
            'complexity_score': complexity,
            'applied_multiplier': complexity_multiplier,
            'quality_factor': quality_factor,
            'worker_adjustment': worker_adjustment
        })
        
        logging.info(f"Enhanced adaptive config - Complexity: {complexity:.0f}, "
                    f"Multiplier: {complexity_multiplier:.2f}, Quality: {quality_factor:.2f}")
        
        return final_config
    
    def should_continue_optimization(self, current_iteration, iterations_without_improvement, 
                                   current_score, best_score, phase_name="optimization"):
        """Determine if optimization should continue based on various criteria with enhanced analysis"""
        
        # Check time limit
        if self.start_time:
            elapsed_minutes = (datetime.now() - self.start_time).total_seconds() / 60
            if elapsed_minutes > self.max_time_minutes:
                logging.info(f"Stopping {phase_name}: Time limit reached ({elapsed_minutes:.1f} min)")
                self._record_stop_reason("time_limit", current_score, elapsed_minutes)
                return False
        
        # Dynamic convergence threshold based on current performance
        dynamic_threshold = self._get_dynamic_convergence_threshold(current_score, best_score)
        
        # Check convergence
        if iterations_without_improvement >= dynamic_threshold:
            logging.info(f"Stopping {phase_name}: No improvement for {iterations_without_improvement} iterations "
                        f"(threshold: {dynamic_threshold})")
            self._record_stop_reason("convergence", current_score, iterations_without_improvement)
            return False
        
        # Enhanced score-based stopping with dynamic thresholds
        excellent_threshold = self._get_dynamic_score_threshold("excellent")
        if current_score >= excellent_threshold:
            logging.info(f"Stopping {phase_name}: Excellent score achieved ({current_score:.2f} >= {excellent_threshold:.1f})")
            self._record_stop_reason("excellent_score", current_score, current_iteration)
            return False
        
        # Check for diminishing returns
        if self._is_showing_diminishing_returns(current_score, best_score, iterations_without_improvement):
            logging.info(f"Stopping {phase_name}: Diminishing returns detected ({current_score:.2f})")
            self._record_stop_reason("diminishing_returns", current_score, iterations_without_improvement)
            return False
        
        return True
    
    def _get_dynamic_convergence_threshold(self, current_score: float, best_score: float) -> int:
        """Get dynamic convergence threshold based on current performance"""
        base_threshold = self.convergence_threshold
        
        # If we have a good score, be less patient
        if current_score >= self.base_thresholds['excellent_score']:
            return max(3, base_threshold - 2)
        elif current_score >= self.base_thresholds['good_score']:
            return max(4, base_threshold - 1)
        # If score is poor, be more patient
        elif current_score < self.base_thresholds['acceptable_score']:
            return base_threshold + 2
        else:
            return base_threshold
    
    def _get_dynamic_score_threshold(self, threshold_type: str) -> float:
        """Get dynamic score threshold based on historical performance"""
        base_threshold = self.base_thresholds.get(f"{threshold_type}_score", 95.0)
        
        # Adjust based on historical difficulty
        if self.optimization_history:
            recent_scores = [opt.get('final_quality', 80.0) for opt in self.optimization_history[-5:]]
            if recent_scores:
                avg_recent = statistics.mean(recent_scores)
                # If historically difficult, lower the threshold slightly
                if avg_recent < self.base_thresholds['good_score']:
                    return base_threshold * 0.95
                elif avg_recent > self.base_thresholds['excellent_score']:
                    return base_threshold * 1.02
        
        return base_threshold
    
    def _is_showing_diminishing_returns(self, current_score: float, best_score: float, 
                                      iterations_without_improvement: int) -> bool:
        """Detect if optimization is showing diminishing returns"""
        # If we've been stuck for a while and score is reasonable, consider stopping
        if (iterations_without_improvement >= 3 and 
            current_score >= self.base_thresholds['good_score'] and
            current_score >= best_score * 0.98):  # Within 2% of best
            return True
        
        # If we're spending too much time for marginal gains
        if (iterations_without_improvement >= self.convergence_threshold // 2 and
            current_score >= self.base_thresholds['acceptable_score']):
            return True
        
        return False
    
    def _record_stop_reason(self, reason: str, final_score: float, additional_info: float):
        """Record why optimization stopped for future analysis"""
        if not hasattr(self, 'stop_reasons_history'):
            self.stop_reasons_history = []
        
        self.stop_reasons_history.append({
            'reason': reason,
            'final_score': final_score,
            'additional_info': additional_info,
            'timestamp': datetime.now()
        })
    
    def _calculate_complexity_multiplier(self) -> float:
        """Calculate complexity multiplier based on historical convergence patterns"""
        if not self.convergence_patterns or len(self.optimization_history) < 3:
            return 1.0  # No history yet, use baseline
        
        # Analyze recent optimization performance
        recent_history = self.optimization_history[-5:]  # Last 5 optimizations
        
        # Calculate average convergence speed
        convergence_speeds = []
        for opt_record in recent_history:
            if 'convergence_speed' in opt_record and opt_record['convergence_speed'] > 0:
                convergence_speeds.append(opt_record['convergence_speed'])
        
        if not convergence_speeds:
            return 1.0
        
        avg_convergence_speed = statistics.mean(convergence_speeds)
        
        # If convergence is typically slow, increase iterations
        if avg_convergence_speed < 0.3:  # Slow convergence
            multiplier = 1.3
        elif avg_convergence_speed > 0.7:  # Fast convergence
            multiplier = 0.8
        else:  # Normal convergence
            multiplier = 1.0
        
        logging.debug(f"Convergence analysis: avg_speed={avg_convergence_speed:.3f}, multiplier={multiplier:.2f}")
        return multiplier
    
    def _calculate_quality_adjustment_factor(self) -> float:
        """Calculate quality adjustment based on historical solution quality"""
        if not self.optimization_history:
            return 1.0
        
        # Analyze quality trends in recent optimizations
        recent_qualities = [opt.get('final_quality', 80.0) for opt in self.optimization_history[-3:]]
        
        if not recent_qualities:
            return 1.0
        
        avg_quality = statistics.mean(recent_qualities)
        
        # If recent quality is consistently low, increase effort
        if avg_quality < self.base_thresholds['acceptable_score']:
            return 1.2  # Increase iterations for better quality
        elif avg_quality > self.base_thresholds['excellent_score']:
            return 0.9  # Can reduce iterations slightly
        else:
            return 1.0  # Maintain current effort
    
    def _apply_historical_adjustments(self, base_config: Dict, complexity_mult: float, quality_mult: float) -> Dict:
        """Apply historical learning adjustments to base configuration"""
        adjusted_config = {}
        
        for key, base_value in base_config.items():
            # Apply both complexity and quality multipliers
            adjustment_factor = complexity_mult * quality_mult
            
            # Some iterations benefit more from adjustments than others
            if key in ['main_loops', 'balance_iterations']:
                # These benefit most from historical adjustments
                adjusted_value = int(base_value * adjustment_factor)
            elif key in ['fill_attempts', 'weekend_passes']:
                # These benefit moderately from adjustments
                adjusted_value = int(base_value * (1.0 + 0.5 * (adjustment_factor - 1.0)))
            else:
                # Others get minimal adjustment
                adjusted_value = int(base_value * (1.0 + 0.2 * (adjustment_factor - 1.0)))
            
            adjusted_config[key] = max(1, adjusted_value)  # Ensure minimum of 1
        
        return adjusted_config
    
    def _calculate_worker_count_adjustment(self, num_workers: int) -> float:
        """Calculate worker count adjustment factor with improved scaling"""
        if num_workers > 50:
            return 1.4  # Large teams need more iterations
        elif num_workers > 30:
            return 1.2  # Medium-large teams
        elif num_workers > 15:
            return 1.0  # Standard teams
        elif num_workers > 8:
            return 0.9  # Small teams can be more efficient
        else:
            return 0.8  # Very small teams
    
    def _adaptive_convergence_threshold(self) -> int:
        """Calculate adaptive convergence threshold based on problem complexity"""
        # More complex problems might need more patience for convergence
        complexity = self.calculate_base_iterations()
        
        if complexity > 15000:
            return 7  # Be more patient with complex schedules
        elif complexity > 5000:
            return 6
        else:
            return 5  # Standard threshold for simpler schedules
    
    def record_optimization_session(self, session_data: Dict):
        """Record data from an optimization session for future learning"""
        session_record = {
            'timestamp': datetime.now(),
            'complexity_score': session_data.get('complexity_score', 0),
            'final_quality': session_data.get('final_quality', 0.0),
            'iterations_used': session_data.get('iterations_used', 0),
            'time_elapsed': session_data.get('time_elapsed', 0.0),
            'convergence_speed': session_data.get('convergence_speed', 0.0),
            'worker_count': len(self.scheduler.workers_data),
            'stop_reason': session_data.get('stop_reason', 'unknown'),
            'quality_metrics': session_data.get('quality_metrics', {})
        }
        
        self.optimization_history.append(session_record)
        
        # Keep only recent history to avoid memory bloat
        if len(self.optimization_history) > 20:
            self.optimization_history = self.optimization_history[-15:]  # Keep last 15
        
        # Update convergence patterns
        complexity_category = self._categorize_complexity(session_record['complexity_score'])
        self.convergence_patterns[complexity_category].append(session_record['convergence_speed'])
        
        logging.info(f"Recorded optimization session: quality={session_record['final_quality']:.1f}, "
                    f"iterations={session_record['iterations_used']}, "
                    f"time={session_record['time_elapsed']:.1f}min")
    
    def calculate_quality_metrics(self, scheduler_instance) -> Dict[str, float]:
        """Calculate comprehensive quality metrics for the current schedule"""
        metrics = {}
        
        if not hasattr(scheduler_instance, 'schedule') or not scheduler_instance.schedule:
            return metrics
        
        try:
            # Basic coverage metrics
            total_slots = sum(len(shifts) for shifts in scheduler_instance.schedule.values())
            filled_slots = sum(1 for shifts in scheduler_instance.schedule.values() 
                              for shift in shifts if shift is not None)
            metrics['coverage_percentage'] = (filled_slots / total_slots * 100) if total_slots > 0 else 0
            
            # Worker distribution quality
            worker_assignments = scheduler_instance.worker_assignments or {}
            assignment_counts = [len(assignments) for assignments in worker_assignments.values()]
            
            if assignment_counts:
                metrics['assignment_std_dev'] = statistics.stdev(assignment_counts) if len(assignment_counts) > 1 else 0
                metrics['assignment_balance'] = max(0, 100 - (metrics['assignment_std_dev'] / statistics.mean(assignment_counts) * 100))
                metrics['min_assignments'] = min(assignment_counts)
                metrics['max_assignments'] = max(assignment_counts)
                metrics['assignment_range'] = metrics['max_assignments'] - metrics['min_assignments']
            
            # Weekend and holiday distribution
            weekend_holiday_counts = self._calculate_weekend_holiday_distribution(scheduler_instance)
            if weekend_holiday_counts:
                metrics['weekend_balance'] = self._calculate_distribution_balance(weekend_holiday_counts)
            
            # Constraint satisfaction rate
            constraint_satisfaction = self._evaluate_constraint_satisfaction(scheduler_instance)
            metrics.update(constraint_satisfaction)
            
            # Overall quality score (weighted combination)
            metrics['overall_quality'] = self._calculate_overall_quality_score(metrics)
            
        except Exception as e:
            logging.warning(f"Error calculating quality metrics: {e}")
            metrics['overall_quality'] = 50.0  # Default fallback score
        
        return metrics
    
    def _calculate_weekend_holiday_distribution(self, scheduler_instance) -> Dict[str, int]:
        """Calculate weekend and holiday shift distribution"""
        weekend_holiday_counts = defaultdict(int)
        holidays_set = set(scheduler_instance.holidays) if hasattr(scheduler_instance, 'holidays') else set()
        
        for date, shifts in scheduler_instance.schedule.items():
            is_weekend_or_holiday = date.weekday() >= 5 or date in holidays_set
            if is_weekend_or_holiday:
                for worker_id in shifts:
                    if worker_id is not None:
                        weekend_holiday_counts[worker_id] += 1
        
        return dict(weekend_holiday_counts)
    
    def _calculate_distribution_balance(self, distribution: Dict[str, int]) -> float:
        """Calculate how balanced a distribution is (higher = more balanced)"""
        if not distribution:
            return 100.0
        
        values = list(distribution.values())
        if len(values) <= 1:
            return 100.0
        
        mean_val = statistics.mean(values)
        std_dev = statistics.stdev(values)
        
        # Convert to balance score (0-100, where 100 is perfectly balanced)
        if mean_val == 0:
            return 100.0
        
        balance_score = max(0, 100 - (std_dev / mean_val * 50))
        return balance_score
    
    def _evaluate_constraint_satisfaction(self, scheduler_instance) -> Dict[str, float]:
        """Evaluate how well constraints are satisfied"""
        metrics = {}
        
        # Gap constraint satisfaction
        gap_violations = self._count_gap_violations(scheduler_instance)
        total_assignments = sum(len(assignments) for assignments in scheduler_instance.worker_assignments.values())
        metrics['gap_compliance'] = max(0, 100 - (gap_violations / max(1, total_assignments) * 100))
        
        # Incompatibility constraint satisfaction
        incompatibility_violations = self._count_incompatibility_violations(scheduler_instance)
        total_shifts = sum(len(shifts) for shifts in scheduler_instance.schedule.values())
        metrics['incompatibility_compliance'] = max(0, 100 - (incompatibility_violations / max(1, total_shifts) * 50))
        
        # Target shifts satisfaction
        target_satisfaction = self._evaluate_target_satisfaction(scheduler_instance)
        metrics['target_satisfaction'] = target_satisfaction
        
        return metrics
    
    def _count_gap_violations(self, scheduler_instance) -> int:
        """Count violations of gap between shifts constraint"""
        violations = 0
        gap_days = getattr(scheduler_instance, 'gap_between_shifts', 1)
        
        for worker_id, assignments in scheduler_instance.worker_assignments.items():
            sorted_dates = sorted(assignments)
            for i in range(1, len(sorted_dates)):
                days_between = (sorted_dates[i] - sorted_dates[i-1]).days
                if days_between <= gap_days:
                    violations += 1
        
        return violations
    
    def _count_incompatibility_violations(self, scheduler_instance) -> int:
        """Count violations of incompatibility constraints"""
        violations = 0
        workers_data = getattr(scheduler_instance, 'workers_data', [])
        
        # Build incompatibility map
        incompatible_map = {}
        for worker in workers_data:
            worker_id = worker['id']
            incompatible_list = worker.get('incompatible_with', [])
            if isinstance(incompatible_list, str):
                incompatible_list = [incompatible_list] if incompatible_list else []
            incompatible_map[worker_id] = set(incompatible_list)
        
        # Check each day for incompatibility violations
        for date, shifts in scheduler_instance.schedule.items():
            workers_on_date = [w for w in shifts if w is not None]
            for i, worker1 in enumerate(workers_on_date):
                for worker2 in workers_on_date[i+1:]:
                    if (worker2 in incompatible_map.get(worker1, set()) or
                        worker1 in incompatible_map.get(worker2, set())):
                        violations += 1
        
        return violations
    
    def _evaluate_target_satisfaction(self, scheduler_instance) -> float:
        """Evaluate how well target shift assignments are met"""
        workers_data = getattr(scheduler_instance, 'workers_data', [])
        if not workers_data:
            return 100.0
        
        satisfaction_scores = []
        
        for worker in workers_data:
            worker_id = worker['id']
            target_shifts = worker.get('target_shifts', 0)
            actual_shifts = len(scheduler_instance.worker_assignments.get(worker_id, set()))
            
            if target_shifts == 0:
                satisfaction_scores.append(100.0)  # No target, perfect satisfaction
            else:
                # Calculate satisfaction as percentage (capped at 100%)
                satisfaction = min(100.0, (actual_shifts / target_shifts) * 100)
                satisfaction_scores.append(satisfaction)
        
        return statistics.mean(satisfaction_scores) if satisfaction_scores else 100.0
    
    def _calculate_overall_quality_score(self, metrics: Dict[str, float]) -> float:
        """Calculate weighted overall quality score"""
        weights = {
            'coverage_percentage': 0.25,
            'assignment_balance': 0.20,
            'weekend_balance': 0.15,
            'gap_compliance': 0.15,
            'incompatibility_compliance': 0.15,
            'target_satisfaction': 0.10
        }
        
        weighted_score = 0.0
        total_weight = 0.0
        
        for metric, weight in weights.items():
            if metric in metrics:
                weighted_score += metrics[metric] * weight
                total_weight += weight
        
        return weighted_score / total_weight if total_weight > 0 else 50.0
    
    def _categorize_complexity(self, complexity_score: float) -> str:
        """Categorize complexity score into bins for pattern analysis"""
        if complexity_score < 1000:
            return "simple"
        elif complexity_score < 5000:
            return "moderate"
        elif complexity_score < 15000:
            return "complex"
        else:
            return "very_complex"
    
    def calculate_adaptive_iterations_enhanced(self):
        """Enhanced version of calculate_adaptive_iterations with additional analysis"""
        # Este es un alias mejorado que incluye análisis adicional
        base_config = self.calculate_adaptive_iterations()
        
        # Añadir análisis adicional específico
        historical_analysis = self.analyze_historical_patterns()
        complexity_category = self._categorize_complexity(base_config.get('complexity_score', 0))
        
        # Enriquecer la configuración con información adicional
        enhanced_config = base_config.copy()
        enhanced_config.update({
            'complexity_category': complexity_category,
            'historical_analysis': historical_analysis,
            'enhancement_version': '2.0',
            'adaptive_features_enabled': True
        })
        
        return enhanced_config
    
    def start_optimization_timer(self):
        """Start the optimization timer"""
        self.start_time = datetime.now()
        logging.info(f"Starting optimization timer at {self.start_time}")
    
    def analyze_historical_patterns(self) -> Dict[str, Any]:
        """Analyze historical optimization patterns for insights"""
        if len(self.optimization_history) < 3:
            return {"status": "insufficient_data", "recommendations": []}
        
        patterns = {
            "status": "analysis_complete",
            "patterns_found": [],
            "recommendations": [],
            "statistics": {}
        }
        
        # Analyze convergence patterns by complexity
        complexity_analysis = self._analyze_convergence_by_complexity()
        patterns["complexity_analysis"] = complexity_analysis
        
        # Analyze quality trends
        quality_trends = self._analyze_quality_trends()
        patterns["quality_trends"] = quality_trends
        
        # Analyze time efficiency
        efficiency_analysis = self._analyze_time_efficiency()
        patterns["efficiency_analysis"] = efficiency_analysis
        
        # Generate recommendations based on patterns
        recommendations = self._generate_optimization_recommendations(complexity_analysis, quality_trends, efficiency_analysis)
        patterns["recommendations"] = recommendations
        
        logging.info(f"Historical pattern analysis complete. Found {len(patterns['patterns_found'])} patterns, "
                    f"generated {len(recommendations)} recommendations")
        
        return patterns
    
    def _analyze_convergence_by_complexity(self) -> Dict[str, Any]:
        """Analyze convergence patterns by complexity category"""
        analysis = {}
        
        for complexity_cat, convergence_speeds in self.convergence_patterns.items():
            if len(convergence_speeds) < 2:
                continue
                
            analysis[complexity_cat] = {
                "avg_convergence_speed": statistics.mean(convergence_speeds),
                "convergence_consistency": 1.0 - (statistics.stdev(convergence_speeds) / statistics.mean(convergence_speeds))
                if statistics.mean(convergence_speeds) > 0 else 0,
                "sample_size": len(convergence_speeds)
            }
        
        return analysis
    
    def _analyze_quality_trends(self) -> Dict[str, Any]:
        """Analyze quality trends over recent optimizations"""
        recent_history = self.optimization_history[-10:]  # Last 10 optimizations
        
        qualities = [opt['final_quality'] for opt in recent_history if 'final_quality' in opt]
        times = [opt['time_elapsed'] for opt in recent_history if 'time_elapsed' in opt]
        
        trends = {}
        
        if len(qualities) >= 3:
            # Simple trend analysis - compare recent vs older
            mid_point = len(qualities) // 2
            recent_avg = statistics.mean(qualities[mid_point:])
            older_avg = statistics.mean(qualities[:mid_point])
            
            trends["quality_trend"] = "improving" if recent_avg > older_avg else "declining" if recent_avg < older_avg else "stable"
            trends["recent_avg_quality"] = recent_avg
            trends["quality_variance"] = statistics.variance(qualities) if len(qualities) > 1 else 0
        
        if len(times) >= 3:
            trends["avg_optimization_time"] = statistics.mean(times)
            trends["time_consistency"] = statistics.stdev(times) if len(times) > 1 else 0
        
        return trends
    
    def _analyze_time_efficiency(self) -> Dict[str, Any]:
        """Analyze time efficiency patterns"""
        efficiency_data = []
        
        for opt in self.optimization_history:
            if 'final_quality' in opt and 'time_elapsed' in opt and opt['time_elapsed'] > 0:
                efficiency = opt['final_quality'] / opt['time_elapsed']  # Quality per minute
                efficiency_data.append({
                    'efficiency': efficiency,
                    'quality': opt['final_quality'],
                    'time': opt['time_elapsed'],
                    'complexity': opt.get('complexity_score', 0)
                })
        
        if not efficiency_data:
            return {}
        
        efficiencies = [data['efficiency'] for data in efficiency_data]
        
        return {
            "avg_efficiency": statistics.mean(efficiencies),
            "efficiency_trend": self._calculate_trend([data['efficiency'] for data in efficiency_data[-5:]]),
            "best_efficiency": max(efficiencies),
            "efficiency_consistency": statistics.stdev(efficiencies) if len(efficiencies) > 1 else 0
        }
    
    def _calculate_trend(self, values: List[float]) -> str:
        """Calculate trend direction for a series of values"""
        if len(values) < 3:
            return "insufficient_data"
        
        # Simple linear trend detection
        mid_point = len(values) // 2
        recent_avg = statistics.mean(values[mid_point:])
        older_avg = statistics.mean(values[:mid_point])
        
        diff = recent_avg - older_avg
        threshold = statistics.stdev(values) * 0.1  # 10% of standard deviation
        
        if abs(diff) < threshold:
            return "stable"
        elif diff > 0:
            return "improving"
        else:
            return "declining"
    
    def _generate_optimization_recommendations(self, complexity_analysis: Dict, 
                                            quality_trends: Dict, efficiency_analysis: Dict) -> List[str]:
        """Generate optimization recommendations based on pattern analysis"""
        recommendations = []
        
        # Analyze convergence patterns
        for complexity_cat, analysis in complexity_analysis.items():
            if analysis['avg_convergence_speed'] < 0.3:
                recommendations.append(f"Consider increasing iterations for {complexity_cat} problems (slow convergence detected)")
            elif analysis['avg_convergence_speed'] > 0.8 and analysis['convergence_consistency'] > 0.7:
                recommendations.append(f"Consider reducing iterations for {complexity_cat} problems (fast, consistent convergence)")
        
        # Quality trend recommendations
        if quality_trends.get('quality_trend') == 'declining':
            recommendations.append("Quality trend declining - consider increasing optimization effort or reviewing constraints")
        elif quality_trends.get('quality_variance', 0) > 100:
            recommendations.append("High quality variance detected - consider more stable optimization parameters")
        
        # Efficiency recommendations
        if efficiency_analysis.get('efficiency_trend') == 'declining':
            recommendations.append("Efficiency declining - consider optimizing iteration parameters or algorithm improvements")
        
        # Time-based recommendations
        if quality_trends.get('avg_optimization_time', 0) > self.max_time_minutes * 0.9:
            recommendations.append("Optimizations frequently hitting time limits - consider increasing time limit or reducing iterations")
        
        return recommendations
    
    def get_optimization_config(self):
        """Get complete optimization configuration with enhanced adaptive features"""
        adaptive_config = self.calculate_adaptive_iterations()
        
        # Add additional enhanced configuration
        adaptive_config.update({
            'max_time_minutes': self.max_time_minutes,
            'early_stop_score': self._get_dynamic_score_threshold('excellent'),
            'good_score_threshold': self._get_dynamic_score_threshold('good'),
            'acceptable_score_threshold': self.base_thresholds['acceptable_score'],
            'last_post_balance_tolerance': 1.0,
            'weekday_balance_tolerance': 2,
            'weekday_balance_max_iterations': 5,
            'improvement_threshold': self.base_thresholds['improvement_threshold'],
            # New adaptive parameters
            'dynamic_convergence_enabled': True,
            'quality_based_stopping': True,
            'diminishing_returns_detection': True,
            'historical_learning_enabled': len(self.optimization_history) >= 3
        })
        
        # Add quality metrics configuration
        adaptive_config['quality_metrics_config'] = {
            'calculate_comprehensive_metrics': True,
            'track_constraint_satisfaction': True,
            'monitor_distribution_balance': True,
            'record_optimization_sessions': True
        }
        
        # Add historical analysis configuration
        if self.optimization_history:
            historical_insights = self.analyze_historical_patterns()
            adaptive_config['historical_insights'] = {
                'has_sufficient_history': len(self.optimization_history) >= 3,
                'recommendations_available': len(historical_insights.get('recommendations', [])) > 0,
                'pattern_analysis_complete': historical_insights.get('status') == 'analysis_complete'
            }
        
        logging.info("Enhanced adaptive iteration configuration generated:")
        for key, value in adaptive_config.items():
            if isinstance(value, dict):
                logging.info(f"  {key}: {len(value)} sub-parameters")
            else:
                logging.info(f"  {key}: {value}")
        
        return adaptive_config
    
    def get_optimization_summary(self) -> Dict[str, Any]:
        """Get a comprehensive summary of optimization performance and learning"""
        summary = {
            'total_optimizations_recorded': len(self.optimization_history),
            'optimization_config': self.get_optimization_config(),
            'historical_patterns': self.analyze_historical_patterns() if len(self.optimization_history) >= 3 else {},
            'current_thresholds': self.base_thresholds.copy(),
            'performance_metrics': {}
        }
        
        if self.optimization_history:
            recent_performance = self.optimization_history[-5:]  # Last 5 optimizations
            summary['performance_metrics'] = {
                'avg_recent_quality': statistics.mean([opt['final_quality'] for opt in recent_performance]),
                'avg_recent_time': statistics.mean([opt['time_elapsed'] for opt in recent_performance]),
                'quality_consistency': statistics.stdev([opt['final_quality'] for opt in recent_performance])
                if len(recent_performance) > 1 else 0,
                'most_common_stop_reason': self._get_most_common_stop_reason()
            }
        
        return summary
    
    def _get_most_common_stop_reason(self) -> str:
        """Get the most common reason for optimization stopping"""
        if not hasattr(self, 'stop_reasons_history') or not self.stop_reasons_history:
            return "unknown"
        
        reasons = [stop['reason'] for stop in self.stop_reasons_history[-10:]]  # Last 10
        if not reasons:
            return "unknown"
        
        # Count occurrences
        reason_counts = defaultdict(int)
        for reason in reasons:
            reason_counts[reason] += 1
        
        return max(reason_counts.items(), key=lambda x: x[1])[0]
