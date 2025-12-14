"""
Predictive Optimizer for AI-Powered Workload Demand Forecasting System

This module implements AI-enhanced optimization that uses predictive insights
to pre-optimize schedules and identify potential conflicts before they occur.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any, Set
import copy

from exceptions import SchedulerError


class PredictiveOptimizer:
    """
    AI-enhanced optimizer that uses forecasts for proactive schedule optimization
    """
    
    def __init__(self, scheduler, predictive_analytics_engine=None):
        """
        Initialize the predictive optimizer
        
        Args:
            scheduler: The main Scheduler object
            predictive_analytics_engine: PredictiveAnalyticsEngine instance
        """
        self.scheduler = scheduler
        self.predictive_analytics = predictive_analytics_engine
        
        # Optimization configuration
        self.config = {
            'enable_proactive_optimization': True,
            'conflict_prediction_horizon': 14,  # Days ahead to predict conflicts
            'optimization_aggressiveness': 'moderate',  # conservative, moderate, aggressive
            'min_improvement_threshold': 0.05,  # Minimum improvement to apply changes
            'max_iterations': 100,
            'early_warning_threshold': 0.7  # Fill rate threshold for warnings
        }
        
        # Optimization state
        self.optimization_history = []
        self.predicted_conflicts = []
        self.recommended_adjustments = []
        
        logging.info("PredictiveOptimizer initialized")
    
    def predict_and_optimize(self, forecast_data: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Perform predictive optimization based on forecast data
        
        Args:
            forecast_data: Forecast data from predictive analytics
            
        Returns:
            Dictionary containing optimization results and recommendations
        """
        try:
            if not forecast_data and self.predictive_analytics:
                # Generate fresh forecasts if not provided
                forecast_result = self.predictive_analytics.generate_demand_forecasts()
                if forecast_result.get('status') == 'success':
                    forecast_data = forecast_result.get('forecasts')
            
            if not forecast_data:
                return {
                    'status': 'no_data',
                    'message': 'No forecast data available for optimization'
                }
            
            optimization_results = {
                'timestamp': datetime.now().isoformat(),
                'status': 'success',
                'predicted_conflicts': [],
                'optimization_recommendations': [],
                'parameter_adjustments': {},
                'early_warnings': [],
                'performance_predictions': {}
            }
            
            # Step 1: Predict potential conflicts
            conflicts = self._predict_conflicts(forecast_data)
            optimization_results['predicted_conflicts'] = conflicts
            
            # Step 2: Generate optimization recommendations
            recommendations = self._generate_optimization_recommendations(forecast_data, conflicts)
            optimization_results['optimization_recommendations'] = recommendations
            
            # Step 3: Recommend parameter adjustments
            parameter_adjustments = self._recommend_parameter_adjustments(forecast_data)
            optimization_results['parameter_adjustments'] = parameter_adjustments
            
            # Step 4: Generate early warnings
            early_warnings = self._generate_early_warnings(forecast_data)
            optimization_results['early_warnings'] = early_warnings
            
            # Step 5: Predict performance impact
            performance_predictions = self._predict_performance_impact(forecast_data)
            optimization_results['performance_predictions'] = performance_predictions
            
            # Store optimization results
            self.optimization_history.append(optimization_results)
            
            # Keep only last 50 optimization results
            if len(self.optimization_history) > 50:
                self.optimization_history = self.optimization_history[-50:]
            
            logging.info(f"Predictive optimization completed: {len(conflicts)} conflicts, {len(recommendations)} recommendations")
            
            return optimization_results
            
        except Exception as e:
            logging.error(f"Error in predictive optimization: {e}")
            return {
                'status': 'error',
                'message': str(e),
                'error_type': type(e).__name__
            }
    
    def _predict_conflicts(self, forecast_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Predict potential scheduling conflicts based on forecasts"""
        conflicts = []
        
        try:
            predictions = forecast_data.get('predictions', {})
            early_warnings = forecast_data.get('early_warnings', [])
            
            # Convert early warnings to conflict predictions
            for warning in early_warnings:
                if warning.get('type') in ['critical_understaffing', 'low_staffing']:
                    conflicts.append({
                        'type': 'staffing_shortage',
                        'date': warning.get('date'),
                        'severity': warning.get('severity', 'medium'),
                        'predicted_fill_rate': warning.get('predicted_fill_rate', 0),
                        'days_ahead': warning.get('days_ahead', 0),
                        'description': f"Predicted staffing shortage on {warning.get('date')}",
                        'confidence': 0.8  # High confidence for forecast-based predictions
                    })
            
            # Analyze trend-based conflicts
            trend_analysis = forecast_data.get('trend_analysis', {})
            fill_rate_trend = trend_analysis.get('fill_rate_trend', {})
            
            if (fill_rate_trend.get('direction') == 'decreasing' and 
                fill_rate_trend.get('strength', 0) > 0.1):
                conflicts.append({
                    'type': 'declining_performance',
                    'severity': 'medium',
                    'trend_slope': fill_rate_trend.get('slope', 0),
                    'description': 'Declining fill rate trend detected',
                    'confidence': 0.7,
                    'projection_days': self.config['conflict_prediction_horizon']
                })
            
            # Identify resource capacity conflicts
            resource_planning = forecast_data.get('resource_planning', {})
            peak_periods = resource_planning.get('capacity_planning', {}).get('peak_periods', [])
            
            for date, fill_rate in peak_periods:
                if fill_rate > 0.98:  # Very high demand
                    conflicts.append({
                        'type': 'capacity_strain',
                        'date': date,
                        'severity': 'high',
                        'predicted_demand': fill_rate,
                        'description': f"Capacity strain predicted on {date} ({fill_rate:.1%} demand)",
                        'confidence': 0.75
                    })
            
            # Seasonal conflict prediction
            seasonal_patterns = forecast_data.get('seasonal_patterns', {})
            if seasonal_patterns and 'seasonal_strength' in seasonal_patterns.get('decomposition_summary', {}):
                seasonal_strength = seasonal_patterns['decomposition_summary']['seasonal_strength']
                if seasonal_strength > 0.15:
                    conflicts.append({
                        'type': 'seasonal_variation',
                        'severity': 'medium',
                        'seasonal_strength': seasonal_strength,
                        'description': 'High seasonal variation may cause scheduling conflicts',
                        'confidence': 0.6
                    })
            
        except Exception as e:
            logging.error(f"Error predicting conflicts: {e}")
            conflicts.append({
                'type': 'prediction_error',
                'severity': 'low',
                'description': 'Conflict prediction failed - manual review recommended',
                'confidence': 0.1
            })
        
        # Sort conflicts by severity and confidence
        severity_order = {'high': 0, 'medium': 1, 'low': 2}
        conflicts.sort(key=lambda c: (severity_order.get(c['severity'], 2), -c.get('confidence', 0)))
        
        return conflicts
    
    def _generate_optimization_recommendations(self, forecast_data: Dict[str, Any], 
                                             conflicts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Generate specific optimization recommendations"""
        recommendations = []
        
        try:
            # Address staffing shortage conflicts
            staffing_conflicts = [c for c in conflicts if c['type'] == 'staffing_shortage']
            if staffing_conflicts:
                for conflict in staffing_conflicts[:3]:  # Top 3 conflicts
                    date = conflict.get('date')
                    fill_rate = conflict.get('predicted_fill_rate', 0)
                    
                    recommendations.append({
                        'type': 'increase_staffing',
                        'target_date': date,
                        'priority': 'high',
                        'action': f'Recruit additional staff or adjust schedules for {date}',
                        'expected_improvement': (0.85 - fill_rate) if fill_rate < 0.85 else 0.1,
                        'implementation_complexity': 'medium'
                    })
            
            # Address capacity strain
            capacity_conflicts = [c for c in conflicts if c['type'] == 'capacity_strain']
            if capacity_conflicts:
                recommendations.append({
                    'type': 'optimize_capacity',
                    'priority': 'high',
                    'action': 'Implement flexible scheduling for peak demand periods',
                    'affected_dates': [c.get('date') for c in capacity_conflicts],
                    'expected_improvement': 0.15,
                    'implementation_complexity': 'high'
                })
            
            # Address declining performance trends
            declining_conflicts = [c for c in conflicts if c['type'] == 'declining_performance']
            if declining_conflicts:
                recommendations.append({
                    'type': 'investigate_decline',
                    'priority': 'medium',
                    'action': 'Investigate root causes of declining fill rates',
                    'expected_improvement': 0.1,
                    'implementation_complexity': 'low'
                })
            
            # Proactive optimization based on forecasts
            predictions = forecast_data.get('predictions', {})
            for method, prediction in predictions.items():
                if 'fill_rates' in prediction and 'dates' in prediction:
                    fill_rates = prediction['fill_rates']
                    dates = prediction['dates']
                    
                    # Identify improvement opportunities
                    for i, (date, fill_rate) in enumerate(zip(dates[:14], fill_rates[:14])):  # Next 2 weeks
                        if 0.75 <= fill_rate <= 0.9:  # Moderate demand - good for optimization
                            recommendations.append({
                                'type': 'proactive_optimization',
                                'target_date': date,
                                'priority': 'low',
                                'action': f'Optimize shift assignments for {date} to improve efficiency',
                                'current_predicted_rate': fill_rate,
                                'expected_improvement': 0.05,
                                'implementation_complexity': 'low'
                            })
                    break  # Use first valid prediction
            
            # Worker allocation recommendations
            resource_planning = forecast_data.get('resource_planning', {})
            optimization_opportunities = resource_planning.get('optimization_opportunities', [])
            
            for opportunity in optimization_opportunities:
                recommendations.append({
                    'type': 'resource_optimization',
                    'priority': 'medium',
                    'action': opportunity,
                    'expected_improvement': 0.08,
                    'implementation_complexity': 'medium'
                })
            
            # Constraint optimization recommendations
            if hasattr(self.scheduler, 'constraint_checker'):
                recommendations.append({
                    'type': 'constraint_optimization',
                    'priority': 'low',
                    'action': 'Review and optimize constraint parameters based on historical data',
                    'expected_improvement': 0.05,
                    'implementation_complexity': 'medium'
                })
            
        except Exception as e:
            logging.error(f"Error generating recommendations: {e}")
            recommendations.append({
                'type': 'error_recovery',
                'priority': 'low',
                'action': 'Manual optimization review recommended due to prediction errors',
                'implementation_complexity': 'high'
            })
        
        # Sort recommendations by priority and expected improvement
        priority_order = {'high': 0, 'medium': 1, 'low': 2}
        recommendations.sort(key=lambda r: (
            priority_order.get(r['priority'], 2),
            -r.get('expected_improvement', 0)
        ))
        
        return recommendations[:10]  # Top 10 recommendations
    
    def _recommend_parameter_adjustments(self, forecast_data: Dict[str, Any]) -> Dict[str, Any]:
        """Recommend adjustments to scheduling parameters based on predictions"""
        adjustments = {
            'iteration_parameters': {},
            'constraint_parameters': {},
            'optimization_settings': {},
            'confidence_score': 0.5
        }
        
        try:
            # Analyze forecast confidence for parameter recommendations
            confidence_score = forecast_data.get('confidence_score', {}).get('overall_score', 0.5)
            adjustments['confidence_score'] = confidence_score
            
            # Iteration parameter recommendations
            if hasattr(self.scheduler, 'adaptive_iterations'):
                # Recommend more iterations for complex periods
                predictions = forecast_data.get('predictions', {})
                avg_complexity = self._estimate_schedule_complexity(predictions)
                
                if avg_complexity > 0.8:
                    adjustments['iteration_parameters']['max_iterations'] = min(
                        self.config['max_iterations'] * 1.5,
                        self.config['max_iterations'] + 50
                    )
                    adjustments['iteration_parameters']['reason'] = 'High complexity periods predicted'
                elif avg_complexity < 0.4:
                    adjustments['iteration_parameters']['max_iterations'] = max(
                        self.config['max_iterations'] * 0.8,
                        self.config['max_iterations'] - 20
                    )
                    adjustments['iteration_parameters']['reason'] = 'Low complexity periods predicted'
            
            # Constraint parameter recommendations
            trend_analysis = forecast_data.get('trend_analysis', {})
            violation_trends = trend_analysis.get('constraint_violations', {})
            
            if isinstance(violation_trends, dict) and violation_trends.get('direction') == 'increasing':
                adjustments['constraint_parameters']['relaxation_factor'] = 1.1
                adjustments['constraint_parameters']['reason'] = 'Increasing constraint violations predicted'
            
            # Optimization aggressiveness recommendations
            risk_assessment = forecast_data.get('risk_assessment', {})
            risk_level = risk_assessment.get('overall_risk_level', 'medium')
            
            if risk_level == 'high':
                adjustments['optimization_settings']['aggressiveness'] = 'aggressive'
                adjustments['optimization_settings']['reason'] = 'High risk level requires aggressive optimization'
            elif risk_level == 'low':
                adjustments['optimization_settings']['aggressiveness'] = 'conservative'
                adjustments['optimization_settings']['reason'] = 'Low risk allows conservative approach'
            
            # Early termination recommendations
            if confidence_score > 0.8:
                adjustments['optimization_settings']['early_termination_threshold'] = 0.95
                adjustments['optimization_settings']['reason'] = 'High confidence allows early termination'
            elif confidence_score < 0.4:
                adjustments['optimization_settings']['early_termination_threshold'] = 0.98
                adjustments['optimization_settings']['reason'] = 'Low confidence requires thorough optimization'
            
        except Exception as e:
            logging.error(f"Error recommending parameter adjustments: {e}")
            adjustments['error'] = str(e)
        
        return adjustments
    
    def _estimate_schedule_complexity(self, predictions: Dict[str, Any]) -> float:
        """Estimate scheduling complexity based on predictions"""
        try:
            complexity_factors = []
            
            # Fill rate variance as complexity indicator
            for prediction in predictions.values():
                if 'fill_rates' in prediction and isinstance(prediction['fill_rates'], list):
                    fill_rates = prediction['fill_rates']
                    if len(fill_rates) > 1:
                        variance = sum((x - sum(fill_rates)/len(fill_rates))**2 for x in fill_rates) / len(fill_rates)
                        complexity_factors.append(variance * 10)  # Scale variance
                    break
            
            # Constraint violation complexity
            # This would need access to predicted constraint violations
            # For now, use a baseline complexity
            complexity_factors.append(0.5)
            
            return min(1.0, sum(complexity_factors) / len(complexity_factors)) if complexity_factors else 0.5
            
        except Exception as e:
            logging.error(f"Error estimating complexity: {e}")
            return 0.5
    
    def _generate_early_warnings(self, forecast_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate early warning alerts for optimization"""
        warnings = []
        
        try:
            # Use warnings from forecast data
            forecast_warnings = forecast_data.get('early_warnings', [])
            for warning in forecast_warnings:
                if warning.get('days_ahead', 999) <= 7:  # Next week
                    warnings.append({
                        'type': 'forecast_based',
                        'source': 'demand_forecast',
                        'severity': warning.get('severity', 'medium'),
                        'message': warning.get('message', ''),
                        'action_required': True,
                        'days_ahead': warning.get('days_ahead', 0)
                    })
            
            # Add optimization-specific warnings
            risk_assessment = forecast_data.get('risk_assessment', {})
            risk_level = risk_assessment.get('overall_risk_level', 'low')
            
            if risk_level == 'high':
                warnings.append({
                    'type': 'optimization_alert',
                    'source': 'predictive_optimizer',
                    'severity': 'high',
                    'message': 'High risk level detected - immediate optimization review required',
                    'action_required': True,
                    'recommended_action': 'Review and implement optimization recommendations'
                })
            
            # Performance degradation warnings
            trend_analysis = forecast_data.get('trend_analysis', {})
            if trend_analysis.get('fill_rate_trend', {}).get('direction') == 'decreasing':
                warnings.append({
                    'type': 'performance_warning',
                    'source': 'trend_analysis',
                    'severity': 'medium',
                    'message': 'Declining performance trend requires attention',
                    'action_required': True,
                    'recommended_action': 'Investigate causes and adjust scheduling strategy'
                })
            
        except Exception as e:
            logging.error(f"Error generating early warnings: {e}")
            warnings.append({
                'type': 'system_error',
                'source': 'predictive_optimizer',
                'severity': 'low',
                'message': 'Warning generation failed - manual monitoring recommended',
                'action_required': False
            })
        
        # Sort by severity and days ahead
        severity_order = {'high': 0, 'medium': 1, 'low': 2}
        warnings.sort(key=lambda w: (severity_order.get(w['severity'], 2), w.get('days_ahead', 999)))
        
        return warnings
    
    def _predict_performance_impact(self, forecast_data: Dict[str, Any]) -> Dict[str, Any]:
        """Predict performance impact of different optimization strategies"""
        performance_predictions = {
            'baseline_performance': {},
            'optimized_performance': {},
            'improvement_potential': {},
            'risk_factors': []
        }
        
        try:
            # Baseline performance from forecasts
            predictions = forecast_data.get('predictions', {})
            ensemble_prediction = predictions.get('ensemble', {})
            
            if 'fill_rates' in ensemble_prediction:
                fill_rates = ensemble_prediction['fill_rates']
                performance_predictions['baseline_performance'] = {
                    'avg_fill_rate': sum(fill_rates) / len(fill_rates),
                    'min_fill_rate': min(fill_rates),
                    'max_fill_rate': max(fill_rates),
                    'variance': sum((x - sum(fill_rates)/len(fill_rates))**2 for x in fill_rates) / len(fill_rates)
                }
            
            if 'efficiency' in ensemble_prediction:
                efficiency = ensemble_prediction['efficiency']
                performance_predictions['baseline_performance']['avg_efficiency'] = sum(efficiency) / len(efficiency)
            
            # Optimized performance predictions
            baseline_fill_rate = performance_predictions['baseline_performance'].get('avg_fill_rate', 0.8)
            baseline_efficiency = performance_predictions['baseline_performance'].get('avg_efficiency', 0.75)
            
            # Conservative optimization estimate
            optimized_fill_rate = min(0.98, baseline_fill_rate + 0.1)
            optimized_efficiency = min(0.95, baseline_efficiency + 0.08)
            
            performance_predictions['optimized_performance'] = {
                'projected_fill_rate': optimized_fill_rate,
                'projected_efficiency': optimized_efficiency,
                'confidence': 0.7
            }
            
            # Improvement potential
            performance_predictions['improvement_potential'] = {
                'fill_rate_improvement': optimized_fill_rate - baseline_fill_rate,
                'efficiency_improvement': optimized_efficiency - baseline_efficiency,
                'overall_improvement': (
                    (optimized_fill_rate - baseline_fill_rate) * 0.6 +
                    (optimized_efficiency - baseline_efficiency) * 0.4
                )
            }
            
            # Risk factors
            risk_assessment = forecast_data.get('risk_assessment', {})
            if risk_assessment.get('overall_risk_level') == 'high':
                performance_predictions['risk_factors'].append(
                    'High risk level may limit optimization effectiveness'
                )
            
            confidence_score = forecast_data.get('confidence_score', {}).get('overall_score', 0.5)
            if confidence_score < 0.6:
                performance_predictions['risk_factors'].append(
                    'Low forecast confidence may affect optimization accuracy'
                )
            
        except Exception as e:
            logging.error(f"Error predicting performance impact: {e}")
            performance_predictions['error'] = str(e)
        
        return performance_predictions
    
    def apply_recommended_adjustments(self, parameter_adjustments: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply recommended parameter adjustments to the scheduler
        
        Args:
            parameter_adjustments: Dictionary of recommended adjustments
            
        Returns:
            Dictionary with application results
        """
        try:
            results = {
                'applied_adjustments': [],
                'skipped_adjustments': [],
                'errors': []
            }
            
            # Apply iteration parameter adjustments
            iteration_params = parameter_adjustments.get('iteration_parameters', {})
            if iteration_params and hasattr(self.scheduler, 'adaptive_iterations'):
                try:
                    if 'max_iterations' in iteration_params:
                        old_value = getattr(self.scheduler.adaptive_iterations, 'max_iterations', None)
                        new_value = iteration_params['max_iterations']
                        
                        # Apply the adjustment (this would depend on the actual API)
                        # For now, just log the recommended change
                        results['applied_adjustments'].append({
                            'parameter': 'max_iterations',
                            'old_value': old_value,
                            'new_value': new_value,
                            'reason': iteration_params.get('reason', 'Predictive optimization')
                        })
                        
                        logging.info(f"Recommended iteration adjustment: max_iterations {old_value} -> {new_value}")
                        
                except Exception as e:
                    results['errors'].append(f"Failed to apply iteration parameters: {e}")
            
            # Apply optimization settings
            opt_settings = parameter_adjustments.get('optimization_settings', {})
            if opt_settings:
                for setting, value in opt_settings.items():
                    if setting != 'reason':
                        results['applied_adjustments'].append({
                            'parameter': f'optimization_{setting}',
                            'new_value': value,
                            'reason': opt_settings.get('reason', 'Predictive optimization')
                        })
            
            # Note: Actual implementation would depend on the scheduler's API
            # This is a framework for how adjustments would be applied
            
            return {
                'status': 'success',
                'results': results,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logging.error(f"Error applying adjustments: {e}")
            return {
                'status': 'error',
                'message': str(e),
                'error_type': type(e).__name__
            }
    
    def get_optimization_summary(self) -> Dict[str, Any]:
        """Get summary of recent optimization activities"""
        try:
            summary = {
                'recent_optimizations': len(self.optimization_history),
                'latest_optimization': None,
                'total_recommendations': 0,
                'high_priority_recommendations': 0,
                'predicted_conflicts': 0,
                'performance_trends': {}
            }
            
            if self.optimization_history:
                latest = self.optimization_history[-1]
                summary['latest_optimization'] = {
                    'timestamp': latest.get('timestamp'),
                    'conflicts_found': len(latest.get('predicted_conflicts', [])),
                    'recommendations_made': len(latest.get('optimization_recommendations', []))
                }
                
                # Aggregate statistics
                for opt in self.optimization_history[-10:]:  # Last 10 optimizations
                    summary['total_recommendations'] += len(opt.get('optimization_recommendations', []))
                    summary['predicted_conflicts'] += len(opt.get('predicted_conflicts', []))
                    
                    high_priority = sum(1 for rec in opt.get('optimization_recommendations', [])
                                      if rec.get('priority') == 'high')
                    summary['high_priority_recommendations'] += high_priority
            
            return {
                'status': 'success',
                'summary': summary
            }
            
        except Exception as e:
            logging.error(f"Error getting optimization summary: {e}")
            return {
                'status': 'error',
                'message': str(e)
            }