"""
Predictive Analytics Engine for AI-Powered Workload Demand Forecasting System

This is the main orchestration module that coordinates historical data collection,
demand forecasting, and predictive optimization to enhance the scheduler.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import json
from pathlib import Path

from exceptions import SchedulerError
from historical_data_manager import HistoricalDataManager
from demand_forecaster import DemandForecaster


class PredictiveAnalyticsEngine:
    """
    Main orchestration engine for predictive analytics capabilities
    """
    
    def __init__(self, scheduler, config: Dict[str, Any] = None):
        """
        Initialize the predictive analytics engine
        
        Args:
            scheduler: The main Scheduler object
            config: Configuration dictionary for predictive analytics
        """
        self.scheduler = scheduler
        self.config = config or {}
        
        # Set default configuration
        self.config.setdefault('enabled', True)
        self.config.setdefault('auto_collect_data', True)
        self.config.setdefault('forecast_horizon', 30)
        self.config.setdefault('min_history_days', 14)
        self.config.setdefault('storage_path', 'historical_data')
        self.config.setdefault('auto_optimize', False)
        
        # Initialize components
        self.historical_data_manager = HistoricalDataManager(
            scheduler, 
            storage_path=self.config['storage_path']
        )
        self.demand_forecaster = DemandForecaster(self.historical_data_manager)
        
        # Analytics state
        self.latest_forecasts = None
        self.analytics_cache = {}
        self.last_analysis_time = None
        
        logging.info("PredictiveAnalyticsEngine initialized")
    
    def is_enabled(self) -> bool:
        """Check if predictive analytics are enabled"""
        return self.config.get('enabled', True)
    
    def collect_and_store_current_data(self) -> Dict[str, Any]:
        """
        Collect current schedule data and store it for historical analysis
        
        Returns:
            Dictionary containing the collected data and storage status
        """
        if not self.is_enabled():
            return {'status': 'disabled', 'message': 'Predictive analytics disabled'}
        
        try:
            # Collect current schedule data
            current_data = self.historical_data_manager.collect_current_schedule_data()
            
            # Store the data
            self.historical_data_manager.store_historical_data(current_data)
            
            logging.info("Current schedule data collected and stored successfully")
            
            return {
                'status': 'success',
                'message': 'Data collected and stored successfully',
                'data_summary': {
                    'timestamp': current_data['timestamp'],
                    'efficiency_score': current_data['efficiency_score'],
                    'coverage_rate': current_data['coverage_metrics']['overall_coverage'],
                    'total_violations': current_data['constraint_metrics']['total_violations']
                }
            }
            
        except Exception as e:
            logging.error(f"Error collecting and storing data: {e}")
            return {
                'status': 'error',
                'message': str(e),
                'error_type': type(e).__name__
            }
    
    def generate_demand_forecasts(self, forecast_days: int = None) -> Dict[str, Any]:
        """
        Generate comprehensive demand forecasts
        
        Args:
            forecast_days: Number of days to forecast (defaults to config value)
            
        Returns:
            Dictionary containing forecast results and recommendations
        """
        if not self.is_enabled():
            return {'status': 'disabled', 'message': 'Predictive analytics disabled'}
        
        forecast_days = forecast_days or self.config['forecast_horizon']
        
        try:
            # Generate forecasts
            forecasts = self.demand_forecaster.generate_forecasts(forecast_days)
            
            # Cache the latest forecasts
            self.latest_forecasts = forecasts
            self.last_analysis_time = datetime.now()
            
            # Enhance with additional analytics
            enhanced_forecasts = self._enhance_forecasts_with_analytics(forecasts)
            
            logging.info(f"Demand forecasts generated for {forecast_days} days")
            
            return {
                'status': 'success',
                'forecasts': enhanced_forecasts,
                'generated_at': datetime.now().isoformat(),
                'forecast_horizon': forecast_days
            }
            
        except Exception as e:
            logging.error(f"Error generating forecasts: {e}")
            return {
                'status': 'error',
                'message': str(e),
                'error_type': type(e).__name__
            }
    
    def _enhance_forecasts_with_analytics(self, forecasts: Dict[str, Any]) -> Dict[str, Any]:
        """Enhance forecasts with additional analytics and insights"""
        try:
            enhanced = forecasts.copy()
            
            # Add risk assessment
            enhanced['risk_assessment'] = self._assess_forecast_risks(forecasts)
            
            # Add resource planning recommendations
            enhanced['resource_planning'] = self._generate_resource_planning(forecasts)
            
            # Add early warning indicators
            enhanced['early_warnings'] = self._generate_early_warnings(forecasts)
            
            # Add confidence scoring
            enhanced['confidence_score'] = self._calculate_forecast_confidence(forecasts)
            
            return enhanced
            
        except Exception as e:
            logging.error(f"Error enhancing forecasts: {e}")
            return forecasts
    
    def _assess_forecast_risks(self, forecasts: Dict[str, Any]) -> Dict[str, Any]:
        """Assess risks based on forecast predictions"""
        risk_assessment = {
            'overall_risk_level': 'low',
            'specific_risks': [],
            'risk_factors': {}
        }
        
        try:
            predictions = forecasts.get('predictions', {})
            
            # Check for low fill rates
            for method, prediction in predictions.items():
                if 'fill_rates' in prediction:
                    fill_rates = prediction['fill_rates']
                    if isinstance(fill_rates, list):
                        min_fill_rate = min(fill_rates)
                        avg_fill_rate = sum(fill_rates) / len(fill_rates)
                        
                        if min_fill_rate < 0.6:
                            risk_assessment['specific_risks'].append('Critical understaffing periods predicted')
                            risk_assessment['overall_risk_level'] = 'high'
                        elif avg_fill_rate < 0.8:
                            risk_assessment['specific_risks'].append('Below-target fill rates predicted')
                            if risk_assessment['overall_risk_level'] == 'low':
                                risk_assessment['overall_risk_level'] = 'medium'
                        
                        risk_assessment['risk_factors']['min_predicted_fill_rate'] = min_fill_rate
                        risk_assessment['risk_factors']['avg_predicted_fill_rate'] = avg_fill_rate
            
            # Check efficiency trends
            for method, prediction in predictions.items():
                if 'efficiency' in prediction:
                    efficiency = prediction['efficiency']
                    if isinstance(efficiency, list) and len(efficiency) > 1:
                        # Calculate trend
                        trend_slope = (efficiency[-1] - efficiency[0]) / len(efficiency)
                        if trend_slope < -0.05:
                            risk_assessment['specific_risks'].append('Declining efficiency trend predicted')
                            if risk_assessment['overall_risk_level'] != 'high':
                                risk_assessment['overall_risk_level'] = 'medium'
                        
                        risk_assessment['risk_factors']['efficiency_trend_slope'] = trend_slope
            
            # Seasonal risk factors
            seasonal_patterns = forecasts.get('seasonal_patterns', {})
            if seasonal_patterns and 'decomposition_summary' in seasonal_patterns:
                seasonal_strength = seasonal_patterns['decomposition_summary'].get('seasonal_strength', 0)
                if seasonal_strength > 0.2:
                    risk_assessment['specific_risks'].append('High seasonal variation detected')
                    risk_assessment['risk_factors']['seasonal_variation'] = seasonal_strength
            
            if not risk_assessment['specific_risks']:
                risk_assessment['specific_risks'].append('No significant risks identified in forecasts')
            
        except Exception as e:
            logging.error(f"Error in risk assessment: {e}")
            risk_assessment['specific_risks'].append('Risk assessment failed due to data processing error')
        
        return risk_assessment
    
    def _generate_resource_planning(self, forecasts: Dict[str, Any]) -> Dict[str, Any]:
        """Generate resource planning recommendations"""
        resource_planning = {
            'staffing_recommendations': [],
            'capacity_planning': {},
            'optimization_opportunities': []
        }
        
        try:
            predictions = forecasts.get('predictions', {})
            
            # Analyze capacity needs
            for method, prediction in predictions.items():
                if 'fill_rates' in prediction and isinstance(prediction['fill_rates'], list):
                    fill_rates = prediction['fill_rates']
                    dates = prediction.get('dates', [])
                    
                    # Identify peak and low demand periods
                    peak_periods = []
                    low_periods = []
                    
                    for i, (date, fill_rate) in enumerate(zip(dates, fill_rates)):
                        if fill_rate > 0.95:
                            peak_periods.append((date, fill_rate))
                        elif fill_rate < 0.7:
                            low_periods.append((date, fill_rate))
                    
                    if peak_periods:
                        resource_planning['staffing_recommendations'].append(
                            f"Prepare for high demand periods: {len(peak_periods)} days with >95% predicted demand"
                        )
                        resource_planning['capacity_planning']['peak_periods'] = peak_periods[:5]  # Top 5
                    
                    if low_periods:
                        resource_planning['optimization_opportunities'].append(
                            f"Consider flexible scheduling for {len(low_periods)} low-demand periods"
                        )
                        resource_planning['capacity_planning']['low_periods'] = low_periods[:5]  # Top 5
                    
                    # Overall capacity recommendation
                    avg_demand = sum(fill_rates) / len(fill_rates)
                    if avg_demand > 0.9:
                        resource_planning['staffing_recommendations'].append(
                            "Consider increasing staff capacity - high average demand predicted"
                        )
                    elif avg_demand < 0.75:
                        resource_planning['staffing_recommendations'].append(
                            "Current capacity may be adequate - moderate demand predicted"
                        )
                    
                    resource_planning['capacity_planning']['average_demand'] = avg_demand
                    break  # Use first valid prediction
            
            # Add general optimization opportunities
            if not resource_planning['optimization_opportunities']:
                resource_planning['optimization_opportunities'].append(
                    "Consider implementing adaptive scheduling based on demand patterns"
                )
            
        except Exception as e:
            logging.error(f"Error in resource planning: {e}")
            resource_planning['staffing_recommendations'].append(
                "Resource planning analysis failed - manual review recommended"
            )
        
        return resource_planning
    
    def _generate_early_warnings(self, forecasts: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate early warning indicators"""
        warnings = []
        
        try:
            predictions = forecasts.get('predictions', {})
            
            # Check for critical periods in next 7 days
            for method, prediction in predictions.items():
                if 'fill_rates' in prediction and 'dates' in prediction:
                    fill_rates = prediction['fill_rates']
                    dates = prediction['dates']
                    
                    for i, (date, fill_rate) in enumerate(zip(dates[:7], fill_rates[:7])):  # Next 7 days
                        if fill_rate < 0.6:
                            warnings.append({
                                'type': 'critical_understaffing',
                                'date': date,
                                'severity': 'high',
                                'predicted_fill_rate': fill_rate,
                                'message': f"Critical understaffing predicted for {date} ({fill_rate:.1%} fill rate)",
                                'days_ahead': i + 1
                            })
                        elif fill_rate < 0.75:
                            warnings.append({
                                'type': 'low_staffing',
                                'date': date,
                                'severity': 'medium',
                                'predicted_fill_rate': fill_rate,
                                'message': f"Below-target staffing predicted for {date} ({fill_rate:.1%} fill rate)",
                                'days_ahead': i + 1
                            })
                    break  # Use first valid prediction
            
            # Check trend warnings
            trend_analysis = forecasts.get('trend_analysis', {})
            fill_rate_trend = trend_analysis.get('fill_rate_trend', {})
            
            if fill_rate_trend.get('direction') == 'decreasing' and fill_rate_trend.get('strength', 0) > 0.1:
                warnings.append({
                    'type': 'declining_trend',
                    'severity': 'medium',
                    'message': f"Declining fill rate trend detected (slope: {fill_rate_trend.get('slope', 0):.3f})",
                    'trend_strength': fill_rate_trend.get('strength', 0)
                })
            
            # Sort warnings by severity and days ahead
            severity_order = {'high': 0, 'medium': 1, 'low': 2}
            warnings.sort(key=lambda w: (severity_order.get(w['severity'], 2), w.get('days_ahead', 999)))
            
        except Exception as e:
            logging.error(f"Error generating early warnings: {e}")
            warnings.append({
                'type': 'system_error',
                'severity': 'medium',
                'message': 'Early warning system error - manual monitoring recommended'
            })
        
        return warnings
    
    def _calculate_forecast_confidence(self, forecasts: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate confidence scores for forecasts"""
        confidence = {
            'overall_score': 0.5,
            'data_quality_score': 0.5,
            'model_agreement_score': 0.5,
            'factors': {}
        }
        
        try:
            metadata = forecasts.get('metadata', {})
            predictions = forecasts.get('predictions', {})
            
            # Data quality score
            data_points = metadata.get('data_points_used', 0)
            if data_points >= 30:
                confidence['data_quality_score'] = 0.9
            elif data_points >= 14:
                confidence['data_quality_score'] = 0.7
            elif data_points >= 7:
                confidence['data_quality_score'] = 0.5
            else:
                confidence['data_quality_score'] = 0.3
            
            confidence['factors']['data_points'] = data_points
            
            # Model agreement score
            methods_used = metadata.get('methods_used', [])
            if len(methods_used) >= 3:
                confidence['model_agreement_score'] = 0.8
            elif len(methods_used) >= 2:
                confidence['model_agreement_score'] = 0.6
            else:
                confidence['model_agreement_score'] = 0.4
            
            confidence['factors']['methods_count'] = len(methods_used)
            confidence['factors']['methods_used'] = methods_used
            
            # Calculate agreement between predictions if multiple methods
            if len(predictions) > 1:
                fill_rate_predictions = []
                for pred in predictions.values():
                    if 'fill_rates' in pred and isinstance(pred['fill_rates'], list):
                        fill_rate_predictions.append(pred['fill_rates'])
                
                if len(fill_rate_predictions) > 1:
                    # Calculate variance between predictions
                    min_length = min(len(pred) for pred in fill_rate_predictions)
                    variances = []
                    for i in range(min_length):
                        values = [pred[i] for pred in fill_rate_predictions]
                        variance = sum((v - sum(values)/len(values))**2 for v in values) / len(values)
                        variances.append(variance)
                    
                    avg_variance = sum(variances) / len(variances)
                    # Higher variance = lower confidence
                    agreement_score = max(0, 1 - avg_variance * 10)  # Scale variance
                    confidence['model_agreement_score'] = agreement_score
                    confidence['factors']['prediction_variance'] = avg_variance
            
            # Overall confidence
            confidence['overall_score'] = (
                confidence['data_quality_score'] * 0.6 +
                confidence['model_agreement_score'] * 0.4
            )
            
        except Exception as e:
            logging.error(f"Error calculating confidence: {e}")
            confidence['factors']['error'] = str(e)
        
        return confidence
    
    def get_predictive_insights(self) -> Dict[str, Any]:
        """
        Get comprehensive predictive insights and recommendations
        
        Returns:
            Dictionary containing insights, recommendations, and analytics
        """
        if not self.is_enabled():
            return {'status': 'disabled', 'message': 'Predictive analytics disabled'}
        
        try:
            insights = {
                'timestamp': datetime.now().isoformat(),
                'historical_summary': self.historical_data_manager.get_historical_summary(),
                'current_forecasts': self.latest_forecasts,
                'recommendations': [],
                'key_insights': [],
                'performance_metrics': {}
            }
            
            # Generate insights from available data
            if self.latest_forecasts:
                insights['key_insights'] = self._extract_key_insights(self.latest_forecasts)
                insights['recommendations'].extend(
                    self.latest_forecasts.get('recommendations', [])
                )
            else:
                insights['recommendations'].append(
                    "Generate forecasts to get predictive insights"
                )
            
            # Add historical insights
            historical_summary = insights['historical_summary']
            if historical_summary.get('status') == 'data_available':
                record_count = historical_summary.get('record_count', 0)
                if record_count > 0:
                    insights['key_insights'].append(
                        f"Historical analysis based on {record_count} data records"
                    )
                else:
                    insights['key_insights'].append("Limited historical data available")
            
            # Performance metrics
            insights['performance_metrics'] = self._calculate_system_performance()
            
            return {
                'status': 'success',
                'insights': insights
            }
            
        except Exception as e:
            logging.error(f"Error getting predictive insights: {e}")
            return {
                'status': 'error',
                'message': str(e),
                'error_type': type(e).__name__
            }
    
    def _extract_key_insights(self, forecasts: Dict[str, Any]) -> List[str]:
        """Extract key insights from forecast data"""
        insights = []
        
        try:
            # Analysis from risk assessment
            risk_assessment = forecasts.get('risk_assessment', {})
            risk_level = risk_assessment.get('overall_risk_level', 'unknown')
            insights.append(f"Overall risk level: {risk_level}")
            
            # Confidence insights
            confidence = forecasts.get('confidence_score', {})
            overall_confidence = confidence.get('overall_score', 0)
            if overall_confidence > 0.8:
                insights.append("High confidence in forecast predictions")
            elif overall_confidence > 0.6:
                insights.append("Moderate confidence in forecast predictions")
            else:
                insights.append("Low confidence in forecast predictions - collect more data")
            
            # Trend insights
            trend_analysis = forecasts.get('trend_analysis', {})
            fill_rate_trend = trend_analysis.get('fill_rate_trend', {})
            if fill_rate_trend.get('direction') == 'increasing':
                insights.append("Positive trend in fill rates detected")
            elif fill_rate_trend.get('direction') == 'decreasing':
                insights.append("Declining trend in fill rates requires attention")
            
            # Early warning insights
            early_warnings = forecasts.get('early_warnings', [])
            critical_warnings = [w for w in early_warnings if w.get('severity') == 'high']
            if critical_warnings:
                insights.append(f"{len(critical_warnings)} critical warnings in next week")
            
            # Methods used insight
            metadata = forecasts.get('metadata', {})
            methods_used = metadata.get('methods_used', [])
            if 'ensemble' in methods_used:
                insights.append("Multi-method ensemble forecasting enabled")
            elif len(methods_used) > 1:
                insights.append(f"Forecasting using {len(methods_used)} methods")
            
        except Exception as e:
            logging.error(f"Error extracting insights: {e}")
            insights.append("Error processing forecast insights")
        
        return insights
    
    def _calculate_system_performance(self) -> Dict[str, Any]:
        """Calculate performance metrics for the predictive system"""
        performance = {
            'analytics_enabled': self.is_enabled(),
            'last_analysis': self.last_analysis_time.isoformat() if self.last_analysis_time else None,
            'historical_data_status': 'unknown',
            'forecasting_capability': 'basic'
        }
        
        try:
            # Check historical data status
            summary = self.historical_data_manager.get_historical_summary()
            performance['historical_data_status'] = summary.get('status', 'unknown')
            
            if summary.get('status') == 'data_available':
                performance['historical_records'] = summary.get('record_count', 0)
            
            # Check forecasting capability
            try:
                import pandas as pd
                import sklearn
                performance['forecasting_capability'] = 'advanced'
            except ImportError:
                performance['forecasting_capability'] = 'basic'
            
            # Cache statistics
            performance['cache_size'] = len(self.analytics_cache)
            performance['has_cached_forecasts'] = self.latest_forecasts is not None
            
        except Exception as e:
            logging.error(f"Error calculating performance metrics: {e}")
            performance['error'] = str(e)
        
        return performance
    
    def get_optimization_suggestions(self, current_schedule_data: Dict[str, Any] = None) -> List[str]:
        """
        Get optimization suggestions based on predictive analytics
        
        Args:
            current_schedule_data: Current schedule data for analysis
            
        Returns:
            List of optimization suggestions
        """
        if not self.is_enabled():
            return ["Predictive analytics disabled - enable for optimization suggestions"]
        
        suggestions = []
        
        try:
            # Use latest forecasts if available
            if self.latest_forecasts:
                # Extract suggestions from forecasts
                forecast_recommendations = self.latest_forecasts.get('recommendations', [])
                suggestions.extend(forecast_recommendations)
                
                # Add forecast-based optimization suggestions
                early_warnings = self.latest_forecasts.get('early_warnings', [])
                critical_warnings = [w for w in early_warnings if w.get('severity') == 'high']
                
                if critical_warnings:
                    suggestions.append(
                        f"Immediate attention needed: {len(critical_warnings)} critical staffing issues predicted"
                    )
                
                # Resource planning suggestions
                resource_planning = self.latest_forecasts.get('resource_planning', {})
                staffing_recommendations = resource_planning.get('staffing_recommendations', [])
                suggestions.extend(staffing_recommendations)
            
            # Add data collection suggestions
            historical_summary = self.historical_data_manager.get_historical_summary()
            if historical_summary.get('status') != 'data_available':
                suggestions.append(
                    "Collect more historical data to improve prediction accuracy"
                )
            elif historical_summary.get('record_count', 0) < 30:
                suggestions.append(
                    "Continue collecting data for more accurate long-term forecasts"
                )
            
            # Add general optimization suggestions if none specific
            if not suggestions:
                suggestions.extend([
                    "Enable automatic data collection for continuous improvement",
                    "Generate demand forecasts to identify optimization opportunities",
                    "Review scheduling patterns to identify inefficiencies"
                ])
            
        except Exception as e:
            logging.error(f"Error getting optimization suggestions: {e}")
            suggestions.append("Error generating optimization suggestions - manual review recommended")
        
        return suggestions[:10]  # Limit to top 10 suggestions
    
    def auto_collect_data_if_enabled(self) -> bool:
        """
        Automatically collect data if auto-collection is enabled
        
        Returns:
            True if data was collected, False otherwise
        """
        if not self.is_enabled() or not self.config.get('auto_collect_data', True):
            return False
        
        try:
            result = self.collect_and_store_current_data()
            return result.get('status') == 'success'
        except Exception as e:
            logging.error(f"Auto data collection failed: {e}")
            return False
    
    def get_analytics_summary(self) -> Dict[str, Any]:
        """
        Get a summary of predictive analytics status and capabilities
        
        Returns:
            Dictionary with analytics summary
        """
        summary = {
            'enabled': self.is_enabled(),
            'components': {
                'historical_data_manager': True,
                'demand_forecaster': True,
                'predictive_optimizer': False  # Not implemented yet
            },
            'configuration': self.config.copy(),
            'status': {}
        }
        
        try:
            # Historical data status
            historical_summary = self.historical_data_manager.get_historical_summary()
            summary['status']['historical_data'] = historical_summary.get('status', 'unknown')
            summary['status']['data_records'] = historical_summary.get('record_count', 0)
            
            # Forecasting status
            summary['status']['latest_forecasts'] = self.latest_forecasts is not None
            summary['status']['last_analysis'] = (
                self.last_analysis_time.isoformat() 
                if self.last_analysis_time else None
            )
            
            # Capability assessment
            try:
                import pandas as pd
                import sklearn
                summary['capabilities'] = 'advanced'
            except ImportError:
                summary['capabilities'] = 'basic'
            
        except Exception as e:
            logging.error(f"Error getting analytics summary: {e}")
            summary['status']['error'] = str(e)
        
        return summary