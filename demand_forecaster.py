"""
Demand Forecaster for AI-Powered Workload Demand Forecasting System

This module implements time series forecasting and machine learning models
to predict future workload demands and resource needs.
"""

import logging
import warnings
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any, Union
import json
import numpy as np
from performance_cache import cached, memoize, time_function, monitor_performance

# ML Dependencies with graceful fallback
try:
    import pandas as pd
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.metrics import mean_absolute_error, mean_squared_error
    from sklearn.model_selection import train_test_split
    import statsmodels.api as sm
    from statsmodels.tsa.arima.model import ARIMA
    from statsmodels.tsa.seasonal import seasonal_decompose
    ML_AVAILABLE = True
except ImportError as e:
    ML_AVAILABLE = False
    logging.warning(f"ML libraries not available: {e}. Falling back to basic forecasting.")

from exceptions import SchedulerError


class DemandForecaster:
    """
    Advanced demand forecasting using multiple algorithms and ensemble methods
    """
    
    def __init__(self, historical_data_manager=None):
        """
        Initialize the demand forecaster
        
        Args:
            historical_data_manager: HistoricalDataManager instance for data access
        """
        self.historical_data_manager = historical_data_manager
        self.models = {}
        self.forecast_cache = {}
        self.prediction_accuracy = {}
        
        # Model configuration
        self.config = {
            'arima_order': (1, 1, 1),  # (p, d, q) for ARIMA
            'seasonal_periods': 7,      # Weekly seasonality
            'forecast_horizon': 30,     # Days to forecast ahead
            'min_data_points': 14,      # Minimum data points required
            'ensemble_weights': {
                'arima': 0.3,
                'seasonal': 0.3,
                'random_forest': 0.4
            }
        }
        
        # Performance optimization: Cache frequently used models
        self._model_cache = {}
        self._forecast_cache_ttl = 3600  # 1 hour
        
        logging.info("DemandForecaster initialized with performance optimizations")
        
        if not ML_AVAILABLE:
            logging.warning("Running in fallback mode without ML libraries")
    
    @time_function
    @monitor_performance("generate_forecasts")
    @cached(ttl=3600)  # Cache forecasts for 1 hour
    def generate_forecasts(self, forecast_days: int = 30) -> Dict[str, Any]:
        """
        Generate comprehensive demand forecasts (cached for performance)
        
        Args:
            forecast_days: Number of days to forecast ahead
            
        Returns:
            Dictionary containing forecasting results and predictions
        """
        try:
            if not self.historical_data_manager:
                return self._generate_basic_forecasts(forecast_days)
            
            # Get historical data for forecasting
            historical_data = self.historical_data_manager.get_data_for_forecasting(days_back=90)
            
            if historical_data['status'] != 'success' or not historical_data['data']:
                logging.warning("Insufficient historical data for advanced forecasting")
                return self._generate_basic_forecasts(forecast_days)
            
            data = historical_data['data']
            
            if not ML_AVAILABLE:
                return self._generate_basic_forecasts_with_data(data, forecast_days)
            
            # Generate forecasts using multiple methods
            forecasts = {
                'metadata': {
                    'forecast_date': datetime.now().isoformat(),
                    'forecast_horizon': forecast_days,
                    'data_points_used': len(data['timestamps']),
                    'methods_used': []
                },
                'predictions': {},
                'confidence_intervals': {},
                'seasonal_patterns': {},
                'trend_analysis': {},
                'recommendations': []
            }
            
            # Time series forecasting (cached)
            ts_forecast = self._generate_time_series_forecast_cached(data, forecast_days)
            if ts_forecast:
                forecasts['predictions'].update(ts_forecast['predictions'])
                forecasts['confidence_intervals'].update(ts_forecast['confidence_intervals'])
                forecasts['metadata']['methods_used'].append('time_series')
            
            # Seasonal decomposition (cached)
            seasonal_forecast = self._generate_seasonal_forecast_cached(data, forecast_days)
            if seasonal_forecast:
                forecasts['seasonal_patterns'] = seasonal_forecast
                forecasts['metadata']['methods_used'].append('seasonal_decomposition')
            
            # Machine learning forecast (cached)
            ml_forecast = self._generate_ml_forecast_cached(data, forecast_days)
            if ml_forecast:
                forecasts['predictions'].update(ml_forecast['predictions'])
                forecasts['metadata']['methods_used'].append('machine_learning')
            
            # Ensemble forecast combining methods
            ensemble_forecast = self._generate_ensemble_forecast(forecasts, forecast_days)
            if ensemble_forecast:
                forecasts['predictions']['ensemble'] = ensemble_forecast
                forecasts['metadata']['methods_used'].append('ensemble')
            
            # Generate recommendations based on forecasts
            forecasts['recommendations'] = self._generate_recommendations(forecasts)
            
            # Trend analysis
            forecasts['trend_analysis'] = self._analyze_trends(data)
            
            return forecasts
            
        except Exception as e:
            logging.error(f"Error generating forecasts: {e}")
            return {'error': str(e), 'fallback': self._generate_basic_forecasts(forecast_days)}
    
    @cached(ttl=1800)  # Cache for 30 minutes
    def _generate_time_series_forecast_cached(self, data: Dict[str, Any], forecast_days: int) -> Optional[Dict[str, Any]]:
        """Generate ARIMA-based time series forecasts (cached)"""
        return self._generate_time_series_forecast(data, forecast_days)
    
    @cached(ttl=1800)  # Cache for 30 minutes  
    def _generate_seasonal_forecast_cached(self, data: Dict[str, Any], forecast_days: int) -> Optional[Dict[str, Any]]:
        """Generate forecasts using seasonal decomposition (cached)"""
        return self._generate_seasonal_forecast(data, forecast_days)
    
    @cached(ttl=1800)  # Cache for 30 minutes
    def _generate_ml_forecast_cached(self, data: Dict[str, Any], forecast_days: int) -> Optional[Dict[str, Any]]:
        """Generate forecasts using machine learning models (cached)"""
        return self._generate_ml_forecast(data, forecast_days)
    
    def _generate_time_series_forecast(self, data: Dict[str, Any], forecast_days: int) -> Optional[Dict[str, Any]]:
        """Generate ARIMA-based time series forecasts"""
        try:
            # Convert to pandas time series
            timestamps = pd.to_datetime(data['timestamps'])
            fill_rates = np.array(data['fill_rates'])
            efficiency_scores = np.array(data['efficiency_scores'])
            
            if len(fill_rates) < self.config['min_data_points']:
                logging.warning("Insufficient data for ARIMA forecasting")
                return None
            
            # Create time series with daily frequency
            ts_fill_rates = pd.Series(fill_rates, index=timestamps)
            ts_efficiency = pd.Series(efficiency_scores, index=timestamps)
            
            # Fit ARIMA models
            arima_fill_rates = self._fit_arima_model(ts_fill_rates, 'fill_rates')
            arima_efficiency = self._fit_arima_model(ts_efficiency, 'efficiency')
            
            if not arima_fill_rates or not arima_efficiency:
                return None
            
            # Generate forecasts
            forecast_dates = pd.date_range(
                start=timestamps.max() + timedelta(days=1),
                periods=forecast_days,
                freq='D'
            )
            
            fill_rate_forecast = arima_fill_rates.forecast(steps=forecast_days)
            efficiency_forecast = arima_efficiency.forecast(steps=forecast_days)
            
            # Get confidence intervals
            fill_rate_ci = arima_fill_rates.get_forecast(steps=forecast_days).conf_int()
            efficiency_ci = arima_efficiency.get_forecast(steps=forecast_days).conf_int()
            
            return {
                'predictions': {
                    'arima_fill_rates': {
                        'dates': [date.strftime('%Y-%m-%d') for date in forecast_dates],
                        'values': fill_rate_forecast.tolist(),
                        'method': 'ARIMA'
                    },
                    'arima_efficiency': {
                        'dates': [date.strftime('%Y-%m-%d') for date in forecast_dates],
                        'values': efficiency_forecast.tolist(),
                        'method': 'ARIMA'
                    }
                },
                'confidence_intervals': {
                    'fill_rates': {
                        'lower': fill_rate_ci.iloc[:, 0].tolist(),
                        'upper': fill_rate_ci.iloc[:, 1].tolist()
                    },
                    'efficiency': {
                        'lower': efficiency_ci.iloc[:, 0].tolist(),
                        'upper': efficiency_ci.iloc[:, 1].tolist()
                    }
                }
            }
            
        except Exception as e:
            logging.error(f"Error in time series forecasting: {e}")
            return None
    
    def _fit_arima_model(self, time_series: pd.Series, metric_name: str):
        """Fit ARIMA model to time series data"""
        try:
            # Handle missing values
            time_series = time_series.fillna(method='forward').fillna(method='backward')
            
            if len(time_series) < self.config['min_data_points']:
                return None
            
            # Suppress warnings for cleaner output
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                
                # Try different ARIMA orders to find the best fit
                best_aic = float('inf')
                best_model = None
                
                for p in range(3):
                    for d in range(2):
                        for q in range(3):
                            try:
                                model = ARIMA(time_series, order=(p, d, q))
                                fitted_model = model.fit()
                                
                                if fitted_model.aic < best_aic:
                                    best_aic = fitted_model.aic
                                    best_model = fitted_model
                            except:
                                continue
                
                if best_model:
                    self.models[f'arima_{metric_name}'] = best_model
                    logging.info(f"ARIMA model fitted for {metric_name} with AIC: {best_aic:.2f}")
                    return best_model
                
        except Exception as e:
            logging.error(f"Error fitting ARIMA model for {metric_name}: {e}")
        
        return None
    
    def _generate_seasonal_forecast(self, data: Dict[str, Any], forecast_days: int) -> Optional[Dict[str, Any]]:
        """Generate forecasts using seasonal decomposition"""
        try:
            timestamps = pd.to_datetime(data['timestamps'])
            fill_rates = np.array(data['fill_rates'])
            
            if len(fill_rates) < self.config['seasonal_periods'] * 2:
                logging.warning("Insufficient data for seasonal decomposition")
                return None
            
            # Create time series
            ts = pd.Series(fill_rates, index=timestamps)
            ts = ts.fillna(method='forward').fillna(method='backward')
            
            # Perform seasonal decomposition
            decomposition = seasonal_decompose(
                ts, 
                model='additive', 
                period=self.config['seasonal_periods']
            )
            
            # Extract components
            trend = decomposition.trend.dropna()
            seasonal = decomposition.seasonal.dropna()
            
            # Project trend forward (simple linear extrapolation)
            if len(trend) >= 2:
                trend_slope = (trend.iloc[-1] - trend.iloc[-2]) / (trend.index[-1] - trend.index[-2]).days
                
                forecast_dates = pd.date_range(
                    start=timestamps.max() + timedelta(days=1),
                    periods=forecast_days,
                    freq='D'
                )
                
                # Extrapolate trend
                trend_forecast = []
                last_trend = trend.iloc[-1]
                for i, date in enumerate(forecast_dates):
                    days_ahead = (date - trend.index[-1]).days
                    trend_forecast.append(last_trend + trend_slope * days_ahead)
                
                # Get seasonal pattern for forecast dates
                seasonal_pattern = []
                for date in forecast_dates:
                    # Map to day of week for weekly seasonality
                    day_of_week = date.dayofweek
                    # Find corresponding seasonal value
                    seasonal_values = seasonal[seasonal.index.dayofweek == day_of_week]
                    if len(seasonal_values) > 0:
                        seasonal_pattern.append(seasonal_values.mean())
                    else:
                        seasonal_pattern.append(0)
                
                # Combine trend and seasonal
                seasonal_forecast = [t + s for t, s in zip(trend_forecast, seasonal_pattern)]
                
                return {
                    'forecast_values': seasonal_forecast,
                    'trend_component': trend_forecast,
                    'seasonal_component': seasonal_pattern,
                    'dates': [date.strftime('%Y-%m-%d') for date in forecast_dates],
                    'decomposition_summary': {
                        'trend_slope': trend_slope,
                        'seasonal_strength': seasonal.std(),
                        'residual_variance': decomposition.resid.var()
                    }
                }
            
        except Exception as e:
            logging.error(f"Error in seasonal forecasting: {e}")
        
        return None
    
    def _generate_ml_forecast(self, data: Dict[str, Any], forecast_days: int) -> Optional[Dict[str, Any]]:
        """Generate forecasts using machine learning models"""
        try:
            # Prepare features and targets
            timestamps = pd.to_datetime(data['timestamps'])
            features_df = pd.DataFrame({
                'timestamp': timestamps,
                'fill_rate': data['fill_rates'],
                'efficiency': data['efficiency_scores'],
                'violations': data['constraint_violations'],
                'coverage': data['coverage_rates']
            })
            
            # Create time-based features
            features_df['day_of_week'] = features_df['timestamp'].dt.dayofweek
            features_df['day_of_month'] = features_df['timestamp'].dt.day
            features_df['month'] = features_df['timestamp'].dt.month
            features_df['days_since_start'] = (features_df['timestamp'] - features_df['timestamp'].min()).dt.days
            
            # Create lag features
            for lag in [1, 7, 14]:  # 1 day, 1 week, 2 weeks
                features_df[f'fill_rate_lag_{lag}'] = features_df['fill_rate'].shift(lag)
                features_df[f'efficiency_lag_{lag}'] = features_df['efficiency'].shift(lag)
            
            # Remove rows with NaN values
            features_df = features_df.dropna()
            
            if len(features_df) < self.config['min_data_points']:
                logging.warning("Insufficient clean data for ML forecasting")
                return None
            
            # Prepare feature matrix and targets
            feature_cols = [col for col in features_df.columns if col not in ['timestamp', 'fill_rate', 'efficiency']]
            X = features_df[feature_cols]
            y_fill_rate = features_df['fill_rate']
            y_efficiency = features_df['efficiency']
            
            # Train Random Forest models
            rf_fill_rate = RandomForestRegressor(n_estimators=100, random_state=42)
            rf_efficiency = RandomForestRegressor(n_estimators=100, random_state=42)
            
            rf_fill_rate.fit(X, y_fill_rate)
            rf_efficiency.fit(X, y_efficiency)
            
            # Generate forecasts
            forecast_dates = pd.date_range(
                start=timestamps.max() + timedelta(days=1),
                periods=forecast_days,
                freq='D'
            )
            
            # Create feature matrix for forecast dates
            forecast_features = []
            last_known_values = features_df.iloc[-1]
            
            for i, date in enumerate(forecast_dates):
                forecast_row = {
                    'day_of_week': date.dayofweek,
                    'day_of_month': date.day,
                    'month': date.month,
                    'days_since_start': last_known_values['days_since_start'] + i + 1,
                    'violations': last_known_values['violations'],  # Assume constant
                    'coverage': last_known_values['coverage']  # Assume constant
                }
                
                # For lag features, use recent predictions or historical values
                if i == 0:
                    # First forecast day uses historical lags
                    forecast_row['fill_rate_lag_1'] = last_known_values['fill_rate']
                    forecast_row['efficiency_lag_1'] = last_known_values['efficiency']
                    
                    if len(features_df) >= 7:
                        forecast_row['fill_rate_lag_7'] = features_df.iloc[-7]['fill_rate']
                        forecast_row['efficiency_lag_7'] = features_df.iloc[-7]['efficiency']
                    else:
                        forecast_row['fill_rate_lag_7'] = last_known_values['fill_rate']
                        forecast_row['efficiency_lag_7'] = last_known_values['efficiency']
                    
                    if len(features_df) >= 14:
                        forecast_row['fill_rate_lag_14'] = features_df.iloc[-14]['fill_rate']
                        forecast_row['efficiency_lag_14'] = features_df.iloc[-14]['efficiency']
                    else:
                        forecast_row['fill_rate_lag_14'] = last_known_values['fill_rate']
                        forecast_row['efficiency_lag_14'] = last_known_values['efficiency']
                else:
                    # Use previous predictions for lag features
                    if i >= 1 and len(forecast_features) >= 1:
                        prev_fill_rate = rf_fill_rate.predict([forecast_features[-1]])[0]
                        prev_efficiency = rf_efficiency.predict([forecast_features[-1]])[0]
                        forecast_row['fill_rate_lag_1'] = prev_fill_rate
                        forecast_row['efficiency_lag_1'] = prev_efficiency
                    
                    # For longer lags, use historical or previous predictions
                    forecast_row['fill_rate_lag_7'] = forecast_row.get('fill_rate_lag_1', last_known_values['fill_rate'])
                    forecast_row['efficiency_lag_7'] = forecast_row.get('efficiency_lag_1', last_known_values['efficiency'])
                    forecast_row['fill_rate_lag_14'] = last_known_values['fill_rate']
                    forecast_row['efficiency_lag_14'] = last_known_values['efficiency']
                
                forecast_features.append([forecast_row[col] for col in feature_cols])
            
            # Make predictions
            X_forecast = np.array(forecast_features)
            fill_rate_predictions = rf_fill_rate.predict(X_forecast)
            efficiency_predictions = rf_efficiency.predict(X_forecast)
            
            # Store models for future use
            self.models['rf_fill_rate'] = rf_fill_rate
            self.models['rf_efficiency'] = rf_efficiency
            
            return {
                'predictions': {
                    'rf_fill_rates': {
                        'dates': [date.strftime('%Y-%m-%d') for date in forecast_dates],
                        'values': fill_rate_predictions.tolist(),
                        'method': 'Random Forest'
                    },
                    'rf_efficiency': {
                        'dates': [date.strftime('%Y-%m-%d') for date in forecast_dates],
                        'values': efficiency_predictions.tolist(),
                        'method': 'Random Forest'
                    }
                },
                'feature_importance': {
                    'fill_rate': dict(zip(feature_cols, rf_fill_rate.feature_importances_)),
                    'efficiency': dict(zip(feature_cols, rf_efficiency.feature_importances_))
                }
            }
            
        except Exception as e:
            logging.error(f"Error in ML forecasting: {e}")
            return None
    
    def _generate_ensemble_forecast(self, forecasts: Dict[str, Any], forecast_days: int) -> Optional[Dict[str, Any]]:
        """Combine forecasts from multiple methods using ensemble approach"""
        try:
            predictions = forecasts.get('predictions', {})
            
            # Collect all fill rate predictions
            fill_rate_forecasts = []
            efficiency_forecasts = []
            
            for key, prediction in predictions.items():
                if 'fill_rate' in key and 'values' in prediction:
                    fill_rate_forecasts.append(np.array(prediction['values']))
                elif 'efficiency' in key and 'values' in prediction:
                    efficiency_forecasts.append(np.array(prediction['values']))
            
            if not fill_rate_forecasts and not efficiency_forecasts:
                return None
            
            # Calculate weighted ensemble
            weights = self.config['ensemble_weights']
            
            ensemble_fill_rate = None
            ensemble_efficiency = None
            
            if fill_rate_forecasts:
                # Simple average if we don't have exactly the right number of methods
                if len(fill_rate_forecasts) == 1:
                    ensemble_fill_rate = fill_rate_forecasts[0]
                else:
                    ensemble_fill_rate = np.mean(fill_rate_forecasts, axis=0)
            
            if efficiency_forecasts:
                if len(efficiency_forecasts) == 1:
                    ensemble_efficiency = efficiency_forecasts[0]
                else:
                    ensemble_efficiency = np.mean(efficiency_forecasts, axis=0)
            
            # Generate dates
            forecast_dates = []
            if predictions:
                # Use dates from first available prediction
                first_prediction = next(iter(predictions.values()))
                if 'dates' in first_prediction:
                    forecast_dates = first_prediction['dates']
            
            if not forecast_dates:
                # Generate dates as fallback
                start_date = datetime.now() + timedelta(days=1)
                forecast_dates = [(start_date + timedelta(days=i)).strftime('%Y-%m-%d') 
                                for i in range(forecast_days)]
            
            result = {
                'dates': forecast_dates,
                'method': 'Ensemble'
            }
            
            if ensemble_fill_rate is not None:
                result['fill_rates'] = ensemble_fill_rate.tolist()
            
            if ensemble_efficiency is not None:
                result['efficiency'] = ensemble_efficiency.tolist()
            
            return result
            
        except Exception as e:
            logging.error(f"Error in ensemble forecasting: {e}")
            return None
    
    def _analyze_trends(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze trends in historical data"""
        try:
            fill_rates = np.array(data['fill_rates'])
            efficiency_scores = np.array(data['efficiency_scores'])
            
            trend_analysis = {
                'fill_rate_trend': self._calculate_trend(fill_rates),
                'efficiency_trend': self._calculate_trend(efficiency_scores),
                'correlation_analysis': {},
                'volatility_analysis': {}
            }
            
            # Correlation analysis
            if len(fill_rates) > 1 and len(efficiency_scores) > 1:
                correlation = np.corrcoef(fill_rates, efficiency_scores)[0, 1]
                trend_analysis['correlation_analysis'] = {
                    'fill_rate_efficiency_correlation': correlation,
                    'correlation_strength': self._interpret_correlation(correlation)
                }
            
            # Volatility analysis
            trend_analysis['volatility_analysis'] = {
                'fill_rate_volatility': np.std(fill_rates) if len(fill_rates) > 1 else 0,
                'efficiency_volatility': np.std(efficiency_scores) if len(efficiency_scores) > 1 else 0
            }
            
            return trend_analysis
            
        except Exception as e:
            logging.error(f"Error in trend analysis: {e}")
            return {}
    
    def _calculate_trend(self, values: np.ndarray) -> Dict[str, Any]:
        """Calculate trend direction and strength"""
        if len(values) < 2:
            return {'direction': 'unknown', 'strength': 0, 'slope': 0}
        
        # Simple linear regression to get trend
        x = np.arange(len(values))
        slope, intercept = np.polyfit(x, values, 1)
        
        # Determine trend direction and strength
        direction = 'increasing' if slope > 0 else 'decreasing' if slope < 0 else 'stable'
        strength = abs(slope) / (np.mean(values) + 1e-8)  # Normalize by mean value
        
        return {
            'direction': direction,
            'strength': strength,
            'slope': slope,
            'intercept': intercept,
            'r_squared': np.corrcoef(x, values)[0, 1] ** 2 if len(values) > 2 else 0
        }
    
    def _interpret_correlation(self, correlation: float) -> str:
        """Interpret correlation strength"""
        abs_corr = abs(correlation)
        if abs_corr > 0.8:
            return 'very_strong'
        elif abs_corr > 0.6:
            return 'strong'
        elif abs_corr > 0.4:
            return 'moderate'
        elif abs_corr > 0.2:
            return 'weak'
        else:
            return 'very_weak'
    
    def _generate_recommendations(self, forecasts: Dict[str, Any]) -> List[str]:
        """Generate actionable recommendations based on forecasts"""
        recommendations = []
        
        try:
            predictions = forecasts.get('predictions', {})
            trend_analysis = forecasts.get('trend_analysis', {})
            
            # Analyze ensemble forecast if available
            ensemble = predictions.get('ensemble', {})
            if 'fill_rates' in ensemble:
                fill_rates = ensemble['fill_rates']
                avg_predicted_fill_rate = np.mean(fill_rates)
                
                if avg_predicted_fill_rate < 0.8:
                    recommendations.append("Predicted low fill rates - consider recruiting additional staff")
                elif avg_predicted_fill_rate > 0.95:
                    recommendations.append("High predicted demand - prepare for potential overscheduling")
                
                # Check for declining trends
                if len(fill_rates) > 7:
                    recent_trend = np.polyfit(range(len(fill_rates)), fill_rates, 1)[0]
                    if recent_trend < -0.01:
                        recommendations.append("Declining fill rate trend detected - investigate scheduling issues")
            
            # Efficiency recommendations
            if 'efficiency' in ensemble:
                efficiency = ensemble['efficiency']
                avg_efficiency = np.mean(efficiency)
                
                if avg_efficiency < 0.7:
                    recommendations.append("Low efficiency predicted - review constraint configurations")
            
            # Trend-based recommendations
            fill_rate_trend = trend_analysis.get('fill_rate_trend', {})
            if fill_rate_trend.get('direction') == 'decreasing' and fill_rate_trend.get('strength', 0) > 0.1:
                recommendations.append("Strong declining trend in fill rates - immediate action recommended")
            
            # Volatility recommendations
            volatility = trend_analysis.get('volatility_analysis', {})
            if volatility.get('fill_rate_volatility', 0) > 0.2:
                recommendations.append("High fill rate volatility - consider more flexible scheduling policies")
            
            # Seasonal recommendations
            seasonal = forecasts.get('seasonal_patterns', {})
            if seasonal and 'seasonal_strength' in seasonal.get('decomposition_summary', {}):
                seasonal_strength = seasonal['decomposition_summary']['seasonal_strength']
                if seasonal_strength > 0.1:
                    recommendations.append("Strong seasonal patterns detected - optimize staffing for peak periods")
            
            if not recommendations:
                recommendations.append("Forecasts indicate stable demand - maintain current scheduling practices")
            
        except Exception as e:
            logging.error(f"Error generating recommendations: {e}")
            recommendations.append("Unable to generate specific recommendations due to data processing error")
        
        return recommendations
    
    def _generate_basic_forecasts(self, forecast_days: int) -> Dict[str, Any]:
        """Generate basic forecasts without historical data"""
        logging.info("Generating basic forecasts without historical data")
        
        # Use simple heuristics
        base_date = datetime.now()
        forecast_dates = [(base_date + timedelta(days=i)).strftime('%Y-%m-%d') 
                         for i in range(1, forecast_days + 1)]
        
        # Simple pattern: higher demand on weekdays, lower on weekends
        fill_rates = []
        efficiency_scores = []
        
        for i in range(forecast_days):
            date = base_date + timedelta(days=i + 1)
            if date.weekday() < 5:  # Weekday
                fill_rate = 0.85 + np.random.normal(0, 0.05)
                efficiency = 0.80 + np.random.normal(0, 0.05)
            else:  # Weekend
                fill_rate = 0.75 + np.random.normal(0, 0.05)
                efficiency = 0.75 + np.random.normal(0, 0.05)
            
            fill_rates.append(max(0, min(1, fill_rate)))
            efficiency_scores.append(max(0, min(1, efficiency)))
        
        return {
            'metadata': {
                'forecast_date': datetime.now().isoformat(),
                'forecast_horizon': forecast_days,
                'method': 'basic_heuristic',
                'data_points_used': 0
            },
            'predictions': {
                'basic_forecast': {
                    'dates': forecast_dates,
                    'fill_rates': fill_rates,
                    'efficiency': efficiency_scores,
                    'method': 'Basic Heuristic'
                }
            },
            'recommendations': [
                "Limited historical data available - collect more data for improved forecasting",
                "Consider implementing systematic data collection for better predictions"
            ]
        }
    
    def _generate_basic_forecasts_with_data(self, data: Dict[str, Any], forecast_days: int) -> Dict[str, Any]:
        """Generate basic forecasts using simple moving averages when ML libraries aren't available"""
        try:
            fill_rates = data['fill_rates']
            efficiency_scores = data['efficiency_scores']
            
            # Simple moving average forecast
            window_size = min(7, len(fill_rates))  # Use last week or available data
            
            if window_size > 0:
                avg_fill_rate = np.mean(fill_rates[-window_size:])
                avg_efficiency = np.mean(efficiency_scores[-window_size:])
                
                # Add some seasonal variation based on weekday
                base_date = datetime.now()
                forecast_dates = []
                forecast_fill_rates = []
                forecast_efficiency = []
                
                for i in range(forecast_days):
                    date = base_date + timedelta(days=i + 1)
                    forecast_dates.append(date.strftime('%Y-%m-%d'))
                    
                    # Apply weekday/weekend adjustment
                    if date.weekday() < 5:  # Weekday
                        fill_rate = avg_fill_rate * 1.05  # Slightly higher on weekdays
                        efficiency = avg_efficiency * 1.02
                    else:  # Weekend
                        fill_rate = avg_fill_rate * 0.95  # Slightly lower on weekends
                        efficiency = avg_efficiency * 0.98
                    
                    forecast_fill_rates.append(max(0, min(1, fill_rate)))
                    forecast_efficiency.append(max(0, min(1, efficiency)))
                
                return {
                    'metadata': {
                        'forecast_date': datetime.now().isoformat(),
                        'forecast_horizon': forecast_days,
                        'method': 'moving_average',
                        'data_points_used': len(fill_rates)
                    },
                    'predictions': {
                        'moving_average': {
                            'dates': forecast_dates,
                            'fill_rates': forecast_fill_rates,
                            'efficiency': forecast_efficiency,
                            'method': 'Moving Average'
                        }
                    },
                    'recommendations': [
                        "Using basic moving average forecast - install ML libraries for advanced forecasting",
                        f"Forecast based on {window_size} day moving average"
                    ]
                }
            
        except Exception as e:
            logging.error(f"Error in basic forecasting with data: {e}")
        
        return self._generate_basic_forecasts(forecast_days)
    
    def validate_forecast_accuracy(self, actual_data: Dict[str, Any], forecast_data: Dict[str, Any]) -> Dict[str, float]:
        """
        Validate forecast accuracy against actual results
        
        Args:
            actual_data: Actual scheduling results
            forecast_data: Previous forecast predictions
            
        Returns:
            Dictionary with accuracy metrics
        """
        try:
            if not ML_AVAILABLE:
                return {'error': 'ML libraries required for accuracy validation'}
            
            accuracy_metrics = {}
            
            # Extract predictions and actual values for comparison
            predictions = forecast_data.get('predictions', {})
            
            for method_name, prediction in predictions.items():
                if 'values' in prediction and isinstance(prediction['values'], list):
                    predicted_values = np.array(prediction['values'])
                    
                    # For this example, assume actual_data has corresponding values
                    # In real implementation, you'd match dates and extract corresponding actual values
                    if len(predicted_values) > 0:
                        # Placeholder for actual comparison - in real usage you'd match dates
                        # actual_values = extract_matching_actual_values(actual_data, prediction['dates'])
                        
                        # For now, just store the prediction for future validation
                        accuracy_metrics[method_name] = {
                            'predictions_count': len(predicted_values),
                            'mean_prediction': np.mean(predicted_values),
                            'std_prediction': np.std(predicted_values)
                        }
            
            return accuracy_metrics
            
        except Exception as e:
            logging.error(f"Error validating forecast accuracy: {e}")
            return {'error': str(e)}