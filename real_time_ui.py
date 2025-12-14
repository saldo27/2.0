"""
Real-time UI components for the scheduler application.
Provides live updates and interactive real-time features.
"""

from kivy.uix.boxlayout import BoxLayout
from kivy.uix.gridlayout import GridLayout
from kivy.uix.label import Label
from kivy.uix.button import Button
from kivy.uix.popup import Popup
from kivy.uix.scrollview import ScrollView
from kivy.uix.textinput import TextInput
from kivy.clock import Clock
from datetime import datetime
import logging
import json


class RealTimeStatusWidget(BoxLayout):
    """Widget to display real-time status and analytics"""
    
    def __init__(self, scheduler, **kwargs):
        super().__init__(**kwargs)
        self.scheduler = scheduler
        self.orientation = 'vertical'
        self.size_hint = (1, None)
        self.height = 150
        
        # Status labels
        self.status_label = Label(
            text='Real-time features: Loading...',
            size_hint_y=None,
            height=30
        )
        self.add_widget(self.status_label)
        
        self.analytics_label = Label(
            text='Analytics: Loading...',
            size_hint_y=None,
            height=30
        )
        self.add_widget(self.analytics_label)
        
        # Action buttons
        button_layout = BoxLayout(orientation='horizontal', size_hint_y=None, height=40)
        
        self.validate_btn = Button(text='Validate Now', size_hint_x=0.25)
        self.validate_btn.bind(on_press=self.validate_schedule)
        button_layout.add_widget(self.validate_btn)
        
        self.undo_btn = Button(text='Undo', size_hint_x=0.25)
        self.undo_btn.bind(on_press=self.undo_change)
        button_layout.add_widget(self.undo_btn)
        
        self.redo_btn = Button(text='Redo', size_hint_x=0.25)
        self.redo_btn.bind(on_press=self.redo_change)
        button_layout.add_widget(self.redo_btn)
        
        self.analytics_btn = Button(text='Analytics', size_hint_x=0.25)
        self.analytics_btn.bind(on_press=self.show_analytics)
        button_layout.add_widget(self.analytics_btn)
        
        self.add_widget(button_layout)
        
        # Change history
        self.history_label = Label(
            text='Recent changes: None',
            size_hint_y=None,
            height=50,
            text_size=(None, None),
            halign='left'
        )
        self.add_widget(self.history_label)
        
        # Schedule periodic updates
        Clock.schedule_interval(self.update_status, 5.0)  # Update every 5 seconds
        
        # Initial update
        Clock.schedule_once(lambda dt: self.update_status(dt), 0.1)
    
    def update_status(self, dt):
        """Update the real-time status display"""
        try:
            if not self.scheduler.is_real_time_enabled():
                self.status_label.text = 'Real-time features: Disabled'
                self.analytics_label.text = 'Enable real-time features in configuration'
                return
            
            # Get current analytics
            analytics = self.scheduler.get_real_time_analytics()
            
            if 'error' in analytics:
                self.status_label.text = f'Real-time features: Error - {analytics["error"]}'
                return
            
            # Update status
            active_ops = analytics.get('active_operations', {}).get('count', 0)
            coverage = analytics.get('schedule_metrics', {}).get('coverage_percentage', 0)
            
            self.status_label.text = f'Real-time: Active ({active_ops} operations, {coverage:.1f}% coverage)'
            
            # Update analytics
            workload = analytics.get('workload_distribution', {})
            avg_workload = workload.get('average_workload', 0)
            worker_count = workload.get('worker_count', 0)
            
            self.analytics_label.text = f'Workers: {worker_count}, Avg workload: {avg_workload:.1f}'
            
            # Update change history
            history = self.scheduler.get_change_history(limit=3)
            if 'error' not in history:
                changes = history.get('changes', [])
                if changes:
                    recent_change = changes[0]
                    change_desc = recent_change.get('description', 'Unknown change')
                    change_time = recent_change.get('timestamp', '')
                    self.history_label.text = f'Last change: {change_desc} ({change_time[:19]})'
                else:
                    self.history_label.text = 'Recent changes: None'
                
                # Update button states
                self.undo_btn.disabled = not history.get('can_undo', False)
                self.redo_btn.disabled = not history.get('can_redo', False)
            
        except Exception as e:
            logging.error(f"Error updating real-time status: {e}")
            self.status_label.text = f'Real-time features: Error updating status'
    
    def validate_schedule(self, instance):
        """Trigger real-time schedule validation"""
        try:
            if not self.scheduler.is_real_time_enabled():
                self.show_message('Real-time features not enabled')
                return
            
            result = self.scheduler.validate_schedule_real_time(quick_check=True)
            
            if result['success']:
                self.show_message(f'Validation passed: {result["message"]}')
            else:
                self.show_message(f'Validation failed: {result["message"]}')
                
        except Exception as e:
            logging.error(f"Error in real-time validation: {e}")
            self.show_message(f'Validation error: {str(e)}')
    
    def undo_change(self, instance):
        """Undo the last change"""
        try:
            if not self.scheduler.is_real_time_enabled():
                self.show_message('Real-time features not enabled')
                return
            
            result = self.scheduler.undo_last_change()
            
            if result['success']:
                self.show_message(f'Undid: {result["message"]}')
                # Update status immediately
                Clock.schedule_once(lambda dt: self.update_status(dt), 0.1)
            else:
                self.show_message(f'Undo failed: {result["message"]}')
                
        except Exception as e:
            logging.error(f"Error in undo: {e}")
            self.show_message(f'Undo error: {str(e)}')
    
    def redo_change(self, instance):
        """Redo the last undone change"""
        try:
            if not self.scheduler.is_real_time_enabled():
                self.show_message('Real-time features not enabled')
                return
            
            result = self.scheduler.redo_last_change()
            
            if result['success']:
                self.show_message(f'Redid: {result["message"]}')
                # Update status immediately
                Clock.schedule_once(lambda dt: self.update_status(dt), 0.1)
            else:
                self.show_message(f'Redo failed: {result["message"]}')
                
        except Exception as e:
            logging.error(f"Error in redo: {e}")
            self.show_message(f'Redo error: {str(e)}')
    
    def show_analytics(self, instance):
        """Show detailed analytics popup"""
        try:
            if not self.scheduler.is_real_time_enabled():
                self.show_message('Real-time features not enabled')
                return
            
            analytics = self.scheduler.get_real_time_analytics()
            
            if 'error' in analytics:
                self.show_message(f'Analytics error: {analytics["error"]}')
                return
            
            # Create analytics popup
            content = ScrollView()
            layout = GridLayout(cols=1, size_hint_y=None, spacing=5, padding=10)
            layout.bind(minimum_height=layout.setter('height'))
            
            # Add analytics sections
            sections = [
                ('Schedule Metrics', analytics.get('schedule_metrics', {})),
                ('Workload Distribution', analytics.get('workload_distribution', {})),
                ('Active Operations', analytics.get('active_operations', {})),
                ('Event System', analytics.get('event_system', {})),
                ('Performance', analytics.get('performance_metrics', {}))
            ]
            
            for section_name, section_data in sections:
                # Section header
                header = Label(
                    text=f'{section_name}:',
                    size_hint_y=None,
                    height=30,
                    bold=True,
                    halign='left'
                )
                header.bind(size=header.setter('text_size'))
                layout.add_widget(header)
                
                # Section content
                for key, value in section_data.items():
                    if isinstance(value, dict):
                        content_text = f'  {key}: {json.dumps(value, indent=2)}'
                    else:
                        content_text = f'  {key}: {value}'
                    
                    content_label = Label(
                        text=content_text,
                        size_hint_y=None,
                        height=25,
                        halign='left',
                        font_size='12sp'
                    )
                    content_label.bind(size=content_label.setter('text_size'))
                    layout.add_widget(content_label)
            
            content.add_widget(layout)
            
            popup = Popup(
                title='Real-Time Analytics',
                content=content,
                size_hint=(0.8, 0.8)
            )
            popup.open()
            
        except Exception as e:
            logging.error(f"Error showing analytics: {e}")
            self.show_message(f'Analytics error: {str(e)}')
    
    def show_message(self, message):
        """Show a simple message popup"""
        popup = Popup(
            title='Real-Time Status',
            content=Label(text=message),
            size_hint=(None, None),
            size=(400, 200)
        )
        popup.open()


class RealTimeWorkerAssignmentWidget(BoxLayout):
    """Widget for real-time worker assignment with live feedback"""
    
    def __init__(self, scheduler, **kwargs):
        super().__init__(**kwargs)
        self.scheduler = scheduler
        self.orientation = 'vertical'
        self.spacing = 10
        self.padding = 10
        
        # Title
        title = Label(
            text='Real-Time Assignment',
            size_hint_y=None,
            height=30,
            bold=True
        )
        self.add_widget(title)
        
        # Input fields
        input_layout = GridLayout(cols=2, size_hint_y=None, height=120, spacing=5)
        
        input_layout.add_widget(Label(text='Worker ID:', halign='left'))
        self.worker_input = TextInput(multiline=False, hint_text='e.g., W001')
        input_layout.add_widget(self.worker_input)
        
        input_layout.add_widget(Label(text='Date (YYYY-MM-DD):', halign='left'))
        self.date_input = TextInput(multiline=False, hint_text='2024-01-01')
        input_layout.add_widget(self.date_input)
        
        input_layout.add_widget(Label(text='Post Index:', halign='left'))
        self.post_input = TextInput(multiline=False, hint_text='0', input_filter='int')
        input_layout.add_widget(self.post_input)
        
        self.add_widget(input_layout)
        
        # Action buttons
        button_layout = BoxLayout(orientation='horizontal', size_hint_y=None, height=40, spacing=5)
        
        assign_btn = Button(text='Assign')
        assign_btn.bind(on_press=self.assign_worker)
        button_layout.add_widget(assign_btn)
        
        unassign_btn = Button(text='Unassign')
        unassign_btn.bind(on_press=self.unassign_worker)
        button_layout.add_widget(unassign_btn)
        
        validate_btn = Button(text='Validate')
        validate_btn.bind(on_press=self.validate_assignment)
        button_layout.add_widget(validate_btn)
        
        self.add_widget(button_layout)
        
        # Feedback area
        self.feedback_label = Label(
            text='Ready for real-time operations...',
            size_hint_y=None,
            height=60,
            halign='left',
            text_size=(None, None)
        )
        self.add_widget(self.feedback_label)
    
    def assign_worker(self, instance):
        """Assign worker using real-time features"""
        try:
            worker_id = self.worker_input.text.strip()
            date_str = self.date_input.text.strip()
            post_str = self.post_input.text.strip()
            
            if not all([worker_id, date_str, post_str]):
                self.show_feedback('Please fill all fields')
                return
            
            # Parse inputs
            shift_date = datetime.strptime(date_str, '%Y-%m-%d')
            post_index = int(post_str)
            
            if not self.scheduler.is_real_time_enabled():
                self.show_feedback('Real-time features not enabled')
                return
            
            # Perform real-time assignment
            result = self.scheduler.assign_worker_real_time(
                worker_id, shift_date, post_index, 'ui_user'
            )
            
            if result['success']:
                self.show_feedback(f'✓ {result["message"]}')
                if result.get('suggestions'):
                    suggestions = '\n'.join(result['suggestions'][:2])
                    self.show_feedback(f'✓ {result["message"]}\nSuggestions: {suggestions}')
            else:
                feedback = f'✗ {result["message"]}'
                if result.get('suggestions'):
                    suggestions = '\n'.join(result['suggestions'][:2])
                    feedback += f'\nSuggestions: {suggestions}'
                self.show_feedback(feedback)
            
        except ValueError as e:
            self.show_feedback(f'Input error: {str(e)}')
        except Exception as e:
            logging.error(f"Error in real-time assignment: {e}")
            self.show_feedback(f'Assignment error: {str(e)}')
    
    def unassign_worker(self, instance):
        """Unassign worker using real-time features"""
        try:
            date_str = self.date_input.text.strip()
            post_str = self.post_input.text.strip()
            
            if not all([date_str, post_str]):
                self.show_feedback('Please fill date and post fields')
                return
            
            # Parse inputs
            shift_date = datetime.strptime(date_str, '%Y-%m-%d')
            post_index = int(post_str)
            
            if not self.scheduler.is_real_time_enabled():
                self.show_feedback('Real-time features not enabled')
                return
            
            # Perform real-time unassignment
            result = self.scheduler.unassign_worker_real_time(
                shift_date, post_index, 'ui_user'
            )
            
            if result['success']:
                feedback = f'✓ {result["message"]}'
                if result.get('suggestions'):
                    suggestions = '\n'.join(result['suggestions'][:2])
                    feedback += f'\nSuggestions: {suggestions}'
                self.show_feedback(feedback)
            else:
                self.show_feedback(f'✗ {result["message"]}')
            
        except ValueError as e:
            self.show_feedback(f'Input error: {str(e)}')
        except Exception as e:
            logging.error(f"Error in real-time unassignment: {e}")
            self.show_feedback(f'Unassignment error: {str(e)}')
    
    def validate_assignment(self, instance):
        """Validate potential assignment"""
        try:
            worker_id = self.worker_input.text.strip()
            date_str = self.date_input.text.strip()
            post_str = self.post_input.text.strip()
            
            if not all([worker_id, date_str, post_str]):
                self.show_feedback('Please fill all fields for validation')
                return
            
            # Parse inputs
            shift_date = datetime.strptime(date_str, '%Y-%m-%d')
            post_index = int(post_str)
            
            if not self.scheduler.is_real_time_enabled():
                self.show_feedback('Real-time features not enabled')
                return
            
            # Get live validator
            validator = self.scheduler.real_time_engine.live_validator
            result = validator.validate_assignment(worker_id, shift_date, post_index)
            
            if result.is_valid:
                self.show_feedback(f'✓ Assignment valid: {result.message}')
            else:
                self.show_feedback(f'✗ Assignment invalid: {result.message}')
            
        except ValueError as e:
            self.show_feedback(f'Input error: {str(e)}')
        except Exception as e:
            logging.error(f"Error in assignment validation: {e}")
            self.show_feedback(f'Validation error: {str(e)}')
    
    def show_feedback(self, message):
        """Show feedback message"""
        self.feedback_label.text = message
        self.feedback_label.text_size = (self.width - 20, None)


def add_real_time_features_to_calendar_screen(calendar_screen, scheduler):
    """Add real-time features to the existing calendar screen"""
    try:
        # Check if real-time features are available
        if not scheduler.is_real_time_enabled():
            logging.info("Real-time features not enabled - skipping UI integration")
            return
        
        # Add real-time status widget to the top of the layout
        status_widget = RealTimeStatusWidget(scheduler)
        calendar_screen.layout.add_widget(status_widget, index=0)
        
        # Add real-time assignment widget as a popup option
        def show_real_time_assignment(instance):
            assignment_widget = RealTimeWorkerAssignmentWidget(scheduler)
            popup = Popup(
                title='Real-Time Assignment',
                content=assignment_widget,
                size_hint=(0.8, 0.7)
            )
            popup.open()
        
        # Add a button to access real-time assignment
        if hasattr(calendar_screen, 'layout'):
            # Find the button layout and add real-time button
            for child in calendar_screen.layout.children:
                if isinstance(child, BoxLayout) and hasattr(child, 'children'):
                    # Look for button layouts
                    buttons = [c for c in child.children if isinstance(c, Button)]
                    if len(buttons) >= 3:  # Likely a button layout
                        rt_btn = Button(text='Real-Time')
                        rt_btn.bind(on_press=show_real_time_assignment)
                        child.add_widget(rt_btn)
                        break
        
        logging.info("Real-time UI features added to calendar screen")
        
    except Exception as e:
        logging.error(f"Error adding real-time features to UI: {e}")


# Add to main.py imports and modify CalendarViewScreen initialization
def initialize_real_time_ui():
    """Initialize real-time UI components"""
    print("Real-time UI components loaded")
    return {
        'RealTimeStatusWidget': RealTimeStatusWidget,
        'RealTimeWorkerAssignmentWidget': RealTimeWorkerAssignmentWidget,
        'add_real_time_features_to_calendar_screen': add_real_time_features_to_calendar_screen
    }