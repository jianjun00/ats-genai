#!/usr/bin/env python3
"""
Tests for Run-Aware Logging System
"""

import pytest
import tempfile
import shutil
from pathlib import Path
import logging
import logging.config
from datetime import datetime, timezone
import sys
import os
import contextvars

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from core.run_context import RunContext
from core.run_aware_logging import (
    RunAwareFormatter, RunAwareFileHandler, configure_run_aware_logging,
    setup_run_aware_logging, get_run_aware_logger, set_current_run_id,
    get_current_run_id, enable_run_aware_logging, current_run_id
)


class TestRunAwareFormatter:
    """Test run-aware logging formatter."""
    
    def setup_method(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        
        # Create test run context
        self.run_context = RunContext(
            run_id="test_run_20241218_123456_abcd1234",
            start_time=datetime.now(timezone.utc),
            base_dir=Path(self.temp_dir) / "test_run",
            artifacts_dir=Path(self.temp_dir) / "test_run" / "artifacts",
            universe_state_dir=Path(self.temp_dir) / "test_run" / "universe_state",
            logs_dir=Path(self.temp_dir) / "test_run" / "logs",
            metadata={"test": "logging"}
        )
        self.run_context.logs_dir.mkdir(parents=True)
    
    def teardown_method(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_dir)
        # Reset context
        current_run_id.set(None)
    
    def test_formatter_with_run_id_in_context(self):
        """Test formatter includes run_id from context."""
        formatter = RunAwareFormatter()
        
        # Set run_id in context
        set_current_run_id(self.run_context.run_id)
        
        # Create log record
        record = logging.LogRecord(
            name="test_logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=10,
            msg="Test message",
            args=(),
            exc_info=None
        )
        
        # Format the record
        formatted = formatter.format(record)
        
        assert self.run_context.run_id in formatted
        assert "Test message" in formatted
    
    def test_formatter_without_run_id(self):
        """Test formatter handles missing run_id gracefully."""
        formatter = RunAwareFormatter()
        
        # Create log record without setting run_id
        record = logging.LogRecord(
            name="test_logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=10,
            msg="Test message",
            args=(),
            exc_info=None
        )
        
        # Format the record
        formatted = formatter.format(record)
        
        assert "[no-run]" in formatted
        assert "Test message" in formatted
    
    def test_formatter_custom_format(self):
        """Test formatter with custom format string."""
        custom_format = "%(levelname)s [%(run_id)s] - %(message)s"
        formatter = RunAwareFormatter(fmt=custom_format)
        
        set_current_run_id("custom_run_123")
        
        record = logging.LogRecord(
            name="test_logger",
            level=logging.WARNING,
            pathname="test.py",
            lineno=10,
            msg="Warning message",
            args=(),
            exc_info=None
        )
        
        formatted = formatter.format(record)
        
        assert formatted.startswith("WARNING [custom_run_123]")
        assert "Warning message" in formatted


class TestRunAwareFileHandler:
    """Test run-aware file handler."""
    
    def setup_method(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        
        self.run_context = RunContext(
            run_id="test_run_file_handler",
            start_time=datetime.now(timezone.utc),
            base_dir=Path(self.temp_dir) / "test_run",
            artifacts_dir=Path(self.temp_dir) / "test_run" / "artifacts",
            universe_state_dir=Path(self.temp_dir) / "test_run" / "universe_state",
            logs_dir=Path(self.temp_dir) / "test_run" / "logs",
            metadata={"test": "file_handler"}
        )
        self.run_context.logs_dir.mkdir(parents=True)
    
    def teardown_method(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_dir)
    
    def test_file_handler_with_run_context(self):
        """Test file handler creates run-specific log file."""
        handler = RunAwareFileHandler(
            filename="test_app.log",
            run_context=self.run_context
        )
        
        # Check file is created in run's logs directory
        expected_path = self.run_context.logs_dir / "test_app.log"
        
        # Write a test log
        formatter = RunAwareFormatter()
        handler.setFormatter(formatter)
        
        set_current_run_id(self.run_context.run_id)
        
        record = logging.LogRecord(
            name="test_logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=10,
            msg="Test log message",
            args=(),
            exc_info=None
        )
        
        handler.handle(record)
        handler.close()
        
        # Verify file exists and has content
        assert expected_path.exists()
        content = expected_path.read_text()
        assert "Test log message" in content
        assert self.run_context.run_id in content
    
    def test_file_handler_without_run_context(self):
        """Test file handler without run context uses default location."""
        with tempfile.TemporaryDirectory() as temp_dir:
            os.chdir(temp_dir)  # Change to temp dir for test
            
            handler = RunAwareFileHandler(filename="default_test.log")
            
            formatter = RunAwareFormatter()
            handler.setFormatter(formatter)
            
            record = logging.LogRecord(
                name="test_logger",
                level=logging.INFO,
                pathname="test.py",
                lineno=10,
                msg="Default location test",
                args=(),
                exc_info=None
            )
            
            handler.handle(record)
            handler.close()
            
            # Should create file in current directory
            log_file = Path(temp_dir) / "default_test.log"
            assert log_file.exists()


class TestLoggingConfiguration:
    """Test logging configuration functions."""
    
    def setup_method(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        
        self.run_context = RunContext(
            run_id="test_run_config",
            start_time=datetime.now(timezone.utc),
            base_dir=Path(self.temp_dir) / "test_run",
            artifacts_dir=Path(self.temp_dir) / "test_run" / "artifacts",
            universe_state_dir=Path(self.temp_dir) / "test_run" / "universe_state",
            logs_dir=Path(self.temp_dir) / "test_run" / "logs",
            metadata={"test": "config"}
        )
        self.run_context.logs_dir.mkdir(parents=True)
    
    def teardown_method(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_dir)
        # Reset logging configuration
        logging.getLogger().handlers.clear()
        current_run_id.set(None)
    
    def test_configure_run_aware_logging(self):
        """Test configuring run-aware logging."""
        config = configure_run_aware_logging(
            run_context=self.run_context,
            log_level="DEBUG",
            console_logging=True,
            file_logging=True,
            detailed_format=False
        )
        
        assert config["version"] == 1
        assert "console" in config["handlers"]
        assert "file" in config["handlers"]
        assert "run_aware" in config["formatters"]
        
        # Check file handler points to run context logs directory
        file_handler = config["handlers"]["file"]
        assert str(self.run_context.logs_dir) in file_handler["filename"]
    
    def test_configure_logging_without_run_context(self):
        """Test configuring logging without run context."""
        config = configure_run_aware_logging(
            run_context=None,
            console_logging=True,
            file_logging=True
        )
        
        assert config["version"] == 1
        assert "console" in config["handlers"]
        assert "file" in config["handlers"]
        
        # Should use default log file
        file_handler = config["handlers"]["file"]
        assert file_handler["filename"] == "ats_genai.log"
    
    def test_setup_run_aware_logging(self):
        """Test setting up run-aware logging."""
        setup_run_aware_logging(
            run_context=self.run_context,
            log_level="INFO",
            console_logging=False,  # Disable console for test
            file_logging=True
        )
        
        # Test logging
        logger = logging.getLogger("test_setup")
        logger.info("Test setup message")
        
        # Check log file
        log_file = self.run_context.logs_dir / "ats_genai.log"
        assert log_file.exists()
        
        content = log_file.read_text()
        assert "Test setup message" in content
        assert self.run_context.run_id in content
    
    def test_get_run_aware_logger(self):
        """Test getting run-aware logger."""
        logger = get_run_aware_logger("test_module", self.run_context)
        
        assert logger.name == "test_module"
        assert get_current_run_id() == self.run_context.run_id
    
    def test_set_and_get_current_run_id(self):
        """Test setting and getting current run ID."""
        test_run_id = "test_current_run_123"
        
        # Initially should be None
        assert get_current_run_id() is None
        
        # Set run ID
        set_current_run_id(test_run_id)
        assert get_current_run_id() == test_run_id
    
    def test_enable_run_aware_logging(self):
        """Test convenience function for enabling run-aware logging."""
        enable_run_aware_logging(self.run_context)
        
        # Test that logging works
        logger = logging.getLogger("test_enable")
        logger.info("Enable test message")
        
        # Check log file exists
        log_file = self.run_context.logs_dir / "ats_genai.log"
        assert log_file.exists()


class TestLoggingIntegration:
    """Integration tests for run-aware logging."""
    
    def setup_method(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
    
    def teardown_method(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_dir)
        logging.getLogger().handlers.clear()
        current_run_id.set(None)
    
    def test_multiple_loggers_same_run(self):
        """Test multiple loggers in same run context."""
        run_context = RunContext(
            run_id="test_multi_loggers",
            start_time=datetime.now(timezone.utc),
            base_dir=Path(self.temp_dir) / "test_run",
            artifacts_dir=Path(self.temp_dir) / "test_run" / "artifacts",
            universe_state_dir=Path(self.temp_dir) / "test_run" / "universe_state",
            logs_dir=Path(self.temp_dir) / "test_run" / "logs",
            metadata={"test": "multi_loggers"}
        )
        run_context.logs_dir.mkdir(parents=True)
        
        # Setup logging
        setup_run_aware_logging(
            run_context=run_context,
            console_logging=False,
            file_logging=True
        )
        
        # Create multiple loggers
        logger1 = get_run_aware_logger("module1", run_context)
        logger2 = get_run_aware_logger("module2", run_context)
        
        # Log from both
        logger1.info("Message from module1")
        logger2.warning("Warning from module2")
        
        # Check all messages in same log file
        log_file = run_context.logs_dir / "ats_genai.log"
        content = log_file.read_text()
        
        assert "Message from module1" in content
        assert "Warning from module2" in content
        assert content.count(run_context.run_id) == 2  # Both messages have run_id
    
    def test_different_run_contexts_separate_logs(self):
        """Test that different run contexts create separate log files."""
        # Create two run contexts
        contexts = []
        for i in range(2):
            context = RunContext(
                run_id=f"test_separate_run_{i}",
                start_time=datetime.now(timezone.utc),
                base_dir=Path(self.temp_dir) / f"test_run_{i}",
                artifacts_dir=Path(self.temp_dir) / f"test_run_{i}" / "artifacts",
                universe_state_dir=Path(self.temp_dir) / f"test_run_{i}" / "universe_state",
                logs_dir=Path(self.temp_dir) / f"test_run_{i}" / "logs",
                metadata={"test": f"separate_{i}"}
            )
            context.logs_dir.mkdir(parents=True)
            contexts.append(context)
        
        # Setup logging for each context and log messages
        for i, context in enumerate(contexts):
            # Reset logging for each context
            logging.getLogger().handlers.clear()
            
            setup_run_aware_logging(
                run_context=context,
                console_logging=False,
                file_logging=True
            )
            
            logger = get_run_aware_logger(f"test_module_{i}", context)
            logger.info(f"Message from run {i}")
        
        # Verify separate log files
        for i, context in enumerate(contexts):
            log_file = context.logs_dir / "ats_genai.log"
            assert log_file.exists()
            
            content = log_file.read_text()
            assert f"Message from run {i}" in content
            assert context.run_id in content
    
    def test_detailed_format_logging(self):
        """Test detailed format logging includes function info."""
        run_context = RunContext(
            run_id="test_detailed_format",
            start_time=datetime.now(timezone.utc),
            base_dir=Path(self.temp_dir) / "test_run",
            artifacts_dir=Path(self.temp_dir) / "test_run" / "artifacts",
            universe_state_dir=Path(self.temp_dir) / "test_run" / "universe_state",
            logs_dir=Path(self.temp_dir) / "test_run" / "logs",
            metadata={"test": "detailed"}
        )
        run_context.logs_dir.mkdir(parents=True)
        
        # Setup with detailed format
        setup_run_aware_logging(
            run_context=run_context,
            console_logging=False,
            file_logging=True,
            detailed_format=True
        )
        
        logger = get_run_aware_logger("test_detailed", run_context)
        logger.info("Detailed format test")
        
        # Check log content includes function info
        log_file = run_context.logs_dir / "ats_genai.log"
        content = log_file.read_text()
        
        assert "test_detailed_format_logging()" in content
        assert run_context.run_id in content
        assert "Detailed format test" in content
    
    def test_log_level_filtering(self):
        """Test that log level filtering works correctly."""
        run_context = RunContext(
            run_id="test_log_levels",
            start_time=datetime.now(timezone.utc),
            base_dir=Path(self.temp_dir) / "test_run",
            artifacts_dir=Path(self.temp_dir) / "test_run" / "artifacts",
            universe_state_dir=Path(self.temp_dir) / "test_run" / "universe_state",
            logs_dir=Path(self.temp_dir) / "test_run" / "logs",
            metadata={"test": "levels"}
        )
        run_context.logs_dir.mkdir(parents=True)
        
        # Setup with WARNING level
        setup_run_aware_logging(
            run_context=run_context,
            log_level="WARNING",
            console_logging=False,
            file_logging=True
        )
        
        logger = get_run_aware_logger("test_levels", run_context)
        
        # Log at different levels
        logger.debug("Debug message - should not appear")
        logger.info("Info message - should not appear")
        logger.warning("Warning message - should appear")
        logger.error("Error message - should appear")
        
        # Check log content
        log_file = run_context.logs_dir / "ats_genai.log"
        content = log_file.read_text()
        
        assert "Debug message" not in content
        assert "Info message" not in content
        assert "Warning message" in content
        assert "Error message" in content


class TestContextVarBehavior:
    """Test context variable behavior in different scenarios."""
    
    def test_context_var_isolation(self):
        """Test that context variables are properly isolated."""
        # Set initial run ID
        set_current_run_id("initial_run")
        assert get_current_run_id() == "initial_run"
        
        # Create a new context (simulating async task)
        ctx = contextvars.copy_context()
        
        def task_function():
            # This should see the copied context
            assert get_current_run_id() == "initial_run"
            
            # Set new run ID in this context
            set_current_run_id("task_run")
            assert get_current_run_id() == "task_run"
        
        # Run in copied context
        ctx.run(task_function)
        
        # Original context should be unchanged
        assert get_current_run_id() == "initial_run"
    
    def test_context_var_inheritance(self):
        """Test context variable inheritance in formatter."""
        # Set run ID
        test_run_id = "inherited_run_123"
        set_current_run_id(test_run_id)
        
        # Create formatter and record
        formatter = RunAwareFormatter()
        record = logging.LogRecord(
            name="test_logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=10,
            msg="Inheritance test",
            args=(),
            exc_info=None
        )
        
        # Format should use inherited run ID
        formatted = formatter.format(record)
        assert test_run_id in formatted


if __name__ == "__main__":
    pytest.main([__file__, "-v"])