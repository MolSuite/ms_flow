import os
import time
from pathlib import Path

from ms_flow.logger import LoggingManager


def test_logging_manager_writes_app_and_executor_logs(tmp_path):
    log_dir = tmp_path / "logs"
    manager = LoggingManager(global_log_dir=log_dir, max_bytes=1024 * 1024, backup_count=2)
    manager.start()

    app_logger = manager.get_app_logger("test")
    executor_logger = manager.get_executor_logger("test")
    app_logger.info("app-message-123")
    executor_logger.info("executor-message-456")
    manager.stop()

    app_log_text = (log_dir / "app.log").read_text()
    executor_log_text = (log_dir / "executor.log").read_text()

    assert "app-message-123" in app_log_text
    assert "executor-message-456" in executor_log_text
    assert "executor-message-456" not in app_log_text


def test_project_logging_writes_to_project_log_file(tmp_path):
    log_dir = tmp_path / "global-logs"
    project_dir = tmp_path / "project-A"
    manager = LoggingManager(global_log_dir=log_dir, max_bytes=1024 * 1024, backup_count=2)
    manager.start()
    manager.set_project_logging(project_dir)

    project_logger = manager.get_project_logger("dock")
    project_logger.info("project-message-001")
    manager.stop()

    project_log = project_dir / "logs" / "project.log"
    assert project_log.exists()
    assert "project-message-001" in project_log.read_text()


def test_log_rotation_uses_configured_size(tmp_path):
    log_dir = tmp_path / "logs"
    manager = LoggingManager(global_log_dir=log_dir, max_bytes=300, backup_count=3)
    manager.start()

    app_logger = manager.get_app_logger("rotator")
    for i in range(120):
        app_logger.info("rotation-line-%03d %s", i, "X" * 80)

    manager.stop()
    rotated_files = {path.name for path in log_dir.glob("app.log*")}
    assert "app.log" in rotated_files
    assert any(name.startswith("app.log.") for name in rotated_files)


def test_cleanup_old_logs_removes_old_files(tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    old_file = log_dir / "old.log"
    new_file = log_dir / "new.log"
    old_file.write_text("old")
    new_file.write_text("new")

    old_ts = time.time() - (40 * 24 * 60 * 60)
    os.utime(old_file, (old_ts, old_ts))

    manager = LoggingManager(global_log_dir=log_dir, retention_days=30)
    removed = manager.cleanup_old_logs()

    assert removed >= 1
    assert not old_file.exists()
    assert new_file.exists()


def test_channel_level_configuration_is_respected(tmp_path):
    log_dir = tmp_path / "logs"
    manager = LoggingManager(
        global_log_dir=log_dir,
        app_level="WARNING",
        executor_level="ERROR",
        project_level="CRITICAL",
        console_level="ERROR",
    )
    manager.start()

    app_logger = manager.get_app_logger("level-test")
    executor_logger = manager.get_executor_logger("level-test")
    app_logger.info("app-info-should-not-appear")
    app_logger.warning("app-warning-should-appear")
    executor_logger.warning("executor-warning-should-not-appear")
    executor_logger.error("executor-error-should-appear")
    manager.stop()

    app_log_text = (log_dir / "app.log").read_text()
    executor_log_text = (log_dir / "executor.log").read_text()

    assert "app-info-should-not-appear" not in app_log_text
    assert "app-warning-should-appear" in app_log_text
    assert "executor-warning-should-not-appear" not in executor_log_text
    assert "executor-error-should-appear" in executor_log_text


def test_logging_manager_supports_custom_root_namespace(tmp_path):
    log_dir = tmp_path / "logs"
    manager = LoggingManager(global_log_dir=log_dir, root_namespace="amdockvs")
    manager.start()

    app_logger = manager.get_app_logger("ui")
    app_logger.info("host-aware-message")
    manager.stop()

    app_log_text = (log_dir / "app.log").read_text()
    assert "amdockvs.app.ui" in app_log_text
    assert "host-aware-message" in app_log_text


def test_logging_manager_appends_correlation_fields_when_present(tmp_path):
    log_dir = tmp_path / "logs"
    manager = LoggingManager(global_log_dir=log_dir)
    manager.start()

    executor_logger = manager.get_executor_logger("corr")
    executor_logger.warning(
        "correlated-message",
        extra={"project_id": "proj-1", "job_id": "job-2", "chunk_id": "chunk-3"},
    )
    manager.stop()

    executor_log_text = (log_dir / "executor.log").read_text()
    assert "correlated-message" in executor_log_text
    assert "project_id=proj-1" in executor_log_text
    assert "job_id=job-2" in executor_log_text
    assert "chunk_id=chunk-3" in executor_log_text
