import socket

from agr_literature_service.lit_processing.utils.container_log_utils import (
    MAX_ERROR_CHARS,
    emit_error_record,
    format_error_record,
)


def test_record_format():
    line = format_error_record("ABC-DQM-FAILED", "IncompleteRead(156247879 bytes read)",
                               job="sort_dqm_json_reference_updates", mod="MGI",
                               datatype="REFERENCE", step="download")
    assert line == (f"ABC-DQM-FAILED job=sort_dqm_json_reference_updates mod=MGI datatype=REFERENCE "
                    f"step=download host={socket.gethostname()} "
                    f'error="IncompleteRead(156247879 bytes read)"')


def test_error_stays_one_quoted_field():
    line = format_error_record("ABC-DQM-FAILED", 'bad "json"\nat line 2\\', mod="ZFIN")
    assert "\n" not in line
    assert line.endswith('''error="bad 'json' at line 2"''')


def test_error_is_cut_to_the_limit():
    line = format_error_record("ABC-DQM-FAILED", "e" * 500)
    assert line.endswith('"' + "e" * MAX_ERROR_CHARS + '"')


def test_field_values_have_no_spaces():
    assert "mod=a_b " in format_error_record("ABC-DQM-FAILED", mod="a b")


def test_record_goes_to_the_container_stderr(tmp_path, monkeypatch, capsys):
    stream = tmp_path / "2"
    stream.write_text("")
    monkeypatch.setenv("CRON_LOG_STREAM_DIR", str(tmp_path))
    line = emit_error_record("ABC-DQM-FAILED", "boom", mod="MGI")
    assert stream.read_text() == line + "\n"
    assert capsys.readouterr().err == ""


def test_falls_back_to_own_stderr(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("CRON_LOG_STREAM_DIR", str(tmp_path / "missing"))
    line = emit_error_record("ABC-DQM-FAILED", "boom", mod="MGI")
    assert capsys.readouterr().err == line + "\n"
