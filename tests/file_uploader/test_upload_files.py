import os
import subprocess
import tempfile
import shutil
import pytest


@pytest.fixture
def setup_test_environment():
    # Create a temporary directory for test files
    temp_dir = tempfile.mkdtemp()

    # Create test files for FB
    fb_folder = os.path.join(temp_dir, "FB")
    os.makedirs(fb_folder)
    fb_file = os.path.join(fb_folder, "12345678.pdf")
    with open(fb_file, "w") as f:
        f.write("Test content for FB file.")

    # Create test files and folders for WB
    wb_folder = os.path.join(temp_dir, "WB")
    os.makedirs(wb_folder)
    wb_supp_file_folder = os.path.join(wb_folder, "12345678")
    os.makedirs(wb_supp_file_folder)
    wb_supp_file = os.path.join(wb_supp_file_folder, "S1.pdf")
    with open(wb_supp_file, "w") as f:
        f.write("Test content for WB supplemental file.")
    wb_file = os.path.join(wb_folder, "12345678_author_year_temp.pdf")
    with open(wb_file, "w") as f:
        f.write("Test content for WB file.")

    yield temp_dir

    # Cleanup after test
    shutil.rmtree(temp_dir)


def test_file_uploader_fb(setup_test_environment):
    temp_dir = setup_test_environment
    temp_dir = os.path.join(temp_dir, "FB")
    script_path = os.path.abspath("file_uploader/upload_files.sh")

    # Run the script for FB
    result = subprocess.run(
        [script_path, "FB", "--test-extraction", temp_dir],
        cwd=temp_dir,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    # Assert the output contains expected values
    assert "Processing main file" in result.stdout
    assert "reference ID: PMID:12345678" in result.stdout
    assert "TEST MODE: Skipping file upload." in result.stdout


def test_file_uploader_wb(setup_test_environment):
    temp_dir = setup_test_environment
    temp_dir = os.path.join(temp_dir, "WB")
    script_path = os.path.abspath("file_uploader/upload_files.sh")

    # Run the script for WB
    result = subprocess.run(
        [script_path, "WB", "--test-extraction", temp_dir],
        cwd=temp_dir,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    # Assert the output contains expected values for the main file
    assert "Processing main file" in result.stdout
    assert "reference ID: WB:WBPaper12345678" in result.stdout
    assert "display_name: 12345678" in result.stdout
    assert "file_class: main" in result.stdout
    assert "TEST MODE: Skipping file upload." in result.stdout

    # Assert the output contains expected values for the supplemental file
    assert "Processing supplemental files from" in result.stdout
    assert "Processing supplement file" in result.stdout
    assert "display_name: S1" in result.stdout
    assert "file_class: supplement" in result.stdout
    assert "TEST MODE: Skipping file upload." in result.stdout
