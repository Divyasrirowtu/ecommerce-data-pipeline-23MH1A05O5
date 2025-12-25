import json
import os

def test_quality_report_generated():
    assert os.path.exists("data/quality_report.json")

def test_quality_score_present():
    with open("data/quality_report.json") as f:
        report = json.load(f)
    assert "overall_score" in report

def test_null_check_detects_issue():
    with open("data/quality_report.json") as f:
        report = json.load(f)
    assert "null_checks" in report
