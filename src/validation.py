import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

os.environ.setdefault("GX_ANALYTICS_ENABLED", "false")   # no usage statistics

import pandas as pd
import great_expectations as gx
from great_expectations.data_context.types.base import ProgressBarsConfig

from src import config
from src import quality_rules as qr
from src.extract import read_grammy_table


class DataQualityError(Exception):
    """A critical rule failed: the affected downstream path must stop."""


def get_context():
    """File context stored in the `gx/` folder (GX_CONTEXT_DIR); GX creates the folder structure the first time."""
    gx_dir = Path(os.getenv("GX_CONTEXT_DIR", "gx")).resolve()
    context = gx.get_context(mode="file", project_root_dir=str(gx_dir.parent))
    context.variables.progress_bars = ProgressBarsConfig(globally=False, metric_calculations=False)  # clean Airflow logs
    return context


def run_checkpoint(context, data_source, name, df, expectations):
    """Builds asset, batch definition, suite, validation definition and checkpoint for one data object, and runs it."""
    try:
        asset = data_source.get_asset(name)
    except LookupError:
        asset = data_source.add_dataframe_asset(name=name)
    try:
        batch_definition = asset.get_batch_definition("whole_dataframe")
    except KeyError:
        batch_definition = asset.add_batch_definition_whole_dataframe("whole_dataframe")

    suite = context.suites.add_or_update(gx.ExpectationSuite(name=f"{name}_suite", expectations=expectations))
    validation = context.validation_definitions.add_or_update(
        gx.ValidationDefinition(name=f"{name}_validation", data=batch_definition, suite=suite))
    checkpoint = context.checkpoints.add_or_update(
        gx.Checkpoint(name=f"{name}_checkpoint", validation_definitions=[validation],
                      result_format={"result_format": "SUMMARY"}))
    return checkpoint.run(batch_parameters={"dataframe": df})


def read_results(checkpoint_result, table_name):
    """One dictionary per expectation with the evidence the check produced."""
    rows = []
    for validation_result in checkpoint_result.run_results.values():
        for result in validation_result.results:
            config_ = result.expectation_config
            detail = result.result or {}
            observed = {key: detail[key] for key in ("element_count", "unexpected_count", "unexpected_percent",
                                                     "observed_value") if key in detail}
            if not result.success and detail.get("partial_unexpected_list"):
                observed["examples"] = detail["partial_unexpected_list"][:5]
            kwargs = config_.kwargs
            rows.append({
                "rule_id": config_.meta["rule_id"],
                "severity": config_.severity.value,
                "table": table_name,
                "expectation": config_.type,
                "column": kwargs.get("column") or kwargs.get("column_list") or kwargs.get("column_A"),
                "success": bool(result.success),
                "observed": observed,
            })
    return rows


def run_gate(gate, data_objects, source=""):
    """data_objects: list of (name, DataFrame, expectations). Returns the summary; raises on a critical failure."""
    context = get_context()
    data_source = context.data_sources.add_or_update_pandas(name="workshop2_pandas")

    expectation_rows = []
    for name, df, expectations in data_objects:
        result = run_checkpoint(context, data_source, name, df, expectations)
        expectation_rows += read_results(result, name)

    # group the expectations by rule: a rule passes only if all its expectations pass
    rules = {}
    for row in expectation_rows:
        rule = rules.setdefault(row["rule_id"], {"rule_id": row["rule_id"], "severity": row["severity"],
                                                 "description": qr.RULES[row["rule_id"]], "success": True, "expectations": []})
        rule["success"] = rule["success"] and row["success"]
        rule["expectations"].append({k: row[k] for k in ("table", "expectation", "column", "success", "observed")})
    rules = sorted(rules.values(), key=lambda r: r["rule_id"])

    failed = {level: [r["rule_id"] for r in rules if r["severity"] == level and not r["success"]]
              for level in (qr.CRITICAL, qr.WARNING, qr.INFO)}
    summary = {
        "gate": gate,
        "run_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "source": source,
        "rows": {name: len(df) for name, df, _ in data_objects},
        "passed": not failed[qr.CRITICAL],
        "rules_checked": len(rules),
        "failed_critical": failed[qr.CRITICAL],
        "failed_warning": failed[qr.WARNING],
        "failed_info": failed[qr.INFO],
        "rules": rules,
    }

    out_dir = config.evidence_dir() / "validation"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{gate}_{datetime.now(timezone.utc):%Y%m%dT%H%M%S%fZ}.json"
    out_file.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")

    print(f"[{gate}] rules checked: {len(rules)} | critical failed: {failed[qr.CRITICAL] or 'none'} | "
          f"warning failed: {failed[qr.WARNING] or 'none'} | info failed: {failed[qr.INFO] or 'none'}")
    print(f"[{gate}] result saved in {out_file}")
    if failed[qr.CRITICAL]:
        raise DataQualityError(f"{gate}: critical rule(s) failed: {', '.join(failed[qr.CRITICAL])}")
    # compact result for the next task (the full detail stays in the JSON file)
    keys = ("gate", "passed", "rules_checked", "failed_critical", "failed_warning", "failed_info")
    return {**{key: summary[key] for key in keys}, "evidence_file": str(out_file)}


def only_rule(expectations, rule_id):
    return [e for e in expectations if e.meta["rule_id"] == rule_id]


def validate_spotify_raw(df, source=""):
    expectations = qr.spotify_raw_expectations()
    if set(qr.SPOTIFY_COLUMNS).issubset(df.columns):
        df = qr.add_spotify_helpers(df)
    else:
        expectations = only_rule(expectations, "DQ-01")   # without the columns the other rules cannot run
    return run_gate("validate_spotify_raw", [("spotify_raw", df, expectations)], source)


def validate_grammys_raw(df, source=""):
    expectations = qr.grammy_raw_expectations()
    if set(qr.GRAMMY_COLUMNS).issubset(df.columns):
        df = qr.add_grammy_helpers(df)
    else:
        expectations = only_rule(expectations, "DQ-11")
    return run_gate("validate_grammys_raw", [("grammy_raw", df, expectations)], source)


def validate_prepared(tables, source=""):
    return run_gate("validate_prepared", qr.prepared_data_objects(tables), source)


# ---------------------------------------------------------------------------------------------
# manual run
# ---------------------------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Run a raw validation gate by hand")
    parser.add_argument("dataset", choices=["spotify", "grammy"])
    parser.add_argument("--csv", help="CSV file to validate (Grammy: only if MySQL is not running)")
    args = parser.parse_args()

    try:
        if args.dataset == "spotify":
            path = args.csv or str(config.spotify_csv_path())
            validate_spotify_raw(pd.read_csv(path), source=path)
        elif args.csv:
            validate_grammys_raw(pd.read_csv(args.csv, dtype=str, keep_default_na=False, na_values=[""]), source=args.csv)
        else:
            validate_grammys_raw(read_grammy_table(), source="MySQL grammy_source.grammy_awards")
    except DataQualityError as error:
        print(error)
        sys.exit(1)


if __name__ == "__main__":
    main()
