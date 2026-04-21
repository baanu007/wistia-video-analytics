"""Pytest config: stub awsglue modules so jobs can be imported locally.

Glue jobs import `awsglue.utils.getResolvedOptions`, which only exists on
the Glue runtime. Locally we stub these modules so unit tests can import
the job files and exercise pure-Python helpers.
"""

import sys
import types
from unittest.mock import MagicMock


# ----- Stub awsglue.utils.getResolvedOptions -----
awsglue_module = types.ModuleType("awsglue")
awsglue_utils = types.ModuleType("awsglue.utils")
awsglue_context = types.ModuleType("awsglue.context")
awsglue_job = types.ModuleType("awsglue.job")

def _stub_get_resolved_options(argv, keys):
    """Tiny parser: looks for '--KEY value' pairs in argv for each requested key."""
    out = {}
    for k in keys:
        flag = f"--{k}"
        if flag in argv:
            idx = argv.index(flag)
            if idx + 1 < len(argv):
                out[k] = argv[idx + 1]
                continue
        out[k] = f"stub_{k}"
    return out

awsglue_utils.getResolvedOptions = _stub_get_resolved_options  # type: ignore[attr-defined]
awsglue_context.GlueContext = MagicMock  # type: ignore[attr-defined]
awsglue_job.Job = MagicMock  # type: ignore[attr-defined]

sys.modules.setdefault("awsglue", awsglue_module)
sys.modules.setdefault("awsglue.utils", awsglue_utils)
sys.modules.setdefault("awsglue.context", awsglue_context)
sys.modules.setdefault("awsglue.job", awsglue_job)


# ----- Stub pyspark (only imported by transform job) -----
try:
    import pyspark  # noqa: F401
except ImportError:
    pyspark_mod = types.ModuleType("pyspark")
    pyspark_context = types.ModuleType("pyspark.context")
    pyspark_sql = types.ModuleType("pyspark.sql")
    pyspark_sql_functions = types.ModuleType("pyspark.sql.functions")
    pyspark_sql_window = types.ModuleType("pyspark.sql.window")

    pyspark_context.SparkContext = MagicMock  # type: ignore[attr-defined]
    pyspark_sql.DataFrame = MagicMock  # type: ignore[attr-defined]
    pyspark_sql_window.Window = MagicMock  # type: ignore[attr-defined]

    # pyspark.sql.functions is accessed as F in the transform job -- stub common fns
    for fn_name in (
        "col", "lit", "when", "lower", "regexp_extract",
        "to_date", "input_file_name", "row_number",
    ):
        setattr(pyspark_sql_functions, fn_name, MagicMock())

    sys.modules["pyspark"] = pyspark_mod
    sys.modules["pyspark.context"] = pyspark_context
    sys.modules["pyspark.sql"] = pyspark_sql
    sys.modules["pyspark.sql.functions"] = pyspark_sql_functions
    sys.modules["pyspark.sql.window"] = pyspark_sql_window


# ----- Stub awswrangler (used by dq_and_load) -----
try:
    import awswrangler  # noqa: F401
except ImportError:
    wr_mod = types.ModuleType("awswrangler")
    wr_s3 = types.ModuleType("awswrangler.s3")
    wr_s3.read_parquet = MagicMock()  # type: ignore[attr-defined]
    wr_mod.s3 = wr_s3  # type: ignore[attr-defined]
    sys.modules["awswrangler"] = wr_mod
    sys.modules["awswrangler.s3"] = wr_s3
