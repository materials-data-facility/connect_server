"""Dataset profiling for MDF v2.

Scans files associated with a submission and produces a structured
profile with column statistics, sample data, and format information.
"""

import csv
import io
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pydantic import BaseModel

from v2.preview import generate_preview
from v2.storage import StorageBackend

logger = logging.getLogger(__name__)

MAX_FILE_BYTES = 10 * 1024 * 1024  # 10 MB cap per file for profiling
MAX_SAMPLE_ROWS = 5


class ColumnSummary(BaseModel):
    name: str
    dtype: str  # "float64", "int64", "string", "bool", "datetime"
    count: int  # non-null count
    nulls: int = 0
    unique: Optional[int] = None
    min: Optional[float] = None
    max: Optional[float] = None
    mean: Optional[float] = None
    std: Optional[float] = None
    top_values: List[str] = []


class FileProfile(BaseModel):
    path: str
    filename: str
    size_bytes: int
    content_type: str
    format: str  # "csv", "json", "hdf5", "cif", "image", "text", "binary"
    columns: List[ColumnSummary] = []
    n_rows: Optional[int] = None
    sample_rows: List[Dict] = []
    structure: Optional[Dict] = None
    preview_lines: List[str] = []
    extra: Dict[str, Any] = {}


class DatasetProfile(BaseModel):
    source_id: str
    profiled_at: str
    total_files: int
    total_bytes: int
    formats: Dict[str, int] = {}
    files: List[FileProfile] = []


def _detect_format(filename: str, content: Optional[bytes] = None) -> str:
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    format_map = {
        "csv": "csv", "tsv": "csv",
        "json": "json", "jsonl": "json",
        "hdf5": "hdf5", "h5": "hdf5", "hdf": "hdf5",
        "cif": "cif",
        "png": "image", "jpg": "image", "jpeg": "image",
        "tif": "image", "tiff": "image", "bmp": "image",
        "txt": "text", "md": "text", "log": "text",
        "py": "text", "yaml": "text", "yml": "text",
        "xml": "text", "html": "text",
    }
    if ext in format_map:
        return format_map[ext]

    if content:
        if content[:8] == b"\x89HDF\r\n\x1a\n":
            return "hdf5"
        if content[:4] == b"\x89PNG":
            return "image"
        if content[:2] == b"\xff\xd8":
            return "image"
        try:
            content[:1000].decode("utf-8")
            return "text"
        except UnicodeDecodeError:
            pass

    return "binary"


def _profile_csv(content: bytes, filename: str) -> dict:
    """Profile a CSV/TSV file, returning columns, sample_rows, and n_rows."""
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError:
        text = content.decode("latin-1")

    dialect = csv.Sniffer().sniff(text[:4096]) if text[:4096].strip() else None
    reader = csv.reader(io.StringIO(text), dialect or csv.excel)
    rows = list(reader)
    if not rows:
        return {"columns": [], "sample_rows": [], "n_rows": 0}

    headers = rows[0]
    data_rows = rows[1:]
    n_rows = len(data_rows)

    columns = []
    for i, header in enumerate(headers):
        col_values = [row[i] for row in data_rows if i < len(row)]
        non_null = [v for v in col_values if v.strip()]
        nulls = len(col_values) - len(non_null)

        # Try numeric detection
        numeric = []
        for v in non_null:
            try:
                numeric.append(float(v))
            except (ValueError, TypeError):
                pass

        if len(numeric) > len(non_null) * 0.5 and numeric:
            mean_val = sum(numeric) / len(numeric)
            variance = sum((x - mean_val) ** 2 for x in numeric) / max(len(numeric) - 1, 1)
            std_val = variance ** 0.5
            # Detect int vs float
            all_int = all(x == int(x) for x in numeric)
            dtype = "int64" if all_int else "float64"
            columns.append(ColumnSummary(
                name=header,
                dtype=dtype,
                count=len(non_null),
                nulls=nulls,
                min=min(numeric),
                max=max(numeric),
                mean=round(mean_val, 6),
                std=round(std_val, 6),
            ))
        else:
            unique_vals = set(non_null)
            top = sorted(unique_vals, key=lambda v: non_null.count(v), reverse=True)[:5]
            columns.append(ColumnSummary(
                name=header,
                dtype="string",
                count=len(non_null),
                nulls=nulls,
                unique=len(unique_vals),
                top_values=top,
            ))

    # Build sample rows as list of dicts
    sample_rows = []
    for row in data_rows[:MAX_SAMPLE_ROWS]:
        row_dict = {}
        for i, header in enumerate(headers):
            row_dict[header] = row[i] if i < len(row) else ""
        sample_rows.append(row_dict)

    return {"columns": columns, "sample_rows": sample_rows, "n_rows": n_rows}


def _profile_json(content: bytes) -> dict:
    """Profile a JSON file, returning structure summary."""
    try:
        data = json.loads(content.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return {"structure": None}

    def summarize(obj, depth=0, max_depth=3):
        if depth >= max_depth:
            return {"_type": type(obj).__name__}
        if isinstance(obj, dict):
            return {k: summarize(v, depth + 1) for k, v in list(obj.items())[:20]}
        elif isinstance(obj, list):
            if not obj:
                return []
            return [summarize(obj[0], depth + 1), f"... ({len(obj)} items)"]
        else:
            return type(obj).__name__

    result = {"structure": summarize(data)}

    # If it's an array of dicts (tabular-like JSON), extract sample rows
    if isinstance(data, list) and data and isinstance(data[0], dict):
        result["n_rows"] = len(data)
        result["sample_rows"] = data[:MAX_SAMPLE_ROWS]
        # Extract "columns" from the first record's keys
        first = data[0]
        columns = []
        for key, val in first.items():
            if isinstance(val, (int, float)):
                dtype = "float64"
            elif isinstance(val, bool):
                dtype = "bool"
            elif isinstance(val, str):
                dtype = "string"
            else:
                dtype = type(val).__name__
            columns.append(ColumnSummary(
                name=key, dtype=dtype, count=len(data),
            ))
        result["columns"] = columns

    return result


def _profile_cif(content: bytes) -> dict:
    """Extract basic CIF info."""
    extra = {}
    try:
        text = content.decode("utf-8")
        for line in text.split("\n")[:200]:
            if line.startswith("_chemical_formula_sum"):
                extra["formula"] = line.split(None, 1)[1].strip().strip("'\"")
            elif line.startswith("_symmetry_space_group_name"):
                extra["space_group"] = line.split(None, 1)[1].strip().strip("'\"")
    except Exception:
        pass
    return {"extra": extra}


def _profile_image(content: bytes, filename: str) -> dict:
    """Extract basic image info."""
    extra = {}
    if content[:4] == b"\x89PNG" and len(content) > 24:
        extra["width"] = int.from_bytes(content[16:20], "big")
        extra["height"] = int.from_bytes(content[20:24], "big")
        extra["image_format"] = "PNG"
    elif content[:2] == b"\xff\xd8":
        extra["image_format"] = "JPEG"
    else:
        ext = filename.rsplit(".", 1)[-1].upper() if "." in filename else "unknown"
        extra["image_format"] = ext
    return {"extra": extra}


def _profile_text(content: bytes, max_lines: int = 30) -> dict:
    """Extract preview lines from text files."""
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError:
        try:
            text = content.decode("latin-1")
        except UnicodeDecodeError:
            return {"preview_lines": []}

    lines = text.split("\n")[:max_lines]
    return {"preview_lines": lines}


def _profile_file(file_meta, content: Optional[bytes]) -> FileProfile:
    """Build a FileProfile for one file."""
    filename = file_meta.filename
    fmt = _detect_format(filename, content)
    size_bytes = file_meta.size_bytes or (len(content) if content else 0)

    profile = FileProfile(
        path=file_meta.path,
        filename=filename,
        size_bytes=size_bytes,
        content_type=file_meta.content_type or "",
        format=fmt,
    )

    if content is None or size_bytes > MAX_FILE_BYTES:
        return profile

    if fmt == "csv":
        result = _profile_csv(content, filename)
        profile.columns = result.get("columns", [])
        profile.sample_rows = result.get("sample_rows", [])
        profile.n_rows = result.get("n_rows")
    elif fmt == "json":
        result = _profile_json(content)
        profile.structure = result.get("structure")
        profile.sample_rows = result.get("sample_rows", [])
        profile.n_rows = result.get("n_rows")
        profile.columns = result.get("columns", [])
    elif fmt == "cif":
        result = _profile_cif(content)
        profile.extra = result.get("extra", {})
    elif fmt == "image":
        result = _profile_image(content, filename)
        profile.extra = result.get("extra", {})
    elif fmt == "text":
        result = _profile_text(content)
        profile.preview_lines = result.get("preview_lines", [])

    return profile


def build_dataset_profile(source_id: str, storage: StorageBackend) -> DatasetProfile:
    """Scan files for a dataset and build a structured profile.

    Args:
        source_id: The source_id / stream_id to scan files for.
        storage: The storage backend to read files from.

    Returns:
        A DatasetProfile with file-level details and aggregate stats.
    """
    files = storage.list_files(source_id)

    total_bytes = 0
    format_counts: Dict[str, int] = {}
    file_profiles: List[FileProfile] = []

    for file_meta in files:
        size = file_meta.size_bytes or 0
        total_bytes += size

        # Only read file content if under the size cap
        content = None
        if size <= MAX_FILE_BYTES:
            try:
                content = storage.get_file(file_meta.path)
            except Exception:
                logger.debug("Could not read %s for profiling", file_meta.path)

        fp = _profile_file(file_meta, content)
        file_profiles.append(fp)
        format_counts[fp.format] = format_counts.get(fp.format, 0) + 1

    return DatasetProfile(
        source_id=source_id,
        profiled_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        total_files=len(files),
        total_bytes=total_bytes,
        formats=format_counts,
        files=file_profiles,
    )
