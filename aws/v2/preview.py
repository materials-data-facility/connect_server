"""Dataset and file preview for MDF v2.

Provides preview capabilities so researchers can inspect data
before downloading entire datasets.

Supported previews:
- CSV/TSV: First N rows, column statistics
- JSON: Structure/schema, first N keys
- Text: First N lines
- Images: Dimensions, format info
- Binary: File info only
"""

import base64
import csv
import io
import json
import os
from typing import Any, Dict, List, Optional

from v2.request import parse_authorizer
from v2.responses import bad_request, ok
from v2.storage import get_storage_backend


def preview_csv(content: bytes, max_rows: int = 20) -> Dict[str, Any]:
    """Preview a CSV file."""
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError:
        text = content.decode("latin-1")

    reader = csv.reader(io.StringIO(text))
    rows = list(reader)

    if not rows:
        return {"type": "csv", "empty": True}

    headers = rows[0] if rows else []
    data_rows = rows[1:max_rows + 1]
    total_rows = len(rows) - 1  # Exclude header

    # Calculate column statistics
    columns = []
    for i, header in enumerate(headers):
        col_values = [row[i] for row in rows[1:] if i < len(row)]

        # Try to detect numeric columns
        numeric_values = []
        for v in col_values:
            try:
                numeric_values.append(float(v))
            except (ValueError, TypeError):
                pass

        col_info = {
            "name": header,
            "index": i,
            "non_null_count": len([v for v in col_values if v]),
            "sample_values": col_values[:5],
        }

        if len(numeric_values) > len(col_values) * 0.5:  # Mostly numeric
            col_info["type"] = "numeric"
            if numeric_values:
                col_info["min"] = min(numeric_values)
                col_info["max"] = max(numeric_values)
                col_info["mean"] = sum(numeric_values) / len(numeric_values)
        else:
            col_info["type"] = "string"
            unique = set(col_values)
            col_info["unique_count"] = len(unique)
            if len(unique) <= 10:
                col_info["unique_values"] = list(unique)[:10]

        columns.append(col_info)

    return {
        "type": "csv",
        "headers": headers,
        "columns": columns,
        "total_rows": total_rows,
        "preview_rows": len(data_rows),
        "rows": data_rows,
        "truncated": total_rows > max_rows,
    }


def preview_json(content: bytes, max_keys: int = 50) -> Dict[str, Any]:
    """Preview a JSON file."""
    try:
        data = json.loads(content.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        return {"type": "json", "error": str(e)}

    def summarize(obj, depth=0, max_depth=3):
        """Recursively summarize JSON structure."""
        if depth >= max_depth:
            return {"_truncated": True, "_type": type(obj).__name__}

        if isinstance(obj, dict):
            result = {}
            for i, (k, v) in enumerate(obj.items()):
                if i >= max_keys:
                    result["_more_keys"] = len(obj) - max_keys
                    break
                result[k] = summarize(v, depth + 1, max_depth)
            return result
        elif isinstance(obj, list):
            if not obj:
                return []
            # Show first few items
            sample = [summarize(item, depth + 1, max_depth) for item in obj[:3]]
            if len(obj) > 3:
                sample.append({"_more_items": len(obj) - 3})
            return sample
        else:
            return obj

    return {
        "type": "json",
        "structure": summarize(data),
        "size_bytes": len(content),
        "is_array": isinstance(data, list),
        "is_object": isinstance(data, dict),
        "top_level_keys": list(data.keys())[:20] if isinstance(data, dict) else None,
        "array_length": len(data) if isinstance(data, list) else None,
    }


def preview_text(content: bytes, max_lines: int = 50) -> Dict[str, Any]:
    """Preview a text file."""
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError:
        try:
            text = content.decode("latin-1")
        except UnicodeDecodeError:
            return {"type": "text", "error": "Unable to decode as text"}

    lines = text.split("\n")
    total_lines = len(lines)

    return {
        "type": "text",
        "total_lines": total_lines,
        "preview_lines": min(max_lines, total_lines),
        "lines": lines[:max_lines],
        "truncated": total_lines > max_lines,
        "size_bytes": len(content),
    }


def preview_binary(content: bytes, filename: str) -> Dict[str, Any]:
    """Preview info for binary files."""
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""

    info = {
        "type": "binary",
        "size_bytes": len(content),
        "extension": ext,
    }

    # Check for known magic bytes
    if content[:4] == b"\x89PNG":
        info["format"] = "PNG image"
        # Parse PNG dimensions
        if len(content) > 24:
            width = int.from_bytes(content[16:20], "big")
            height = int.from_bytes(content[20:24], "big")
            info["dimensions"] = {"width": width, "height": height}

    elif content[:2] == b"\xff\xd8":
        info["format"] = "JPEG image"

    elif content[:4] == b"PK\x03\x04":
        info["format"] = "ZIP archive"

    elif content[:8] == b"\x89HDF\r\n\x1a\n":
        info["format"] = "HDF5 file"

    elif content[:4] == b"CDF\x01" or content[:4] == b"CDF\x02":
        info["format"] = "NetCDF file"

    elif ext in ("npy", "npz"):
        info["format"] = "NumPy array"

    elif ext == "cif":
        info["format"] = "Crystallographic Information File"
        # Try to extract basic CIF info
        try:
            text = content.decode("utf-8")
            for line in text.split("\n")[:100]:
                if line.startswith("_chemical_formula_sum"):
                    info["formula"] = line.split(None, 1)[1].strip().strip("'\"")
                elif line.startswith("_symmetry_space_group_name"):
                    info["space_group"] = line.split(None, 1)[1].strip().strip("'\"")
        except Exception:
            pass

    return info


def generate_preview(
    content: bytes,
    filename: str,
    content_type: str = "",
    max_rows: int = 20,
    max_lines: int = 50,
) -> Dict[str, Any]:
    """Generate a preview for any file type."""
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""

    # Determine preview type
    if ext in ("csv", "tsv") or "csv" in content_type:
        return preview_csv(content, max_rows)

    elif ext == "json" or "json" in content_type:
        return preview_json(content)

    elif ext in ("txt", "md", "log", "py", "yaml", "yml", "xml", "html") or "text" in content_type:
        return preview_text(content, max_lines)

    else:
        # Try to detect if it's text
        try:
            sample = content[:1000].decode("utf-8")
            # Check if it looks like text (mostly printable)
            printable = sum(1 for c in sample if c.isprintable() or c in "\n\r\t")
            if printable > len(sample) * 0.9:
                return preview_text(content, max_lines)
        except UnicodeDecodeError:
            pass

        return preview_binary(content, filename)


def lambda_handler(event, context):
    """Preview a file from a stream.

    GET /stream/{stream_id}/files/{filename}/preview
    or
    POST /stream/{stream_id}/preview
    {"path": "..."}
    """
    from v2.request import parse_json_body

    path_params = event.get("pathParameters") or {}
    stream_id = path_params.get("stream_id")
    filename = path_params.get("filename")

    if not stream_id:
        return bad_request("stream_id is required")

    # Get path from body if not in URL
    if not filename:
        payload, _ = parse_json_body(event)
        if payload:
            path = payload.get("path")
            if path:
                filename = path.split("/")[-1]

    if not filename:
        return bad_request("filename or path is required")

    # Get the file from storage
    storage = get_storage_backend()

    # Try to find the file
    files = storage.list_files(stream_id)
    file_meta = None
    for f in files:
        if f.filename == filename or f.path.endswith(filename):
            file_meta = f
            break

    if not file_meta:
        # Try direct path
        path = f"{stream_id}_{filename}" if storage.backend_name == "globus" else f"streams/{stream_id}/{filename}"
        content = storage.get_file(path)
        if content is None:
            return bad_request(f"File not found: {filename}")
    else:
        content = storage.get_file(file_meta.path)
        if content is None:
            return bad_request(f"Could not read file: {filename}")

    # Generate preview
    query = event.get("queryStringParameters") or {}
    max_rows = int(query.get("max_rows", 20))
    max_lines = int(query.get("max_lines", 50))

    preview = generate_preview(
        content=content,
        filename=filename,
        max_rows=max_rows,
        max_lines=max_lines,
    )

    return ok({
        "success": True,
        "stream_id": stream_id,
        "filename": filename,
        "preview": preview,
    })


def preview_stream_handler(event, context):
    """Get previews for all files in a stream.

    GET /stream/{stream_id}/preview
    """
    path_params = event.get("pathParameters") or {}
    stream_id = path_params.get("stream_id")

    if not stream_id:
        return bad_request("stream_id is required")

    storage = get_storage_backend()
    files = storage.list_files(stream_id)

    previews = []
    for f in files:
        content = storage.get_file(f.path)
        if content:
            preview = generate_preview(content, f.filename)
            previews.append({
                "filename": f.filename,
                "path": f.path,
                "size_bytes": f.size_bytes,
                "preview": preview,
            })

    return ok({
        "success": True,
        "stream_id": stream_id,
        "file_count": len(previews),
        "previews": previews,
    })
