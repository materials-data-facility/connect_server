"""Email notifications for MDF v2 via AWS SES.

Sends transactional emails for key curation lifecycle events:
  - New submission awaiting curation  → curators
  - Submission approved / published   → submitter
  - Submission rejected               → submitter

Configuration (environment variables):
  SES_FROM_EMAIL        Sender address (must be SES-verified)
  CURATOR_EMAILS        Comma-separated curator addresses for new-submission alerts
  PORTAL_URL            Public dataset portal base URL  (e.g. https://app.materialsdatafacility.org)
  CURATION_PORTAL_URL   Curation review page base URL   (e.g. https://app.materialsdatafacility.org/curate)
"""

import logging
import os
from typing import Any, Dict, List, Optional

from v2.metadata import parse_metadata

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def _from_address() -> str:
    return os.environ.get("SES_FROM_EMAIL", "noreply@materialsdatafacility.org")


def _curator_emails() -> List[str]:
    raw = os.environ.get("CURATOR_EMAILS", "")
    return [e.strip() for e in raw.split(",") if e.strip()]


def _portal_url() -> str:
    return os.environ.get("PORTAL_URL", "https://www.materialsdatafacility.org").rstrip("/")


def _curation_url() -> str:
    return os.environ.get("CURATION_PORTAL_URL", _portal_url() + "/curation").rstrip("/")


def _emails_enabled() -> bool:
    return bool(os.environ.get("SES_FROM_EMAIL"))


# ---------------------------------------------------------------------------
# SES send
# ---------------------------------------------------------------------------

def _send(to: List[str], subject: str, html: str, text: str) -> bool:
    """Send via SES. Silently logs and returns False on any failure."""
    if not to:
        return True
    if not _emails_enabled():
        logger.debug("Email skipped (SES_FROM_EMAIL not set): %s → %s", subject, to)
        return True
    try:
        import boto3
        region = os.environ.get("SES_REGION", "us-east-1")
        client = boto3.client("ses", region_name=region)
        client.send_email(
            Source=_from_address(),
            Destination={"ToAddresses": to},
            Message={
                "Subject": {"Data": subject, "Charset": "UTF-8"},
                "Body": {
                    "Html": {"Data": html, "Charset": "UTF-8"},
                    "Text": {"Data": text, "Charset": "UTF-8"},
                },
            },
        )
        logger.info("Email sent subject=%r to=%s", subject, to)
        return True
    except Exception:
        logger.warning("Failed to send email to %s", to, exc_info=True)
        return False


# ---------------------------------------------------------------------------
# Public notification functions
# ---------------------------------------------------------------------------

def notify_curators_new_submission(record: Dict[str, Any]) -> bool:
    """Email curators when a new submission enters pending_curation."""
    recipients = _curator_emails()
    if not recipients:
        return True

    meta = parse_metadata(record)
    source_id = record.get("source_id", "")
    version = record.get("version", "")
    review_url = _curation_url()

    subject = f"New Dataset Pending Review: {meta.title}"
    html = _build_email(
        header_color="#1e3a5f",
        header_label="Curation Request",
        header_title="New Dataset Awaiting Review",
        body_html=_dataset_card(meta, record) + _cta_button(review_url, "Review Submission", "#2563eb"),
        footer_note=f"Submission ID: {source_id} · v{version}",
    )
    text = _plain_text(
        f"New Dataset Awaiting Review\n\n"
        f"Title:   {meta.title}\n"
        f"Authors: {', '.join(a.name for a in meta.authors)}\n"
        f"Org:     {record.get('organization', '')}\n"
        f"ID:      {source_id} v{version}\n\n"
        f"Review: {review_url}"
    )
    return _send(recipients, subject, html, text)


def notify_submitter_approved(record: Dict[str, Any]) -> bool:
    """Email the submitter when their dataset is approved and published."""
    submitter_email = record.get("user_email")
    if not submitter_email:
        return True

    meta = parse_metadata(record)
    source_id = record.get("source_id", "")
    version = record.get("version", "")
    doi = record.get("dataset_doi") or record.get("doi")
    dataset_url = f"{_portal_url()}/detail/{source_id}"

    doi_line = f"\nDOI: https://doi.org/{doi}" if doi else ""
    subject = f"Your MDF Dataset is Now Published: {meta.title}"
    html = _build_email(
        header_color="#15803d",
        header_label="Publication Confirmed",
        header_title="Your Dataset is Now Live!",
        body_html=(
            _message_block(
                "Congratulations — your submission has been reviewed and is now publicly available "
                "in the Materials Data Facility."
            )
            + _dataset_card(meta, record, show_doi=True)
            + _cta_button(dataset_url, "View Your Dataset →", "#15803d")
        ),
        footer_note=f"Dataset ID: {source_id} · v{version}{doi_line}",
    )
    text = _plain_text(
        f"Your Dataset is Now Live!\n\n"
        f"Title:   {meta.title}\n"
        f"Authors: {', '.join(a.name for a in meta.authors)}\n"
        f"Version: {version}\n"
        + (f"DOI:     https://doi.org/{doi}\n" if doi else "")
        + f"\nView: {dataset_url}"
    )
    return _send([submitter_email], subject, html, text)


def notify_submitter_rejected(record: Dict[str, Any], reason: str, suggestions: str = "") -> bool:
    """Email the submitter when their dataset is rejected."""
    submitter_email = record.get("user_email")
    if not submitter_email:
        return True

    meta = parse_metadata(record)
    source_id = record.get("source_id", "")
    version = record.get("version", "")
    dataset_url = f"{_portal_url()}/detail/{source_id}"

    reason_block = (
        f'<div style="background:#fef2f2;border-left:4px solid #ef4444;'
        f'border-radius:0 6px 6px 0;padding:16px 20px;margin:20px 0;">'
        f'<p style="margin:0 0 6px;font-size:12px;font-weight:700;'
        f'color:#991b1b;text-transform:uppercase;letter-spacing:.05em;">Curator Feedback</p>'
        f'<p style="margin:0;font-size:14px;color:#1e293b;line-height:1.6;">{_escape(reason)}</p>'
        + (
            f'<p style="margin:12px 0 0;font-size:13px;color:#64748b;font-style:italic;">'
            f'{_escape(suggestions)}</p>'
            if suggestions else ""
        )
        + "</div>"
    )

    subject = f"MDF Submission Needs Attention: {meta.title}"
    html = _build_email(
        header_color="#b45309",
        header_label="Submission Update",
        header_title="Your Submission Needs Revision",
        body_html=(
            _message_block(
                "Thank you for your submission. After review, our curators have requested "
                "changes before this dataset can be published."
            )
            + _dataset_card(meta, record)
            + reason_block
            + _cta_button(dataset_url, "View Feedback & Resubmit →", "#b45309")
        ),
        footer_note=f"Dataset ID: {source_id} · v{version}",
    )
    text = _plain_text(
        f"Your Submission Needs Revision\n\n"
        f"Title:   {meta.title}\n"
        f"Version: {version}\n\n"
        f"Curator feedback:\n{reason}\n"
        + (f"\nSuggestions:\n{suggestions}\n" if suggestions else "")
        + f"\nView & resubmit: {dataset_url}"
    )
    return _send([submitter_email], subject, html, text)


# ---------------------------------------------------------------------------
# HTML building blocks
# ---------------------------------------------------------------------------

def _escape(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _build_email(
    header_color: str,
    header_label: str,
    header_title: str,
    body_html: str,
    footer_note: str = "",
) -> str:
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1.0">
  <meta http-equiv="X-UA-Compatible" content="IE=edge">
</head>
<body style="margin:0;padding:0;background:#f0f4f8;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'Helvetica Neue',Arial,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background:#f0f4f8;padding:40px 16px;">
    <tr><td align="center">
      <table width="600" cellpadding="0" cellspacing="0" border="0" style="max-width:600px;width:100%;">

        <!-- Header -->
        <tr><td style="background:{header_color};border-radius:10px 10px 0 0;padding:36px 40px 32px;">
          <p style="margin:0 0 10px;font-size:11px;font-weight:700;color:rgba(255,255,255,0.65);letter-spacing:.12em;text-transform:uppercase;">Materials Data Facility</p>
          <p style="margin:0 0 6px;font-size:12px;color:rgba(255,255,255,0.55);letter-spacing:.08em;text-transform:uppercase;">{_escape(header_label)}</p>
          <h1 style="margin:0;font-size:26px;font-weight:700;color:#ffffff;line-height:1.2;">{_escape(header_title)}</h1>
        </td></tr>

        <!-- Body -->
        <tr><td style="background:#ffffff;padding:36px 40px;">
          {body_html}
        </td></tr>

        <!-- Footer -->
        <tr><td style="background:#f8fafc;border-radius:0 0 10px 10px;padding:20px 40px;border-top:1px solid #e2e8f0;">
          <p style="margin:0 0 4px;font-size:11px;color:#94a3b8;text-align:center;">
            <a href="{_portal_url()}" style="color:#94a3b8;text-decoration:none;">Materials Data Facility</a>
            &nbsp;·&nbsp; University of Chicago &nbsp;·&nbsp; Argonne National Laboratory
          </p>
          {f'<p style="margin:4px 0 0;font-size:11px;color:#cbd5e1;text-align:center;">{_escape(footer_note)}</p>' if footer_note else ''}
        </td></tr>

      </table>
    </td></tr>
  </table>
</body>
</html>"""


def _message_block(text: str) -> str:
    return (
        f'<p style="margin:0 0 24px;font-size:15px;color:#475569;line-height:1.65;">'
        f"{_escape(text)}</p>"
    )


def _dataset_card(meta: Any, record: Dict[str, Any], show_doi: bool = False) -> str:
    authors_str = ", ".join(a.name for a in meta.authors[:5])
    if len(meta.authors) > 5:
        authors_str += f" +{len(meta.authors) - 5} more"

    description = (meta.description or "").strip()
    description_html = ""
    if description:
        snippet = description[:400] + ("…" if len(description) > 400 else "")
        description_html = (
            f'<p style="margin:12px 0 0;font-size:13px;color:#64748b;line-height:1.6;">'
            f"{_escape(snippet)}</p>"
        )

    # Stats row
    stats: list[str] = []
    org = record.get("organization") or ""
    if org:
        stats.append(f"<strong>Org:</strong> {_escape(org)}")
    file_count = record.get("file_count")
    if file_count:
        stats.append(f"<strong>Files:</strong> {file_count:,}")
    total_bytes = record.get("total_bytes")
    if total_bytes:
        stats.append(f"<strong>Size:</strong> {_human_bytes(total_bytes)}")
    doi = record.get("dataset_doi") or record.get("doi")
    if doi and show_doi:
        doi_link = f'<a href="https://doi.org/{_escape(doi)}" style="color:#2563eb;">{_escape(doi)}</a>'
        stats.append(f"<strong>DOI:</strong> {doi_link}")
    stats_html = ""
    if stats:
        stats_html = (
            '<p style="margin:14px 0 0;font-size:12px;color:#94a3b8;line-height:1.8;">'
            + " &nbsp;·&nbsp; ".join(stats)
            + "</p>"
        )

    keywords = meta.keywords[:6]
    keywords_html = ""
    if keywords:
        tags = "".join(
            f'<span style="display:inline-block;background:#eff6ff;color:#1d4ed8;'
            f'font-size:11px;font-weight:600;padding:3px 10px;border-radius:20px;'
            f'margin:4px 4px 0 0;">{_escape(k)}</span>'
            for k in keywords
        )
        keywords_html = f'<div style="margin-top:14px;">{tags}</div>'

    return (
        f'<div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;'
        f'padding:24px 24px 20px;margin:0 0 28px;">'
        f'<h2 style="margin:0 0 6px;font-size:18px;font-weight:700;color:#0f172a;line-height:1.3;">'
        f"{_escape(meta.title)}</h2>"
        f'<p style="margin:0;font-size:13px;color:#475569;">{_escape(authors_str)}</p>'
        f"{description_html}"
        f"{stats_html}"
        f"{keywords_html}"
        f"</div>"
    )


def _cta_button(url: str, label: str, color: str) -> str:
    return (
        f'<div style="text-align:center;margin:32px 0 8px;">'
        f'<a href="{url}" style="display:inline-block;background:{color};color:#ffffff;'
        f'text-decoration:none;padding:14px 36px;border-radius:7px;font-size:15px;'
        f'font-weight:700;letter-spacing:.01em;line-height:1;">{_escape(label)}</a>'
        f'</div>'
        f'<p style="text-align:center;margin:10px 0 0;font-size:11px;color:#94a3b8;">'
        f'Or copy this link: <a href="{url}" style="color:#94a3b8;">{url}</a></p>'
    )


def _human_bytes(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024
    return f"{n:.1f} PB"


def _plain_text(body: str) -> str:
    return (
        "Materials Data Facility\n"
        "─────────────────────────────────────\n"
        + body
        + "\n\n─────────────────────────────────────\n"
        "materialsdatafacility.org\n"
    )
