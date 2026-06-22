from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class AuthContext(BaseModel):
    user_id: str
    name: Optional[str] = None
    user_email: Optional[str] = None
    identities: Optional[list] = None
    group_info: Optional[dict] = None
    dependent_token: Optional[Any] = None


class StatusUpdateRequest(BaseModel):
    source_id: str
    version: str
    status: str


class StreamCreateRequest(BaseModel):
    title: str
    lab_id: Optional[str] = None
    organization: Optional[str] = None
    metadata: Optional[dict] = None
    stream_id: Optional[str] = None


class StreamAppendFiles(BaseModel):
    filename: Optional[str] = None
    size: Optional[int] = Field(default=0, ge=0, le=10 * 1024 * 1024 * 1024)


class StreamAppendRequest(BaseModel):
    stream_id: Optional[str] = None
    files: Optional[List[StreamAppendFiles]] = Field(default=None, max_length=1000)
    file_count: Optional[int] = Field(default=None, ge=0, le=10000)
    total_bytes: Optional[int] = Field(default=None, ge=0, le=50 * 1024 * 1024 * 1024)
    last_file: Optional[dict] = None


class FileUploadItem(BaseModel):
    filename: str
    content_base64: str
    content_type: Optional[str] = "application/octet-stream"
    metadata: Optional[dict] = None


class FileUploadRequest(BaseModel):
    filename: Optional[str] = None
    content_base64: Optional[str] = None
    content_type: Optional[str] = "application/octet-stream"
    metadata: Optional[dict] = None
    files: Optional[List[FileUploadItem]] = Field(default=None, max_length=100)


class UploadUrlRequest(BaseModel):
    filename: str
    content_type: Optional[str] = "application/octet-stream"
    size_bytes: Optional[int] = None
    expires_in: Optional[int] = 3600


class ConfirmUploadRequest(BaseModel):
    path: str
    size_bytes: Optional[int] = 0
    checksum_md5: Optional[str] = ""
    metadata: Optional[dict] = None


class DownloadUrlRequest(BaseModel):
    path: Optional[str] = None


class StreamCloseRequest(BaseModel):
    stream_id: Optional[str] = None
    mint_doi: Optional[bool] = False
    title: Optional[str] = None
    description: Optional[str] = None
    authors: Optional[list] = None
    keywords: Optional[list] = None
    license: Optional[str] = None


class StreamSnapshotRequest(BaseModel):
    stream_id: Optional[str] = None
    title: Optional[str] = None
    description: Optional[str] = None
    author: Optional[str] = None
    source_id: Optional[str] = None
    update: Optional[bool] = False
    data_sources: Optional[list] = None
    test: Optional[bool] = False


class MetadataEditRequest(BaseModel):
    title: Optional[str] = None
    authors: Optional[List[dict]] = None
    description: Optional[str] = None
    keywords: Optional[List[str]] = None
    license: Optional[dict] = None
    funding: Optional[List[dict]] = None
    related_works: Optional[List[dict]] = None
    methods: Optional[List[str]] = None
    facility: Optional[str] = None
    fields_of_science: Optional[List[str]] = None
    domains: Optional[List[str]] = None
    ml: Optional[dict] = None
    geo_locations: Optional[List[dict]] = None
    tags: Optional[List[str]] = None
    extensions: Optional[Dict[str, Any]] = None
    version: Optional[str] = None


class WithdrawRequest(BaseModel):
    reason: Optional[str] = ""
    version: Optional[str] = None


class ResubmitRequest(BaseModel):
    notes: Optional[str] = ""
    version: Optional[str] = None


class CurationApproveRequest(BaseModel):
    notes: Optional[str] = ""
    mint_doi: Optional[bool] = True
    metadata_updates: Optional[dict] = None
    version: Optional[str] = None


class CurationRejectRequest(BaseModel):
    reason: str
    suggestions: Optional[str] = ""
    version: Optional[str] = None


class DeleteSubmissionRequest(BaseModel):
    reason: str
    version: Optional[str] = None
