from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional
import uuid


@dataclass
class JobPosting:
    """Normalized job posting extracted by crawler pipelines."""

    title: str
    company: str
    url: str

    location: Optional[str] = None
    description: Optional[str] = None
    posted_date: Optional[datetime] = None
    source: Optional[str] = None

    job_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    raw: Dict[str, Any] = field(default_factory=dict)


__all__ = ["JobPosting"]

