from datetime import datetime
from typing import Optional, Literal
from pydantic import BaseModel, Field


class SearchParams(BaseModel):
    keyword:      Optional[str] = None
    categoryId:   Optional[str] = None
    categoryName: Optional[str] = None
    maxProducts:  Optional[int] = 100
    maxPages:     Optional[int] = 10


class JobProgress(BaseModel):
    currentPage:     int = 0   # mirrors default: 0
    totalPages:      int = 0
    productsScraped: int = 0
    detailsScraped:  int = 0
    detailsFailed:   int = 0


class JobResults(BaseModel):
    totalProducts:     int = 0  # mirrors default: 0
    successfulDetails: int = 0
    failedDetails:     int = 0
    updatedProducts:   int = 0
    detailsScraped:    int = 0


class ScrapingJob(BaseModel):
    """
    Mirrors ScrapingJob Mongoose schema exactly.

    Mongoose enums → Pydantic Literal types:
        platform:   'taobao' | 'tmall' | '1688'
        searchType: 'keyword' | 'category'
        status:     'pending' | 'running' | 'completed' | 'failed' | 'cancelled'
    """
    jobId:        str
    platform:     Literal["taobao", "tmall", "1688", "all"]
    searchType:   Literal["keyword", "category", "pending_details"]
    searchParams: Optional[SearchParams] = None
    status:       Literal["pending", "running", "completed", "failed", "cancelled"] = "pending"
    progress:     JobProgress = Field(default_factory=JobProgress)
    results:      JobResults  = Field(default_factory=JobResults)
    error:        Optional[str] = None
    startedAt:    Optional[datetime] = None
    completedAt:  Optional[datetime] = None
    createdAt:    datetime = Field(default_factory=datetime.utcnow)
    updatedAt:    datetime = Field(default_factory=datetime.utcnow)

    def to_dict(self) -> dict:
        """Serialize to plain dict for JSON file storage."""
        return self.model_dump(mode="json")

    @classmethod
    def from_dict(cls, data: dict) -> "ScrapingJob":
        """Deserialize from JSON file / MongoDB document."""
        return cls(**data)