from datetime import datetime
from typing import Optional, Literal, Any
from pydantic import BaseModel, Field

PRODUCT_TEXT_INDEX_NAME = "products_text_search"
PRODUCT_TEXT_INDEX_FIELDS = [
    ("title", "text"),
    ("detailedInfo.fullDescription", "text"),
    ("shopName", "text"),
]


class SellerInfo(BaseModel):
    positiveFeedbackRate: Optional[int] = None
    hasVIP: Optional[bool] = False
    averageDeliveryTime: Optional[str] = None
    averageRefundTime: Optional[str] = None


class ShopInfo(BaseModel):
    shopName: Optional[str] = None
    shopLink: Optional[str] = None
    shopRating: Optional[float] = None
    shopLocation: Optional[str] = None
    shopAge: Optional[str] = None
    sellerInfo: Optional[SellerInfo] = None
    badges: list[str] = []
    extractedFrom: Optional[Any] = None


class DataQuality(BaseModel):
    hasTitle: bool = False
    hasPrice: bool = False
    hasImages: bool = False
    hasVariants: bool = False
    hasSpecs: bool = False
    hasBrand: bool = False
    hasReviews: bool = False
    hasDescription: bool = False
    hasSalesVolume: bool = False
    hasShopName: bool = False
    completeness: int = 0


class DetailedInfo(BaseModel):
    fullTitle: Optional[str] = None
    fullDescription: Optional[str] = None
    specifications: Optional[dict] = None
    brand: Optional[str] = None
    additionalImages: list[str] = []
    reviewCount: Optional[str] = None
    rating: Optional[str] = None
    inStock: Optional[bool] = None
    shippingInfo: Optional[str] = None
    guarantees: list[str] = []
    variants: Optional[Any] = None
    salesVolume: Optional[str] = None
    price: Optional[float] = None
    priceUsd: Optional[float] = None
    originalPrice: Optional[str] = None
    originalPriceUsd: Optional[float] = None
    shopInfo: Optional[ShopInfo] = None
    dataQuality: Optional[DataQuality] = None
    extractionStrategies: Optional[dict] = None


class MigrationHistory(BaseModel):
    version: int
    migratedAt: datetime
    changes: Optional[dict] = None


class Product(BaseModel):
    itemId: str
    title: str
    price: Optional[str] = None
    image: Optional[str] = None
    link: str
    searchKeyword: Optional[str] = None
    categoryId: Optional[str] = None
    categoryName: Optional[str] = None
    groupCategoryId: Optional[str] = None
    groupCategoryName: Optional[str] = None
    groupCategoryNameEn: Optional[str] = None
    pageNumber: Optional[int] = None
    shopName: Optional[str] = None
    shopInfo: Optional[ShopInfo] = None
    salesCount: Optional[str] = None
    location: Optional[str] = None
    platform: str
    detailedInfo: Optional[DetailedInfo] = None
    detailsScraped: bool = False
    detailsScrapedAt: Optional[datetime] = None
    extractionQuality: Optional[int] = None
    contentHash: Optional[str] = None
    lastSeenAt: Optional[datetime] = None
    lastDiscoveryAt: Optional[datetime] = None
    lastSuccessfulDetailAt: Optional[datetime] = None
    enrichmentStatus: Optional[str] = None
    enrichmentSource: Optional[str] = None
    enrichmentAttempts: int = 0
    lastEnrichmentError: Optional[str] = None
    extractedAt: Optional[datetime] = None
    createdAt: datetime = Field(default_factory=datetime.utcnow)
    updatedAt: datetime = Field(default_factory=datetime.utcnow)
    migrationVersion: int = 2
    migrationHistory: list[MigrationHistory] = []


class SearchParams(BaseModel):
    keyword: Optional[str] = None
    categoryId: Optional[str] = None
    categoryName: Optional[str] = None
    groupCategoryId: Optional[str] = None
    groupCategoryName: Optional[str] = None
    groupCategoryNameEn: Optional[str] = None
    maxProducts: Optional[int] = 100
    maxPages: Optional[int] = 10


class JobProgress(BaseModel):
    currentPage: int = 0
    totalPages: int = 0
    productsScraped: int = 0
    detailsScraped: int = 0
    detailsFailed: int = 0


class JobResults(BaseModel):
    totalProducts: int = 0
    successfulDetails: int = 0
    failedDetails: int = 0
    updatedProducts: int = 0
    detailsScraped: int = 0


class ScrapingJob(BaseModel):
    jobId: str
    platform: Literal["taobao", "tmall", "1688", "alibaba", "all"]
    searchType: Literal["keyword", "category", "pending_details"]
    searchParams: Optional[SearchParams] = None
    status: Literal["pending", "running", "completed", "failed", "cancelled"] = "pending"
    progress: JobProgress = Field(default_factory=JobProgress)
    results: JobResults = Field(default_factory=JobResults)
    error: Optional[str] = None
    startedAt: Optional[datetime] = None
    completedAt: Optional[datetime] = None
    createdAt: datetime = Field(default_factory=datetime.utcnow)
    updatedAt: datetime = Field(default_factory=datetime.utcnow)


def _is_text_index(index: dict) -> bool:
    key = index.get("key", {})
    if hasattr(key, "items"):
        return any(value == "text" for _, value in key.items()) or key.get("_fts") == "text"
    return False


def _text_index_matches(index: dict) -> bool:
    if index.get("name") != PRODUCT_TEXT_INDEX_NAME:
        return False
    weights = index.get("weights") or {}
    expected_fields = {field for field, kind in PRODUCT_TEXT_INDEX_FIELDS if kind == "text"}
    return set(weights) == expected_fields


async def _ensure_product_text_index(col) -> None:
    indexes = await col.list_indexes().to_list(length=None)
    for index in indexes:
        if not _is_text_index(index):
            continue
        if _text_index_matches(index):
            return
        await col.drop_index(index["name"])

    await col.create_index(PRODUCT_TEXT_INDEX_FIELDS, name=PRODUCT_TEXT_INDEX_NAME)


async def ensure_product_indexes(db):
    col = db.products
    await col.create_index("itemId", unique=True)
    await col.create_index([("platform", 1), ("categoryId", 1)])
    await col.create_index([("searchKeyword", 1), ("platform", 1)])
    await col.create_index("detailsScraped")
    await col.create_index([("createdAt", -1)])
    await col.create_index("shopName")
    await col.create_index("shopInfo.shopName")
    await col.create_index([("extractionQuality", -1)])
    await col.create_index("categoryName", sparse=True)
    await col.create_index("contentHash", sparse=True)
    await col.create_index([("enrichmentStatus", 1), ("updatedAt", -1)])
    await col.create_index([("lastSeenAt", -1)])
    await _ensure_product_text_index(col)


async def ensure_enrichment_queue_indexes(db):
    col = db.enrichment_queue
    await col.create_index("itemId", unique=True)
    await col.create_index([("status", 1), ("nextAttemptAt", 1)])
    await col.create_index([("platform", 1), ("status", 1), ("priority", -1)])
