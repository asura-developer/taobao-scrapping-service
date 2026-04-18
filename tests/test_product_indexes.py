import pytest

from models.product import PRODUCT_TEXT_INDEX_FIELDS, PRODUCT_TEXT_INDEX_NAME, _ensure_product_text_index


class FakeIndexCursor:
    def __init__(self, indexes):
        self.indexes = indexes

    async def to_list(self, length=None):
        return self.indexes


class FakeCollection:
    def __init__(self, indexes):
        self.indexes = indexes
        self.dropped = []
        self.created = []

    def list_indexes(self):
        return FakeIndexCursor(self.indexes)

    async def drop_index(self, name):
        self.dropped.append(name)

    async def create_index(self, fields, **kwargs):
        self.created.append((fields, kwargs))


@pytest.mark.asyncio
async def test_ensure_product_text_index_drops_generated_conflicting_text_index():
    col = FakeCollection([
        {"name": "_id_", "key": {"_id": 1}},
        {
            "name": "title_text_detailedInfo.fullDescription_text_shopName_text",
            "key": {"_fts": "text", "_ftsx": 1},
            "weights": {"title": 10, "detailedInfo.fullDescription": 5, "shopName": 2},
        },
    ])

    await _ensure_product_text_index(col)

    assert col.dropped == ["title_text_detailedInfo.fullDescription_text_shopName_text"]
    assert col.created == [(PRODUCT_TEXT_INDEX_FIELDS, {"name": PRODUCT_TEXT_INDEX_NAME})]


@pytest.mark.asyncio
async def test_ensure_product_text_index_keeps_matching_stable_text_index():
    col = FakeCollection([
        {
            "name": PRODUCT_TEXT_INDEX_NAME,
            "key": {"_fts": "text", "_ftsx": 1},
            "weights": {"title": 1, "detailedInfo.fullDescription": 1, "shopName": 1},
        },
    ])

    await _ensure_product_text_index(col)

    assert col.dropped == []
    assert col.created == []
