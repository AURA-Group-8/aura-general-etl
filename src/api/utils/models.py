from typing import List, Optional
from pydantic import BaseModel


class MappingItem(BaseModel):
    source_column: str
    target_field: Optional[str]


class MappingResponse(BaseModel):
    mappings: List[MappingItem]
    missing_required: List[str]
