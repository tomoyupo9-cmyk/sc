from dataclasses import dataclass
from typing import List, Optional

@dataclass
class TrendPoint:
    timestamp: str
    score: float

@dataclass
class Trends:
    latest: float
    series: List[TrendPoint]

@dataclass
class BbsStats:
    posts_24h: int
    posts_72h: int
    threads_sampled: int

@dataclass
class NewsItem:
    title: str
    link: str
    published: str
    source: Optional[str] = None

@dataclass
class NewsStats:
    count_24h: int
    count_72h: int
    items: List[NewsItem]

@dataclass
class SymbolRow:
    ticker: str
    name: str
    trends: Optional[Trends]
    bbs: Optional[BbsStats]
    news: Optional[NewsStats]
    score: float

@dataclass
class Output:
    generated_at: str
    rows: List[SymbolRow]
