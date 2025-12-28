"""K-POP 아티스트 패턴 및 추출 유틸리티."""
from typing import Dict, List

# Canonical artist labels (uppercase ASCII) mapped to recognizable patterns.
ARTIST_PATTERNS: Dict[str, List[str]] = {
    "BTS": ["bts", "bangtan", "방탄소년단", "防弾少年団"],
    "BLACKPINK": ["blackpink", "블랙핑크", "블핑", "bp"],
    "TWICE": ["twice", "트와이스"],
    "NEWJEANS": ["newjeans", "뉴진스"],
    "AESPA": ["aespa", "æspa", "에스파"],
    "LESSERAFIM": ["le sserafim", "르세라핌", "lesserafim"],
    "IVE": ["ive", "아이브"],
    "ITZY": ["itzy", "있지"],
    "NMIXX": ["nmixx", "엔믹스"],
    "KEPLER": ["kep1er", "케플러", "케이플러"],
    "STRAYKIDS": ["stray kids", "스트레이 키즈", "skz"],
    "ATEEZ": ["ateez", "에이티즈"],
    "ENHYPEN": ["enhypen", "엔하이픈"],
    "SEVENTEEN": ["seventeen", "세븐틴", "svt"],
    "NCT": ["nct", "엔시티"],
    "GOT7": ["got7", "갓세븐"],
    "BIGBANG": ["bigbang", "빅뱅"],
    "EXO": ["exo", "엑소"],
    "GIRLSGENERATION": ["girls generation", "snsd", "소녀시대"],
    "2NE1": ["2ne1", "투애니원"],
    "SHINEE": ["shinee", "샤이니"],
    "BABYMONSTER": ["babymonster", "베이비몬스터"],
    "RIIZE": ["riize", "라이즈"],
    "ZEROBASEONE": ["zerobaseone", "제로베이스원", "zb1"],
}

# Known official YouTube channel IDs mapped to canonical artist labels.
OFFICIAL_CHANNELS: Dict[str, str] = {
    "UC2eCJnq0L3cEGAy9d2kWcww": "NEWJEANS",
    "UC9YddkK5P9f9B8D7mN6A9pQ": "AESPA",
    "UC3IZKseVpdzPSBaWxBxundA": "BLACKPINK",
    "UC3IZ7CGYBbbZpg6_CwKM2w": "BTS",
    "UCWEV31bGbZKO3-3kMXHUN4w": "RIIZE",
    "UC4LH5FswA1l4R77_c2zY5gQ": "LESSERAFIM",
    "UCn8zNIfYAQNdrFRrr8oibKw": "IVE",
    "UCwgtORdDtUKhpjE1VBvCTdQ": "ITZY",
    "UC4hGkv5ayUBsSOU10isjk6Q": "NMIXX",
    "UCsgS6iSH_zGqtdx6b9MxbpA": "KEPLER",
    "UC9CuvdOVfMPvKCiwdGKL3cQ": "STRAYKIDS",
}


def extract_artist(title: str, channel_id: str = None) -> str:
    """Infer a canonical artist label from title/channel metadata."""
    if channel_id and channel_id in OFFICIAL_CHANNELS:
        return OFFICIAL_CHANNELS[channel_id]

    title_norm = title.lower() if title else ""
    if title_norm:
        for canonical, patterns in ARTIST_PATTERNS.items():
            for pattern in patterns:
                if pattern in title_norm:
                    return canonical

    return "OTHER"


if __name__ == "__main__":
    # Test
    print(extract_artist("BTS - Dynamite Official MV"))
    print(extract_artist("NewJeans 'Super Shy'", "UC2eCJnq0L3cEGAy9d2kWcww"))
    print(extract_artist("Random Video"))
