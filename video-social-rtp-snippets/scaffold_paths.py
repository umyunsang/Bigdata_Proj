"""Create local data/checkpoint folders under this snippet directory."""
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent
    for p in [
        root / "data" / "landing",
        root / "data" / "bronze",
        root / "data" / "silver",
        root / "data" / "gold",
        root / "chk",
    ]:
        p.mkdir(parents=True, exist_ok=True)
        print("Ensured:", p)


if __name__ == "__main__":
    main()

