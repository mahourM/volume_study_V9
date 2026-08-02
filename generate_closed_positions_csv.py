from __future__ import annotations

import logging

from execution.closed_position_report_generator import generate_closed_position_report


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    result = generate_closed_position_report()
    print(
        "closed_positions_csv_generated "
        f"timeframe={result.timeframe} "
        f"symbols={','.join(result.symbols)} "
        f"closed_positions={result.closed_positions}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
