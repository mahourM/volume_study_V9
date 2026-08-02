from __future__ import annotations

import asyncio
import logging

from absorption.html_server import AbsorptionHtmlServer
from absorption.session_service import AbsorptionFootprintService
from config.config_runtime import RuntimeConfig
<<<<<<< HEAD
from cme_provider.local_data import CmeLocalDataCatalog
from core.symbol_resolver import CmeSymbolResolver, MarketSymbolResolver
from core.symbol_session_registry import SymbolSessionRegistry
from core.system_models import SystemInitRequest, SystemInitSymbolSpec
=======
from core.performance_metrics import get_performance_metrics_recorder
from core.symbol_resolver import BinanceSymbolResolver
from core.symbol_session_registry import SymbolSessionRegistry
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
from core.system_startup_router import SystemStartupRouter
from execution.trading_decision_event_recorder import get_trading_decision_event_recorder
from network.tcp_json_server import TcpJsonServer

LOGGER = logging.getLogger(__name__)


<<<<<<< HEAD
async def main_async(runtime_config: RuntimeConfig | None = None) -> None:
    runtime_config = runtime_config or RuntimeConfig()
    get_trading_decision_event_recorder().configure(
        output_path=runtime_config.project_root / "runtime_metrics" / "trading_decision_events.csv",
    )
    cme_catalog = CmeLocalDataCatalog(
        data_dir=runtime_config.project_root / runtime_config.cme_local_data_dir_name,
        dataset=runtime_config.cme_dataset,
        schema=runtime_config.cme_schema,
        default_tick_size=runtime_config.cme_default_tick_size,
    )
    symbol_resolver = MarketSymbolResolver(
        cme_resolver=CmeSymbolResolver(
            available_symbols=cme_catalog.available_symbols(),
            dataset=runtime_config.cme_dataset,
            schema=runtime_config.cme_schema,
            tick_sizes={
                symbol: str(cme_catalog.tick_size_for(symbol))
                for symbol in cme_catalog.available_symbols()
            },
        )
    )
    session_registry = SymbolSessionRegistry(symbol_resolver)
    absorption_service = AbsorptionFootprintService(runtime_config)
    _bootstrap_cme_study_sessions(
        cme_catalog=cme_catalog,
        session_registry=session_registry,
        absorption_service=absorption_service,
    )
=======
async def main_async() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    runtime_config = RuntimeConfig()
    metrics_recorder = get_performance_metrics_recorder()
    metrics_recorder.configure(
        output_path=runtime_config.project_root / "runtime_metrics" / "ws_performance_metrics.csv",
    )
    get_trading_decision_event_recorder().configure(
        output_path=runtime_config.project_root / "runtime_metrics" / "trading_decision_events.csv",
    )
    metrics_recorder.start()
    symbol_resolver = BinanceSymbolResolver()
    session_registry = SymbolSessionRegistry(symbol_resolver)
    absorption_service = AbsorptionFootprintService(runtime_config)
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
    absorption_html_server = AbsorptionHtmlServer(
        host=runtime_config.absorption_http_host,
        port=runtime_config.absorption_http_port,
        snapshot_provider=absorption_service,
    )
    request_router = SystemStartupRouter(session_registry, absorption_service)
    tcp_json_server = TcpJsonServer(
        host=runtime_config.tcp_host,
        port=runtime_config.tcp_port,
        request_router=request_router,
    )

    LOGGER.info(
        "PYTHON_STARTUP_READY | host=%s | port=%d | dpc_path=shell | absorption_path=shell",
        runtime_config.tcp_host,
        runtime_config.tcp_port,
    )
    LOGGER.info(
        "ABSORPTION_FOOTPRINT_HTML_READY | host=%s | port=%d",
        runtime_config.absorption_http_host,
        runtime_config.absorption_http_port,
    )
    await asyncio.gather(
        tcp_json_server.run_forever(),
        absorption_service.run_forever(),
        absorption_html_server.run_forever(),
    )


<<<<<<< HEAD
def run() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    asyncio.run(main_async(RuntimeConfig()))


def _bootstrap_cme_study_sessions(
    *,
    cme_catalog: CmeLocalDataCatalog,
    session_registry: SymbolSessionRegistry,
    absorption_service: AbsorptionFootprintService,
) -> list:
    available_symbols = cme_catalog.available_symbols()
    if not available_symbols:
        return []

    request = SystemInitRequest(
        symbols_count=len(available_symbols),
        symbols=tuple(
            SystemInitSymbolSpec(mt5_symbol=symbol)
            for symbol in available_symbols
        ),
        schema_version="7.0",
    )
    sessions = session_registry.initialize(request)
    absorption_service.configure_sessions(sessions)
    LOGGER.info(
        "CME_STUDY_BOOTSTRAP_READY | symbols=%s | sessions=%d",
        ",".join(available_symbols),
        len(sessions),
    )
    return sessions


if __name__ == "__main__":
    run()
=======
if __name__ == "__main__":
    asyncio.run(main_async())
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
