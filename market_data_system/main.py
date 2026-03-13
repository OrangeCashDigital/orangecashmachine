# market_data_system/main.py
"""
OrangeCashMachine – Pipeline Histórico Profesional (Paralelizado + Throttling Adaptativo)
=================================================================

Responsabilidad:
Descargar, transformar y almacenar datos OHLCV históricos de múltiples símbolos y timeframes,
respetando rate limits de exchanges grandes como Binance o Bybit.

Características:
- Descarga incremental automática
- Logging robusto con loguru
- Organización de data lake por símbolo/año/mes
- Manejo seguro de errores y excepciones
- Paralelización segura con control dinámico de hilos
- Reintentos automáticos con backoff exponencial y jitter
- Throttling adaptativo según respuestas del exchange
"""

from pathlib import Path
import yaml
import time
import random
from threading import Semaphore, Lock
from concurrent.futures import ThreadPoolExecutor, as_completed
from loguru import logger

from market_data.historical.fetcher import HistoricalFetcher
from market_data.historical.transformer import OHLCVTransformer
from market_data.historical.historical_storage import HistoricalStorage


def load_config(config_file: Path) -> dict:
    """Carga la configuración YAML desde archivo."""
    try:
        with open(config_file, "r") as f:
            config = yaml.safe_load(f)
        logger.info(f"Configuración cargada desde {config_file}")
        return config
    except Exception as e:
        logger.critical(f"No se pudo cargar la configuración: {e}")
        raise


def safe_retry(max_retries=3, base_delay=1.0, jitter=True):
    """Decorador para reintentos seguros con backoff exponencial y logging."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    delay = base_delay * (2 ** (attempt - 1))
                    if jitter:
                        delay += random.uniform(0, 0.5)
                    logger.warning(f"[Retry {attempt}/{max_retries}] {e}. Reintentando en {delay:.1f}s...")
                    time.sleep(delay)
            logger.error(f"❌ Todos los reintentos fallaron para {args}, {kwargs}")
        return wrapper
    return decorator


class AdaptiveThrottler:
    """
    Control dinámico de hilos y pausas para respetar rate limits.
    Ajusta paralelismo según errores HTTP 429 y tiempos de respuesta.
    """
    def __init__(self, initial_limit=4, min_limit=1, max_limit=12):
        self.limit = initial_limit
        self.min_limit = min_limit
        self.max_limit = max_limit
        self.lock = Lock()
        self.semaphore = Semaphore(initial_limit)

    def acquire(self):
        """Adquirir permiso para ejecutar una tarea."""
        self.semaphore.acquire()

    def release(self):
        """Liberar permiso después de ejecutar la tarea."""
        self.semaphore.release()

    def adjust(self, last_error: bool):
        """Ajusta el número de hilos según el resultado de la última tarea."""
        with self.lock:
            if last_error:
                # Disminuir hilos si hubo rate limit
                self.limit = max(self.min_limit, self.limit - 1)
            else:
                # Incrementar hilos lentamente hasta max_limit
                self.limit = min(self.max_limit, self.limit + 1)
            # Actualizar semáforo
            delta = self.limit - self.semaphore._value
            if delta > 0:
                for _ in range(delta):
                    self.semaphore.release()
            elif delta < 0:
                for _ in range(-delta):
                    self.semaphore.acquire()


class HistoricalPipeline:
    """
    Pipeline histórico con paralelización y throttling adaptativo.
    """

    def __init__(self, symbols, timeframes, start_date, parallelism=4):
        self.symbols = symbols
        self.timeframes = timeframes
        self.start_date = start_date
        self.parallelism = parallelism

        self.fetcher = HistoricalFetcher()
        self.storage = HistoricalStorage()
        self.throttler = AdaptiveThrottler(initial_limit=parallelism)

    @safe_retry(max_retries=3)
    def process_task(self, symbol: str, tf: str):
        """Ejecuta pipeline completo para un símbolo y timeframe con control adaptativo."""
        self.throttler.acquire()
        last_error = False
        try:
            last_ts = self.storage.get_last_timestamp(symbol, tf)
            fetch_from = last_ts.isoformat() if last_ts else self.start_date

            logger.info(f"🔄 Descargando {symbol} {tf} desde {fetch_from}...")

            raw_df = self.fetcher.download_data(symbol, tf, fetch_from)
            if raw_df.empty:
                logger.info(f"No hay datos nuevos para {symbol} {tf}.")
                return

            clean_df = OHLCVTransformer.transform(raw_df)
            self.storage.save_ohlcv(clean_df, symbol, tf, mode="append")

            # Pequeño delay aleatorio para evitar burst requests
            time.sleep(random.uniform(0.05, 0.2))

            logger.success(f"✅ Completado: {symbol} {tf} ({len(clean_df)} filas)")

        except Exception as e:
            logger.error(f"❌ Error en {symbol} {tf}: {e}")
            # Marcar si es error de rate limit para ajustar hilos
            last_error = "429" in str(e) or "Rate limit" in str(e)

        finally:
            self.throttler.adjust(last_error)
            self.throttler.release()

    def run(self):
        """Ejecuta todas las tareas en paralelo con throttling adaptativo."""
        logger.info(f"--- INICIANDO PIPELINE HISTÓRICO PARA {len(self.symbols)} SÍMBOLOS x {len(self.timeframes)} TIMEFRAMES ---")
        tasks = [(s, tf) for s in self.symbols for tf in self.timeframes]

        with ThreadPoolExecutor(max_workers=self.parallelism) as executor:
            future_to_task = {
                executor.submit(self.process_task, symbol, tf): (symbol, tf)
                for symbol, tf in tasks
            }

            for future in as_completed(future_to_task):
                symbol, tf = future_to_task[future]
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"❌ Excepción en hilo para {symbol} {tf}: {e}")

        logger.success("--- PIPELINE HISTÓRICO COMPLETADO ---")


def run_historical_pipeline(parallelism=6):
    """Función wrapper para inicializar y ejecutar el pipeline."""
    config_path = Path(__file__).parent / "config" / "settings.yaml"
    config = load_config(config_path)

    symbols = config.get("symbols", [])
    timeframes = config.get("timeframes", [])
    start_date = config.get("start_date")

    if not symbols or not timeframes or not start_date:
        logger.warning("No hay símbolos, timeframes o start_date definidos en la configuración.")
        return

    pipeline = HistoricalPipeline(
        symbols=symbols,
        timeframes=timeframes,
        start_date=start_date,
        parallelism=parallelism
    )
    pipeline.run()


if __name__ == "__main__":
    log_dir = Path(__file__).parent / "logs"
    log_dir.mkdir(exist_ok=True)
    logger.add(
        log_dir / "historical_pipeline_{time:YYYY-MM-DD}.log",
        rotation="1 day",
        retention="14 days",
        level="INFO",
        enqueue=True,
        backtrace=True,
        diagnose=True,
    )

    logger.info("--- ORANGE CASH MACHINE: STARTING ---")
    run_historical_pipeline(parallelism=6)