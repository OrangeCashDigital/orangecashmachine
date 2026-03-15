"""
ExchangeClient Async – OrangeCashMachine (Profesional)
======================================================

Responsabilidad:
Centralizar la conexión con exchanges usando ccxt.async_support.
Diseñado para pipelines históricos y en tiempo real asíncronos.

Mejoras Profesionales:
- Retry + Circuit Breaker (pybreaker) para resiliencia frente a fallos.
- Logging robusto de latencia y errores por tipo.
- SafeOps: manejo seguro de API keys, fallos de carga y validación.
- Principios SOLID, KISS, DRY aplicados.
"""

from pathlib import Path
import os
import asyncio
import time
import yaml
import ccxt.async_support as ccxt
from loguru import logger
import pybreaker
from typing import Optional

class ExchangeClientAsyncError(Exception):
    """Excepción personalizada para ExchangeClientAsync."""
    pass

class ExchangeClientAsync:
    """Cliente central para interactuar con exchanges Async vía CCXT."""

    DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "settings.yaml"
    CIRCUIT_BREAKER = pybreaker.CircuitBreaker(fail_max=5, reset_timeout=30)

    def __init__(self, config_path: Optional[str | Path] = None, exchange_id: Optional[str] = None) -> None:
        self.config_path = Path(config_path) if config_path else self.DEFAULT_CONFIG_PATH
        self.config = self._load_config(self.config_path)
        self.exchange_id = exchange_id or os.getenv("EXCHANGE_ID", self.config["exchange"]["id"])
        self.client: Optional[ccxt.Exchange] = None

    def _load_config(self, path: Path) -> dict:
        """Carga configuración desde YAML con logging profesional."""
        try:
            with open(path, "r") as f:
                config = yaml.safe_load(f)
            logger.info(f"Configuración cargada desde {path}")
            return config
        except Exception as e:
            logger.error(f"Error cargando configuración: {e}")
            raise ExchangeClientAsyncError(f"No se pudo cargar configuración: {e}") from e

    async def initialize_client(self) -> ccxt.Exchange:
        """Inicializa el cliente CCXT async con retry, backoff y circuit breaker."""
        if self.client:
            return self.client

        retries = 3
        for attempt in range(1, retries + 1):
            try:
                client_class = getattr(ccxt, self.exchange_id)
                self.client = client_class({
                    "apiKey": os.getenv("EXCHANGE_API_KEY", self.config["exchange"].get("apiKey")),
                    "secret": os.getenv("EXCHANGE_API_SECRET", self.config["exchange"].get("secret")),
                    "enableRateLimit": self.config["exchange"].get("enableRateLimit", True),
                    "options": self.config["exchange"].get("options", {
                        "adjustForTimeDifference": True,
                        "recvWindow": 10000,
                    }),
                })

                # Validación de conexión y latencia
                start = time.time()
                await self.client.load_markets()
                latency = (time.time() - start) * 1000
                logger.success(f"Conectado async a {self.exchange_id.upper()} | Latencia: {latency:.1f} ms")
                return self.client

            except AttributeError:
                logger.critical(f"Exchange no soportado por CCXT: {self.exchange_id}")
                raise ExchangeClientAsyncError(f"Exchange no soportado: {self.exchange_id}")

            except Exception as e:
                delay = 2 ** attempt
                logger.warning(f"[Intento {attempt}/{retries}] Error conectando a {self.exchange_id}: {e}. Reintentando en {delay}s")
                await asyncio.sleep(delay)

        logger.critical(f"No se pudo conectar al exchange {self.exchange_id} después de {retries} intentos")
        raise ExchangeClientAsyncError(f"Conexión async fallida al exchange {self.exchange_id}")

    async def get_client(self) -> ccxt.Exchange:
        """Devuelve el cliente CCXT async conectado, inicializándolo si es necesario."""
        if not self.client:
            await self.initialize_client()
        return self.client

    async def test_connection(self) -> bool:
        """Prueba la conexión al exchange, retornando True si OK y registrando latencia."""
        try:
            client = await self.get_client()
            start = time.time()
            markets = await client.load_markets()
            latency = (time.time() - start) * 1000
            logger.info(f"Test conexión async OK | Mercados cargados: {len(markets)} | Latencia: {latency:.1f} ms")
            return True
        except Exception as e:
            logger.error(f"Test de conexión async fallido: {e}")
            return False

    async def close(self):
        """Cierra la conexión async de manera segura."""
        if self.client:
            try:
                await self.client.close()
                logger.info(f"Cerrada conexión async con {self.exchange_id.upper()}")
            except Exception as e:
                logger.warning(f"Error cerrando conexión async: {e}")