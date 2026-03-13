"""
Exchange Client Connector – OrangeCashMachine
=============================================

Responsabilidad:
Centralizar la conexión con exchanges usando CCXT, leyendo configuración
desde settings.yaml o variables de entorno, para pipelines históricos y
realtime.

Arquitectura:
connectors → acceso a servicios externos
"""

from pathlib import Path
import os
import ccxt
import yaml
from loguru import logger


class ExchangeClient:
    """
    Cliente central para interactuar con exchanges vía CCXT.

    Características:
    - Lee configuración desde settings.yaml o variables de entorno.
    - Maneja errores de conexión y validación de exchange.
    - Preparado para ser usado por múltiples pipelines sin duplicar conexión.
    """

    def __init__(self, config_path: str | Path | None = None) -> None:
        """
        Inicializa el cliente Exchange.

        Args:
            config_path (str | Path | None): Ruta al archivo YAML de configuración.
                                             Si no se proporciona, usa la ruta
                                             por defecto en config/settings.yaml.
        """

        if config_path is None:
            config_path = Path(__file__).resolve().parents[2] / "config" / "settings.yaml"

        self.config = self._load_config(config_path)
        self.exchange_id = os.getenv("EXCHANGE_ID", self.config["exchange"]["id"])
        self.client = self._initialize_client()

    def _load_config(self, path: str | Path) -> dict:
        """
        Carga la configuración YAML desde el archivo.

        Args:
            path (str | Path): Ruta al archivo YAML.

        Returns:
            dict: Diccionario con la configuración cargada.

        Raises:
            Exception: Si no se puede leer el archivo o es inválido.
        """
        try:
            with open(path, "r") as f:
                config = yaml.safe_load(f)
            logger.info(f"Configuración cargada desde {path}")
            return config
        except Exception as e:
            logger.error(f"Error cargando configuración: {e}")
            raise

    def _initialize_client(self) -> ccxt.Exchange:
        """
        Inicializa y valida el cliente CCXT para el exchange configurado.

        Returns:
            ccxt.Exchange: Instancia del cliente CCXT conectado.

        Raises:
            AttributeError: Si el exchange no es soportado por CCXT.
            Exception: Si ocurre un fallo de conexión.
        """
        try:
            # Selecciona la clase del exchange dinámicamente
            exchange_class = getattr(ccxt, self.exchange_id)

            client = exchange_class(
                {
                    "apiKey": os.getenv("EXCHANGE_API_KEY", self.config["exchange"].get("apiKey")),
                    "secret": os.getenv("EXCHANGE_API_SECRET", self.config["exchange"].get("secret")),
                    "enableRateLimit": self.config["exchange"].get("enableRateLimit", True),
                    "options": self.config["exchange"].get("options", {
                        "adjustForTimeDifference": True,
                        "recvWindow": 10000,
                    }),
                }
            )

            # Validación básica: carga mercados
            client.load_markets()
            logger.success(f"Conectado al exchange {self.exchange_id.upper()}")
            return client

        except AttributeError:
            logger.critical(f"Exchange no soportado por CCXT: {self.exchange_id}")
            raise

        except Exception as e:
            logger.critical(f"Error conectando al exchange {self.exchange_id}: {e}")
            raise

    def get_client(self) -> ccxt.Exchange:
        """
        Devuelve la instancia del cliente CCXT.

        Returns:
            ccxt.Exchange: Cliente conectado.
        """
        return self.client