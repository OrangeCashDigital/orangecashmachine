from __future__ import annotations

"""core/config/loader/validator.py — Validación Pydantic y reglas de negocio."""

from pydantic import ValidationError

from .exceptions import ConfigurationError, ConfigValidationError


class ConfigValidator:
    @staticmethod
    def validate(data: dict, source: str):
        from core.config.schema import AppConfig
        from core.config.rules  import check_all_rules
        try:
            config = AppConfig.model_validate(data)
        except ValidationError as exc:
            errors = "\n".join(
                f"  [{' -> '.join(map(str, err['loc']))}] {err['msg']}"
                for err in exc.errors()
            )
            raise ConfigurationError(f"Validation error ({source}):\n{errors}") from exc
        rule_errors = check_all_rules(config, source)
        if rule_errors:
            raise ConfigValidationError(
                f"Business rule violations ({source}):\n"
                + "\n".join(f"  - {e}" for e in rule_errors)
            )
        return config
