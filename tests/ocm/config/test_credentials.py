"""Tests del resolver de credenciales (SSOT) — ocm.config.credentials."""

from __future__ import annotations

from ocm.config.credentials import resolve_exchange_credentials, resolve_provider_api_key


class TestResolveProviderApiKey:
    def test_provider_specific_env_wins(self, monkeypatch):
        monkeypatch.setenv("COINGLASS_API_KEY", "provider")
        monkeypatch.setenv("OCM_API_KEY", "generic")
        assert resolve_provider_api_key("COINGLASS") == "provider"

    def test_falls_back_to_generic_ocm_api_key(self, monkeypatch):
        monkeypatch.delenv("COINGLASS_API_KEY", raising=False)
        monkeypatch.setenv("OCM_API_KEY", "generic")
        assert resolve_provider_api_key("COINGLASS") == "generic"

    def test_empty_when_no_credentials(self, monkeypatch):
        monkeypatch.delenv("COINGLASS_API_KEY", raising=False)
        monkeypatch.delenv("OCM_API_KEY", raising=False)
        assert resolve_provider_api_key("COINGLASS") == ""

    def test_yaml_credentials_used_when_no_env(self, monkeypatch):
        monkeypatch.delenv("COINGLASS_API_KEY", raising=False)
        monkeypatch.delenv("OCM_API_KEY", raising=False)
        assert resolve_provider_api_key("COINGLASS", {"apiKey": "yaml-key"}) == "yaml-key"

    def test_env_beats_yaml(self, monkeypatch):
        monkeypatch.setenv("COINGLASS_API_KEY", "env-key")
        monkeypatch.delenv("OCM_API_KEY", raising=False)
        assert resolve_provider_api_key("COINGLASS", {"apiKey": "yaml-key"}) == "env-key"

    def test_none_credentials_yaml_is_safe(self, monkeypatch):
        monkeypatch.delenv("COINGLASS_API_KEY", raising=False)
        monkeypatch.delenv("OCM_API_KEY", raising=False)
        assert resolve_provider_api_key("COINGLASS", None) == ""


class TestResolveExchangeCredentials:
    def test_api_key_delegates_to_provider_resolver(self, monkeypatch):
        monkeypatch.setenv("BINANCE_API_KEY", "k")
        monkeypatch.delenv("OCM_API_KEY", raising=False)
        assert resolve_exchange_credentials("BINANCE", {})["api_key"] == "k"

    def test_api_secret_and_password_env(self, monkeypatch):
        monkeypatch.setenv("BINANCE_API_KEY", "k")
        monkeypatch.setenv("BINANCE_API_SECRET", "s")
        monkeypatch.setenv("BINANCE_PASSPHRASE", "p")
        res = resolve_exchange_credentials("BINANCE", {})
        assert (res["api_key"], res["api_secret"], res["api_password"]) == ("k", "s", "p")

    def test_all_keys_present_when_nothing_set(self, monkeypatch):
        vars_ = (
            "BINANCE_API_KEY",
            "BINANCE_API_SECRET",
            "BINANCE_PASSPHRASE",
            "BINANCE_PASSWORD",
            "OCM_API_KEY",
            "OCM_API_SECRET",
        )
        for var in vars_:
            monkeypatch.delenv(var, raising=False)
        res = resolve_exchange_credentials("BINANCE", {})
        assert res == {"api_key": "", "api_secret": "", "api_password": ""}
