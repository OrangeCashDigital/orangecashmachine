"""
market_data.factories — composition root del bounded context market_data.

Este paquete tiene licencia explícita (BC-28) para importar desde
todas las capas internas: domain, ports, application, adapters,
infrastructure. Es el único lugar donde se instancian dependencias
concretas y se cabla el grafo de objetos.

Ninguna otra capa importa desde market_data.factories.
"""
