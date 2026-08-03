# Gobernanza de arquitectura — OrangeCashMachine

Fase 0: preservación de conocimiento. SSOT de reglas sobre cómo se
documenta, respalda y decide arquitectura en OCM.

## 1. Archivos y artefactos críticos

- architecture/importlinter.toml
- **/bootstrap/composition_root.py de cada bounded context
- ocm/config/schema.py
- shared/contracts/boundaries.py
- docs/architecture/decisions/
- docs/architecture/recovered/

Regla dura: ningún archivo bajo **/bootstrap/ se considera terminado
hasta estar committeado a git.

## 2. Cuándo un cambio requiere ADR

- Cambiar la firma del constructor de cualquier Composition Root.
- Agregar o eliminar un contrato de architecture/importlinter.toml.
- Cambiar quién es dueño de un estado mutable compartido.
- Documentar y posponer deuda técnica en vez de resolverla ya.

No requiere ADR: bugfixes, refactors internos sin cambio de contrato
público, cambios de test.

## 3. Sistema de ADR

- Ubicación: docs/architecture/decisions/ADR-NNNN-titulo-slug.md
- Template: docs/architecture/decisions/ADR-template.md
- Numeración secuencial, nunca se reutiliza un número eliminado.

## 4. Documentación de recuperaciones forenses

1. Backup inmediato fuera del working tree antes de tocar nada.
2. Documento con secciones separadas: evidencia objetiva, comparación
   con estado actual, decisiones de arquitectura.
3. No se reconstruye código roto solo para completar lo perdido.

## 5. Backups

Cualquier artefacto irremplazable se respalda fuera del repo git antes
de cualquier otra acción. Los backups no reemplazan el commit a git.

## 6. Inventario de activos arquitectónicos

Ver docs/architecture/INVENTORY.md (pendiente de crear): bounded
contexts, composition roots, ports, adapters, contratos BC-NN activos.
