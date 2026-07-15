# Transacciones Distribuidas

Implementaciones de protocolos de transacciones distribuidas en Rust.

---

## Ejercicio 10 – Implementación Simplificada (Two-Phase Commit)

Implementación de un protocolo **Two-Phase Commit (2PC)** simplificado utilizando canales `std::sync::mpsc`.

### Arquitectura

- **1 Coordinador** – orquesta la transacción enviando `Prepare` y decidiendo `Commit` o `Rollback`.
- **3 Participantes** – evalúan condiciones locales y responden `YES` o `NO`.

### Fases del protocolo

| Fase | Acción |
|------|--------|
| **Phase 1** | El coordinador envía `Prepare` a todos los participantes. |
| **Phase 1** | Cada participante evalúa condiciones locales y responde con un `Vote(Yes)` o `Vote(No)`. |
| **Phase 2** | El coordinador recopila todos los votos. Si todos son `YES`, envía `Commit`; en caso contrario, envía `Rollback`. |
| **Timeout** | Si un participante no responde en 5 segundos, el coordinador aborta con `Rollback`. |

### Ejecución

```bash
cd Ejercicio10-ImplementacionSimplificada
cargo run
```

### Dependencias

- `rand 0.9` – para simular probabilidades de voto (90% `YES`, 10% `NO`).

---

## Ejercicio 11 – Simulación de Fallos

*(Pendiente)*

---

## Ejercicio 12 – Logging

*(Pendiente)*