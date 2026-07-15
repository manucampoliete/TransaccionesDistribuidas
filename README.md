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

Modificación del Ejercicio 10 donde el comportamiento de cada participante es **forzado** (`Behavior::RespondWith` / `Behavior::Delay`) en lugar de aleatorio, para poder reproducir a demanda cada escenario de falla.

| Caso | Qué se fuerza | Qué ocurre |
|------|----------------|------------|
| **a) Voto NO** | Un participante responde `Vote::No`, los otros dos `Vote::Yes`. | El coordinador recibe los 3 votos dentro del timeout, detecta que no son unánimes y envía `Rollback` a todos. |
| **b) Timeout** | Un participante duerme 6s antes de votar (supera el `recv_timeout(5s)` del coordinador). | El coordinador agota el tiempo de espera en la tercera iteración, asume falla del participante lento y envía `Rollback` a todos de inmediato — sin esperar la respuesta tardía. Cuando el participante lento despierta e intenta votar, el envío falla silenciosamente porque el coordinador ya cerró el canal; igual recibe el `Rollback` que ya le habían enviado. |
| **c) Rollback forzado** | Los 3 participantes votan `YES`, pero se pasa `force_rollback = true`. | Pese a la unanimidad, el coordinador decide abortar la transacción (ej. por una regla de negocio externa al protocolo) y envía `Rollback` a todos. |

### Ejecución

```bash
cd Ejercicio11-SimulacionDeFallos
cargo run
```

---

## Ejercicio 12 – Logging

Mismo protocolo del Ejercicio 10 (votación aleatoria vía `rand`), agregando un **Write-Ahead Log**: cada participante escribe su intención en un archivo (`wal_participant_<id>.log`) *antes* de ejecutar la acción correspondiente (`Prepared`, `Commit` o `Rollback`).

Este registro previo es indispensable porque es lo único que sobrevive a un crash del proceso: si el participante cae justo después de aplicar la acción pero antes de confirmarlo, el WAL en disco ya refleja qué se pretendía hacer, permitiendo que al reiniciar se pueda determinar el estado correcto en lugar de quedar en un estado ambiguo (Durabilidad y Safety).

### Ejecución

```bash
cd Ejercicio12-Logging
cargo run
```