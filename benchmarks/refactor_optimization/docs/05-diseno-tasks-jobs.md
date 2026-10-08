# 05 — Diseño de tasks y jobs

Borrador del 2026-10-08 para revisar. Nada construido. Sale de la auditoría (`04-auditoria-tasks-jobs.md`) y de sus cuatro rondas de discusión.

## Piezas

| Pieza | Qué es | Quién la escribe |
|---|---|---|
| Task | La función sobre **un** elemento, y lo que necesita: CPU, GPU, memoria, tiempo máximo, reintentos | El autor del job |
| Fuente | Un generador que da los elementos uno a uno | El autor, o una de `ms_flow` |
| Job | Task + fuente + destino de los resultados | El autor |
| Envío | Parámetros, ejecutor, dependencias extra | Quien lanza |
| Lote (chunk) | Cuántos elementos viajan juntos a un worker | El ejecutor, con ajustes |

## Cómo se escribe

```python
from ms_flow import job, report, table_sink, task


@task(cpu=1, mem_mb=500, timeout=600, retries=1)
def dock(pair: dict, params: DockingJobParams) -> list[dict]:
    """Dock one receptor-ligand pair."""
    ...
    report(0.4)  # optional: does nothing unless the engine asked for fine progress
    return rows


@job(
    task=dock,
    name="amdock_docking_job",
    params_model=DockingJobParams,
    output=table_sink(model=DockingResultRecord, write_mode="bulk"),
    reads=("engine_states", "binding_sites"),
)
def docking_job(params: DockingJobParams, project):
    """The source: one pair at a time, already-docked pairs left out."""
    for pair in pending_pairs(project, params):
        yield pair


job_id = molsuite.submit(docking_job, params={...})  # executor=, batch=, after=, progress=
```

Es la forma de hoy (`@task` y `@job` sobre la función que reparte el trabajo) con tres cambios: la task recibe un elemento, la fuente da elementos y no lotes armados a mano, y los parámetros del job viajan una vez por lote en lugar de copiarse en cada elemento. Si la fuente ya existe como objeto (`ProjectTableInput`, `ProjectQueryInput`), se pasa con `source=` y no se decora nada.

Una herramienta que solo sabe trabajar por lotes lo declara (`@task(batched=True)`) y recibe la lista.

## Reglas

**Lote.** Por orden de prioridad:

1. `batch=` en el envío.
2. La configuración del ejecutor para esa task (lo normal en HPC).
3. `@task(batch=N)`: lo que el autor sabe de su trabajo.
4. Automático: el motor mide lo que tarda un elemento y agrupa los que caben en 10 ms, hasta 64. Un elemento de segundos va solo.

**Progreso.** Del job: (elementos hechos + fracción de los que corren) / total. La fracción llega por casillas en un fichero mapeado en memoria, no por colas. Lote de un elemento: `report()` de la task, que el motor pide solo cuando el job tiene pocos elementos (hasta 2 × workers; se fuerza con `progress=`). Lote de varios: el worker cuenta los hechos solo. En HPC, únicamente elementos hechos por lote, leídos al sondear.

**Total.** Al empezar, `count(*)` del filtro con un tope `max(id)` por tabla leída; el tope se guarda con el job. Las filas nuevas no entran, ni al reanudar. Durante la ejecución el total se recalcula cada 30 s (configurable) como leídos + los que le quedan a la fuente, y queda exacto al agotarse. El recuento corre en el hilo que lee la fuente.

**Dependencias.** Un job espera a todo job activo que escribe una tabla que él lee. Lo que escribe sale de `output`; lo que lee, de la fuente de `ms_flow` o de `reads=`. `after=` añade dependencias a mano.

**Resultados.** Una sola puerta: lo que devuelve la task va a `output`. `finalize` corre una vez al terminar.

**Entradas con ficheros.** Un `FileInputSpec` en un elemento se resuelve en el hilo que lee la fuente, con la función que da el ejecutor (en local, la ruta tal cual; en remoto, subir el fichero). Nunca en el hilo de eventos.

## Qué desaparece

- La clase `JobSpec`, las plantillas `streaming_job`, `batch_job` y `build_streaming_job_definition`, y `molsuite.task` / `molsuite.job`.
- De la declaración: `required`, `produces`, `setup`, `stage_chunk`, `stage_fail_policy`, `max_stage_failures`, `store_results`, `result_handler_factory`, `cpu_required` (pasa a `cpu`, `gpu`, `mem_mb`) y los pares función / `*_ref` (la referencia se deriva).
- `InputSource.iter_chunks` y `batch_size`; queda `iter_items`.
- El saco `config` con objetos vivos: la fuente recibe el proyecto; la task, solo parámetros y rutas.
- En AMDockVS, más adelante: las reglas por nombre de `runtime.py` y los `progress_cb`.

## Abierto

1. Confirmar la regla de «entradas con ficheros».
2. Qué recibe la task además del elemento y los parámetros (¿rutas del proyecto?).
3. Con una fuente que ya excluye lo hecho, el registro por elemento del journal sobra: decidir si esos jobs lo apagan.
4. Tamaño de lote en HPC: Mario tiene detalles que contar cuando se configure.
5. La capa de workflows (`workflow()`, `WorkflowSpec`) queda fuera de este documento.
