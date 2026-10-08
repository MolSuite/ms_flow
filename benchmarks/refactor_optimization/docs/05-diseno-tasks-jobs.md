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
4. Automático: el motor mide lo que tarda un elemento y agrupa los que caben en el objetivo del ejecutor. Un elemento de segundos va solo.

El objetivo hoy es 10 ms y es poco. Medido con 14 workers y elementos que ocupan CPU (tiempo real / tiempo ideal):

| Elemento × cantidad | Objetivo 10 ms | 100 ms | 500 ms |
|---|---|---|---|
| 1 ms × 42 000 | 1,15 | 1,03 | 1,03 |
| 5 ms × 8 400 | 1,22 | 1,02 | 1,07 |
| 20 ms × 2 100 | 1,06 | 1,03 | 1,09 |
| 100 ms × 420 | 1,02 | 1,02 | 1,10 |
| 100 ms × 28 | 1,02 | 1,05 | 2,52 |
| 20 ms × 140 | 1,11 | 1,33 | 2,42 |

Con 10 ms se pierde hasta un 22 %: despachar una tarea cuesta cerca de 1 ms cuando todos los núcleos están ocupados. Con 100 ms la pérdida baja al 2-3 %. Con 500 ms no se gana nada más y los jobs pequeños tardan 2,5 veces más, porque los últimos elementos se agrupan en pocos workers y el resto queda parado. Propuesta: objetivo de 100 ms en local, propio de cada ejecutor, y no agrupar más de lo que deja a todos los workers con trabajo (elementos leídos / workers). Con esa segunda regla el caso de 20 ms × 140 deja de perder.

**Progreso.** Del job: (elementos hechos + fracción de los que corren) / total. La fracción llega por casillas en un fichero mapeado en memoria, no por colas. Lote de un elemento: `report()` de la task, que el motor pide solo cuando el job tiene pocos elementos (hasta 2 × workers; se fuerza con `progress=`). Lote de varios: el worker cuenta los hechos solo. En HPC, únicamente elementos hechos por lote, leídos al sondear.

**Total.** Al empezar, `count(*)` del filtro con un tope `max(id)` por tabla leída; el tope se guarda con el job. Las filas nuevas no entran, ni al reanudar. Durante la ejecución el total se recalcula cada 30 s (configurable) como leídos + los que le quedan a la fuente, y queda exacto al agotarse. El recuento corre en el hilo que lee la fuente.

**Dependencias.** Un job espera a todo job activo que escribe una tabla que él lee. Lo que escribe sale de `output`; lo que lee, de la fuente de `ms_flow` o de `reads=`. `after=` añade dependencias a mano.

**Resultados.** Una sola puerta: lo que devuelve la task va a `output`. `finalize` corre una vez al terminar.

**Datos y base de datos.** La base de datos solo se toca en el proceso que lanza el job. El hilo del job lee la fuente y arma el elemento con datos planos y rutas; el hilo de entrega escribe en `output` lo que devuelve la task. Un worker nunca recibe una conexión ni la ruta de la base: recibe datos y rutas de ficheros, y devuelve filas.

**Entradas con ficheros.** Un `FileInputSpec` en un elemento lo resuelve el ejecutor (en local, la ruta tal cual; en remoto, copiar el fichero, una vez si varios elementos lo comparten). El motor llama a esa función desde el hilo del job, nunca desde el hilo de eventos: `submit` de un ejecutor no puede bloquear.

**Preparación en remoto.** Para lo que era `setup`: lo que es de cada elemento va por la regla anterior; lo que es una vez por job es el código de la fuente antes de su primer `yield` (corre una vez, en el proceso que lanza, antes de cualquier elemento) o un job previo con `after=`. Si con ejecutores remotos no basta, se añade entonces un `setup=` al job.

**Registro de lo hecho.** El journal sigue registrando cada elemento hecho cuando el job se envía con `journal=` y `key=`. Quien invoca decide qué pide (por ejemplo, excluir lo ya calculado); `ms_flow` sabe por sí mismo qué tiene activo y qué tiene registrado. El total al reanudar usa lo registrado.

## Qué desaparece

- La clase `JobSpec`, las plantillas `streaming_job`, `batch_job` y `build_streaming_job_definition`, y `molsuite.task` / `molsuite.job`.
- De la declaración: `required`, `produces`, `setup`, `stage_chunk`, `stage_fail_policy`, `max_stage_failures`, `store_results`, `result_handler_factory`, `cpu_required` (pasa a `cpu`, `gpu`, `mem_mb`) y los pares función / `*_ref` (la referencia se deriva).
- `InputSource.iter_chunks` y `batch_size`; queda `iter_items`.
- El saco `config` con objetos vivos: la fuente recibe el proyecto; la task, solo parámetros y rutas.
- En AMDockVS, más adelante: las reglas por nombre de `runtime.py` y los `progress_cb`.

## Abierto

1. Objetivo del lote automático: 100 ms (medido) frente a los 500 ms que propuso Mario.
2. Al reanudar, el motor avisa si la fuente no trae ninguno de los elementos hechos. Con una fuente que excluye lo hecho ese aviso es falso: quitarlo o dejar que el job lo apague.
3. Tamaño de lote en HPC: Mario tiene detalles que contar cuando se configure.
4. La capa de workflows (`workflow()`, `WorkflowSpec`) queda fuera de este documento.
