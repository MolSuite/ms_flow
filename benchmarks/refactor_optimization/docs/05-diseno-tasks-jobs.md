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
4. Automático: el motor mide lo que tarda un elemento y agrupa los que caben en el objetivo del ejecutor (100 ms en local). Un elemento de segundos va solo.

El objetivo era 10 ms y era poco. Medido con 14 workers y elementos que ocupan CPU (tiempo real / tiempo ideal):

| Elemento × cantidad | Objetivo 10 ms | 100 ms | 500 ms |
|---|---|---|---|
| 1 ms × 42 000 | 1,15 | 1,03 | 1,03 |
| 5 ms × 8 400 | 1,22 | 1,02 | 1,07 |
| 20 ms × 2 100 | 1,06 | 1,03 | 1,09 |
| 100 ms × 420 | 1,02 | 1,02 | 1,10 |
| 100 ms × 28 | 1,02 | 1,05 | 2,52 |
| 20 ms × 140 | 1,11 | 1,33 | 2,42 |

Con 10 ms se pierde hasta un 22 %: despachar una tarea cuesta cerca de 1 ms cuando todos los núcleos están ocupados. Con 100 ms la pérdida baja al 2-3 %. Con 500 ms no se gana nada más y los jobs pequeños tardan 2,5 veces más, porque los últimos elementos se agrupan en pocos workers y el resto queda parado. Decidido el 2026-10-08 y hecho en la rama `refactor/engine-pack-target`: objetivo de 100 ms, y cuando la fuente se agota lo que queda se reparte entre los workers. Con las dos cosas, 20 ms × 140 queda en 1,09 y 100 ms × 28 en 1,04. Falta que cada ejecutor ponga su propio objetivo.

**Progreso.** Del job: (elementos hechos + fracción de los que corren) / total. La fracción llega por casillas en un fichero mapeado en memoria, no por colas. Lote de un elemento: `report()` de la task, que el motor pide solo cuando el job tiene pocos elementos (hasta 2 × workers; se fuerza con `progress=`). Lote de varios: el worker cuenta los hechos solo. En HPC, únicamente elementos hechos por lote, leídos al sondear.

**Total.** Al empezar, `count(*)` del filtro con un tope `max(id)` por tabla leída; el tope se guarda con el job. Las filas nuevas no entran, ni al reanudar. Durante la ejecución el total se recalcula cada 30 s (configurable) como leídos + los que le quedan a la fuente, y queda exacto al agotarse. El recuento corre en el hilo que lee la fuente.

**Dependencias.** Un job espera a todo job activo que escribe una tabla que él lee. Lo que escribe sale de `output`; lo que lee, de la fuente de `ms_flow` o de `reads=`. `after=` añade dependencias a mano.

**Resultados.** Una sola puerta: lo que devuelve la task va a `output`. `finalize` corre una vez al terminar.

Una puerta no es una tabla: `output` puede ser un `graph_sink` (ya existe; AMDockVS lo usa en import, química y descriptores). La task devuelve `{"tabla_a": filas, "tabla_b": filas}`, las hijas llevan una referencia a su fila madre y el sink pone la clave ajena al insertar, todo en una transacción. Una lista vacía en un nodo no escribe nada, así que un paso opcional es un `if` en la task.

Caso que lo fija (decidido por Mario el 2026-10-08): las interacciones viven en `interaction_results`, y el docking con la opción marcada devuelve filas para `docking_results` e `interaction_results` desde una sola task. No son dos tasks encadenadas: la fila de interacción necesita el id de la pose, que no existe hasta que el hilo del job la escribe; con una task lo resuelve el sink, con dos haría falta escribir, devolver el id y mandar la segunda al mismo sitio donde está el fichero de la pose.

Medido en una pose (1iep, Vina): detectar interacciones 0,04 s; lo que encarece el diagrama es lo que solo sirve para dibujar: exposición al solvente 3,0 s, layout 0,15 s, imagen 1,8 s (intérprete nuevo). Por eso lo barato y que es dato va dentro de la task de docking, y la imagen sigue en su job aparte, que espera al docking por tabla.

**Datos y base de datos.** La base de datos solo se toca en el proceso que lanza el job. El hilo del job lee la fuente y arma el elemento con datos planos y rutas; el hilo de entrega escribe en `output` lo que devuelve la task. Un worker nunca recibe una conexión ni la ruta de la base: recibe datos y rutas de ficheros, y devuelve filas.

**Entradas con ficheros.** Un `FileInputSpec` en un elemento lo resuelve el ejecutor (en local, la ruta tal cual; en remoto, copiar el fichero, una vez si varios elementos lo comparten). El motor llama a esa función desde el hilo del job, nunca desde el hilo de eventos: `submit` de un ejecutor no puede bloquear.

**Pasos previos (`setup`).** Hoy un job ejecuta una sola task; `setup`, `stage_chunk` y `finalize` son funciones sueltas que corren en el proceso que lanza, en un pool aparte, sin recursos declarados ni reintentos. El motor nuevo también lleva una función por job. Propuesta: un job sigue siendo una task aplicada a muchos elementos, y además puede listar pasos que corren una vez, en orden, antes del primer elemento:

```python
@task(local=True, timeout=600)
def upload_inputs(params): ...      # runs where the job is submitted

@task(timeout=60)
def check_env(params): ...          # runs on the job's executor


@job(task=dock, setup=[upload_inputs, check_env], ...)
def docking_job(params, project, setup): ...
```

- Cada paso es una task normal: declara recursos, tiempo máximo y reintentos, y tiene log y progreso. `local=True` lo fija al proceso que lanza; sin él corre en el ejecutor del job.
- Si un paso falla, el job falla sin leer la fuente. El job muestra la fase en que está (preparando, ejecutando, finalizando).
- Lo que devuelven los pasos lo recibe la fuente.
- Al reanudar se repiten, así que deben poder repetirse sin daño (subir solo lo que falta).
- En el motor es lo que ya hace `after=`: la fuente no se lee hasta que terminan y el job falla si uno falla. Se construye con el primer ejecutor remoto; en local nadie lo usa.
- Preparar el entorno queda fuera (se hace al configurar el ejecutor, en `ms_env`); comprobarlo sí puede ser un paso.
- Los ficheros de cada elemento no necesitan paso: los cubre la regla anterior.

**Registro de lo hecho.** El journal sigue registrando cada elemento hecho cuando el job se envía con `journal=` y `key=`. Quien invoca decide qué pide (por ejemplo, excluir lo ya calculado); `ms_flow` sabe por sí mismo qué tiene activo y qué tiene registrado. El total al reanudar usa lo registrado.

## Qué desaparece

- La clase `JobSpec`, las plantillas `streaming_job`, `batch_job` y `build_streaming_job_definition`, y `molsuite.task` / `molsuite.job`.
- De la declaración: `required`, `produces`, `setup` como función suelta, `stage_chunk`, `stage_fail_policy`, `max_stage_failures`, `store_results`, `result_handler_factory`, `cpu_required` (pasa a `cpu`, `gpu`, `mem_mb`) y los pares función / `*_ref` (la referencia se deriva).
- `InputSource.iter_chunks` y `batch_size`; queda `iter_items`.
- El saco `config` con objetos vivos: la fuente recibe el proyecto; la task, solo parámetros y rutas.
- En AMDockVS, más adelante: las reglas por nombre de `runtime.py` y los `progress_cb`.

## Abierto

1. Confirmar los pasos previos como lista de tasks (`setup=[...]`).
2. Tamaño de lote en HPC: Mario tiene detalles que contar cuando se configure.
3. La capa de workflows (`workflow()`, `WorkflowSpec`) queda fuera de este documento.

## Para más adelante: task compuesta

Pregunta de Mario (2026-10-08): encadenar varias tasks por elemento (preparar y hacer docking molécula a molécula) para ver resultados desde el principio, en lugar de esperar a que termine cada etapa.

- En `ms_flow` no existe. `workflow()` es, pese al nombre, una entrada, un proceso y una salida: un job con una task. El `WorkflowRunner` de AMDockVS encadena jobs enteros, etapa tras etapa.
- Sí existe escrito a mano dentro de una task: la de química acepta una lista de operaciones por molécula (protonar, 3D, minimizar) y el docking dibuja el diagrama en el mismo worker (`compute_diagram`).
- En el diseño nuevo no pide un tipo de job distinto: una task compuesta es una función que llama a otras. Lo que `ms_flow` podría dar es un `chain(a, b)` que haga una task de varias: recursos = el máximo, tiempo límite = la suma, progreso = paso k de n.
- Costes: la compuesta retiene los recursos del paso más caro durante todo el elemento; un reintento repite la cadena entera; lo intermedio no se guarda salvo que cada paso tenga su destino (un ligando preparado sirve para muchos receptores).
- Es la única vía para ver resultados pronto: con el tope de `max(id)` un job no ve lo que otro escribe mientras corre.
- No se construye ahora (decidido por Mario). El caso que hay hoy, docking con interacciones opcionales, no la necesita: es un `if` dentro de la task de docking con salida a dos tablas (ver Resultados).

## Hecho en el motor a raíz de este documento

- `refactor/engine-pack-target`: objetivo del lote automático de 100 ms y reparto del final del job.
- `refactor/engine-resume-count`: sin aviso al reanudar; lo hecho cuenta aunque la fuente no lo traiga.
