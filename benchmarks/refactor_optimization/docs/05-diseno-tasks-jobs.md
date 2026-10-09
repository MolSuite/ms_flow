# 05 — Diseño de tasks y jobs

Estado: acordado con Mario el 2026-10-08. Nada de esto está construido salvo lo que lista la última sección.

Sale de la auditoría (`04-auditoria-tasks-jobs.md`) y de sus cuatro rondas de discusión.

## Piezas

| Pieza | Qué es | Quién la escribe |
|---|---|---|
| Task | La función sobre **un** elemento, y lo que necesita: CPU, GPU, memoria, reintentos | El autor del job |
| Fuente | Un generador que da los elementos uno a uno | El autor, o una de `ms_flow` |
| Job | Task + fuente + destino de los resultados | El autor |
| Envío | Parámetros, ejecutor, dependencias extra | Quien lanza |
| Lote (chunk) | Cuántos elementos viajan juntos a un worker | El ejecutor, con ajustes |

## Cómo se escribe

```python
from ms_flow import job, report, table_sink, task


@task(cpu=1, mem_mb=500, retries=1)
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

Medido en una pose (1iep, Vina): detectar interacciones 0,04 s; lo que encarece el diagrama es lo que solo sirve para dibujar: exposición al solvente 3,0 s, layout 0,15 s, imagen 1,8 s (intérprete nuevo). Por eso lo barato y que es dato va dentro de la task de docking. La imagen no se genera en el docking (decidido por Mario): el diagrama se dibuja en la UI a partir de las interacciones. El cambio en AMDockVS queda anotado en su `docs/TODO.md` y espera a que el motor esté cerrado.

**Datos y base de datos.** La base de datos solo se toca en el proceso que lanza el job. El hilo del job lee la fuente y arma el elemento con datos planos y rutas; el hilo de entrega escribe en `output` lo que devuelve la task. Un worker nunca recibe una conexión ni la ruta de la base: recibe datos y rutas de ficheros, y devuelve filas.

**Entradas con ficheros.** Un `FileInputSpec` en un elemento lo resuelve el ejecutor (en local, la ruta tal cual; en remoto, copiar el fichero, una vez si varios elementos lo comparten). El motor llama a esa función desde el hilo del job, nunca desde el hilo de eventos: `submit` de un ejecutor no puede bloquear.

**Pasos previos (`setup`).** Hoy un job ejecuta una sola task; `setup`, `stage_chunk` y `finalize` son funciones sueltas que corren en el proceso que lanza, en un pool aparte, sin recursos declarados ni reintentos. El motor nuevo también lleva una función por job. Acordado con Mario el 2026-10-08: un job sigue siendo una task aplicada a muchos elementos, y además puede listar pasos que corren una vez, en orden, antes del primer elemento:

```python
@task(local=True)
def upload_inputs(params): ...      # runs where the job is submitted

@task
def check_env(params): ...          # runs on the job's executor


@job(task=dock, setup=[upload_inputs, check_env], ...)
def docking_job(params, project, setup): ...
```

- Cada paso es una task normal: declara recursos y reintentos, y tiene log y progreso. `local=True` lo fija al proceso que lanza; sin él corre en el ejecutor del job.
- Si un paso falla, el job falla sin leer la fuente. El job muestra la fase en que está (preparando, ejecutando, finalizando).
- Lo que devuelven los pasos lo recibe la fuente.
- Al reanudar se repiten, así que deben poder repetirse sin daño (subir solo lo que falta).
- En el motor es lo que ya hace `after=`: la fuente no se lee hasta que terminan y el job falla si uno falla. Se construye con el primer ejecutor remoto; en local nadie lo usa.
- Preparar el entorno queda fuera (se hace al configurar el ejecutor, en `ms_env`); comprobarlo sí puede ser un paso.
- Los ficheros de cada elemento no necesitan paso: los cubre la regla anterior.
- `finalize` se trata igual: es un paso del job, al final, y se ve como tal.

En el monitor los pasos son parte del job, no jobs aparte (vista acordada, sin construir):

```
Job                 Status          Executor  Progress  Chunks
amdock_docking_job  preparing 1/2   cluster   —         0/12000

    step   upload_inputs   running   62 %   00:41
    step   check_env       pending
```

- `Status` lleva la fase: `preparing 1/2`, `running`, `finalizing`.
- Los pasos son filas del panel de detalle, con nombre, estado, tiempo y progreso si llaman a `report()`; los previos van antes de los lotes y `finalize` después.
- Un paso que falla sale en Errors con su traceback, como un lote.
- La barra cuenta solo elementos; durante la preparación queda vacía.

**Registro de lo hecho.** El journal sigue registrando cada elemento hecho cuando el job se envía con `journal=` y `key=`. Quien invoca decide qué pide (por ejemplo, excluir lo ya calculado); `ms_flow` sabe por sí mismo qué tiene activo y qué tiene registrado. El total al reanudar usa lo registrado.

## Qué desaparece

- La clase `JobSpec`, las plantillas `streaming_job`, `batch_job` y `build_streaming_job_definition`, y `molsuite.task` / `molsuite.job`.
- De la declaración: `required`, `produces`, `setup` como función suelta, `stage_chunk`, `stage_fail_policy`, `max_stage_failures`, `store_results`, `result_handler_factory`, `cpu_required` (pasa a `cpu`, `gpu`, `mem_mb`) y los pares función / `*_ref` (la referencia se deriva).
- `InputSource.iter_chunks` y `batch_size`; queda `iter_items`.
- El saco `config` con objetos vivos: la fuente recibe el proyecto; la task, solo parámetros y rutas.
- En AMDockVS, más adelante: las reglas por nombre de `runtime.py` y los `progress_cb`.

## Abierto

1. Tamaño de lote en HPC: Mario dio los detalles el 2026-10-09 (Ray dentro del job del HPC, runner que es parte de `ms_flow`). La propuesta y sus decisiones abiertas están en [06-diseno-ejecutor-remoto.md](06-diseno-ejecutor-remoto.md).
2. La capa de workflows (`workflow()`, `WorkflowSpec`) queda fuera de este documento.

## Para más adelante: parar por tiempo

Decidido por Mario el 2026-10-08: una task no declara tiempo máximo por ahora. Irá junto con el pool de procesos propio, que es quien puede parar una sola task; el pool actual, para parar una, para todas las suyas y las reinicia.

El motor ya acepta `timeout` en `submit` (paso "retries"). Se queda como está y la API de tasks no lo pasa.

## Para más adelante: task compuesta

Pregunta de Mario (2026-10-08): encadenar varias tasks por elemento (preparar y hacer docking molécula a molécula) para ver resultados desde el principio, en lugar de esperar a que termine cada etapa.

- En `ms_flow` no existe. `workflow()` es, pese al nombre, una entrada, un proceso y una salida: un job con una task. El `WorkflowRunner` de AMDockVS encadena jobs enteros, etapa tras etapa.
- Sí existe escrito a mano dentro de una task: la de química acepta una lista de operaciones por molécula (protonar, 3D, minimizar) y el docking dibuja el diagrama en el mismo worker (`compute_diagram`).
- En el diseño nuevo no pide un tipo de job distinto: una task compuesta es una función que llama a otras. Lo que `ms_flow` podría dar es un `chain(a, b)` que haga una task de varias: recursos = el máximo, progreso = paso k de n.
- Costes: la compuesta retiene los recursos del paso más caro durante todo el elemento; un reintento repite la cadena entera; lo intermedio no se guarda salvo que cada paso tenga su destino (un ligando preparado sirve para muchos receptores).
- Es la única vía para ver resultados pronto: con el tope de `max(id)` un job no ve lo que otro escribe mientras corre.
- No se construye ahora (decidido por Mario). El caso que hay hoy, docking con interacciones opcionales, no la necesita: es un `if` dentro de la task de docking con salida a dos tablas (ver Resultados).

## Comprobado y por comprobar (2026-10-08)

Prueba de usar y tirar (`/tmp/api_spike/spike.py`, no está en el repo): decoradores de 60 líneas sobre el motor nuevo tal como está, pool de procesos, proyecto SQLite real, task sin trabajo (solo mide el camino).

Funciona sin tocar el motor:

- `@task` + `@job` con una fuente generadora que da elementos de uno en uno: 600 elementos, 0,43 s.
- Parámetros como modelo pydantic hasta el worker.
- Una task, dos tablas por `graph_sink`, escritas desde el hilo de entrega: 1800 poses y 3600 interacciones, ninguna con la clave ajena mal ni huérfana; con la opción sin marcar, 0 interacciones.
- Dependencia por tabla resuelta a `after=`: el job que lee `docking_results` no leyó su fuente hasta que el docking tenía las 1800 poses.
- Tope: 200 ligandos insertados durante la ejecución no entraron (600 de 800).

Hace falta trabajo, ya medido o localizado:

- **Escritura agrupada (hecho, ver abajo).** Una escritura por elemento cuesta 0,39 ms y con tasks rápidas es todo el tiempo del job (0,31 de 0,32 s en 800 elementos); agrupando 64 baja a 0,06 s. El motor da un elemento por hecho cuando vuelve `on_result`, así que agrupar fuera del motor dejaría elementos registrados como hechos sin estar en disco. Tiene que agrupar el motor y dar el lote por hecho al escribirlo.
- **Lote forzado y task por lotes (hecho, ver abajo).** El motor solo empaqueta por su medida; faltan `batch=` y `@task(batched=True)`.
- **Total que cambia (hecho, ver abajo).** `total=` se fija al enviar; falta poder corregirlo durante la ejecución.
- **Progreso.** Hecho en el ejecutor de procesos (ver «Hecho: progreso»). Falta el de HPC, por lote.
- **Monitor.** Sin datos desde el borrado del motor viejo (ver «Hecho: borrado del motor anterior»); `JobSnapshot` no lleva fase ni ritmo.

Sin probar por decisión (van con el ejecutor remoto): pasos `setup`, resolución de `FileInputSpec` en el hilo del job, progreso por lote en HPC.

## Hecho en el motor a raíz de este documento

- `refactor/engine-pack-target`: objetivo del lote automático de 100 ms y reparto del final del job.
- `refactor/engine-resume-count`: sin aviso al reanudar; lo hecho cuenta aunque la fuente no lo traiga.
- `refactor/engine-grouped-delivery`: escritura agrupada. `on_results(pares)` recibe de una vez todos los resultados del job que están esperando (hasta 256) y se dan por hechos cuando vuelve. Si falla, se ofrecen de uno en uno y solo fallan los malos, así que tiene que ser todo o nada (una transacción). Guardar una fila por resultado cuesta 0,004 ms por elemento, frente a 0,02-0,03 ms con una transacción por resultado. Límite: resultados de dos jobs que llegan intercalados salen de uno en uno.
- `refactor/engine-forced-batch`: `batch=N` fija cuántos elementos van por tarea (la tarea espera a tener N; solo la última lleva menos) y `batched=True` hace que la función reciba la lista y devuelva un resultado por elemento. Si una función por lotes falla, falla su lote entero. Solo en ejecutores que agrupan (el de procesos); el de hilos lo rechaza. Registrado en el informe el 2026-10-09 (paso "forced batch"): sin cambio en las medidas largas.
- `refactor/engine-stopped-results`: un resultado que ya iba camino del callback cuando el job se paró (cancelar, cerrar el proyecto, cerrar el motor) se escribía pero no quedaba registrado como hecho, y al reanudar se repetía. Ahora queda registrado; `detach` y `close` esperan a esa anotación antes de cerrar el diario. Salió al probar la fachada: de 14 a 23 filas duplicadas al cerrar un proyecto con 14 hilos. Tras un `kill -9` se sigue repitiendo lo último sin registrar.

## Hecho en la API (2026-10-08)

Rama `refactor/task-job-api`: `task`, `job` y `Flow` en `ms_flow.engine` (`engine/flow.py`, 112 líneas), junto al motor nuevo. Los nombres `task` y `job` de `ms_flow` siguen siendo los viejos hasta que la fachada cambie de motor.

```python
from ms_flow.engine import Flow, job, task

flow = Flow(engine, project)   # project: lo que recibe la fuente y donde escriben los sinks
job_id = flow.submit(docking_job, {"exhaustiveness": 8}, executor="process", batch=None, after=())
```

Lo que hay:

- `@task(cpu, gpu, mem_mb, retries, batch, batched)` deja la función como estaba, con sus necesidades anotadas: se puede llamar a mano y viaja a los workers por su nombre.
- La task recibe `params` solo si tiene un parámetro con ese nombre. Viajan con la función, una vez por lote.
- `params_model` valida al enviar: un parámetro mal puesto falla en `submit`, no en un worker.
- `output` es `table_sink` (la task devuelve filas) o `graph_sink` (devuelve `{nodo: filas}`). Se escribe en el hilo de entrega, una transacción por grupo de resultados; un elemento que no devuelve nada no escribe.
- Las referencias (`$ref`) de un `graph_sink` son locales a lo que devuelve un elemento: al escribir varios juntos, cada una lleva delante el puesto de su elemento en el grupo. Excepción: si el campo de la referencia es también una columna de la tabla, es dato y no se toca; ahí tiene que ser única en el job, como hoy.
- Encontrado al probarlo: `insert_graph` no detecta dos filas con la misma referencia en una escritura (la comprobación mira un mapa que se rellena después de recorrer las filas) y se queda con la última, así que las filas hijas de la primera acaban colgadas de la otra, sin error. Con el motor viejo pasa igual cuando agrupa varios chunks (`flush_every` > 1). Arreglado (decidido por Mario, rama `refactor/graph-duplicate-refs`): el store rechaza la escritura entera si un nodo cuyas referencias resuelve alguna relación trae dos filas con la misma. Los nodos `upsert` quedan fuera: repiten referencia a propósito.
- Lote: `batch=` del envío, y si no, el de `@task`. Falta el nivel del ejecutor.
- Dependencias: el job espera a los jobs sin terminar, enviados por el mismo `Flow`, que escriben una tabla de su `reads=`. Si uno de ellos falla o se cancela, el que espera falla (regla de `after=` del motor).
- Lo demás de `Engine.submit` (`key`, `journal`, `priority`, `total`) pasa tal cual.

Lo que no hay todavía: `file_sink` como destino (ningún job de AMDockVS lo usa), `source=` con objetos de `ms_flow`, `local=True`, pasos `setup` y `finalize`, `report()`.

Medido con la máquina ocupada (200 000 elementos, una fila por elemento): 0,017 ms por elemento con el sink del proyecto frente a 0,0155 ms escribiendo con `sqlite3` a mano. Medida nueva en el informe: `api_long_ms`. Registrado el 2026-10-09 con la máquina libre (paso "task api"): 0,021 ms por elemento, frente a 0,017 ms de `write_long_ms`.

## Hecho: total y tope (2026-10-08)

Rama `refactor/source-total-and-cap`.

- **Motor.** Una fuente puede traer dos métodos, los dos opcionales. `remaining()`: cuántos elementos le quedan; con él el job tiene total mientras corre (contado en el primer elemento y cada 30 s, exacto al agotarse la fuente). `open(state)`: se llama antes de leer, y lo que devuelve se guarda con el job en el diario y vuelve como `state` al reanudar. Los dos corren en el hilo que lee la fuente. El diario pasa a la versión 2 (columna `source`).
- **Fuente de tabla.** `ms_flow.query.Rows(project, QuerySpec(table=...))`: páginas por clave hasta el `max(id)` que había al empezar a leer, que es lo que guarda. La función del job la devuelve en lugar de hacer `yield`, y de ella sale también la tabla que lee, sin `reads=`:

```python
@job(task=dock, output=table_sink("docking_results", columns=(...)))
def docking(params, project):
    return Rows(project, QuerySpec(table="ligands", fields=("id", "smiles"), filters=params.filters))
```

- Comprobado en `tests/test_engine_flow.py`: 400 filas, el total baja a 390 al borrar 10 sin leer, se cancela, llegan 10 filas nuevas, se reanuda y termina con 390 hechas y ninguna de las nuevas; un job nuevo sí las toma.
- El total antes de empezar a leer no existe (el job puede estar esperando a otro que escribe esa tabla).
- Al reanudar, si la fuente vuelve a traer lo ya hecho en otro orden, el total sale alto hasta el siguiente recuento.
- Límite conocido: el tope es `max(id)`. Si se borran las últimas filas de una tabla sin `AUTOINCREMENT`, SQLite vuelve a dar esas claves y las filas que las tomen entran.
- Sin hacer: `source=` en `@job` para no escribir la función; se puede devolver la fuente desde ella, que además deja usar los parámetros en el filtro.

## Hecho: progreso (2026-10-08)

Rama `refactor/job-progress`.

- **`JobSnapshot.progress`**: de 0 a 1, (elementos hechos + fallidos + lo que llevan los que corren) / total. `None` mientras no hay total.
- **Casillas.** `ProcessExecutor` crea un fichero temporal de 1024 números, uno por task vigilada, y los workers lo mapean. El worker escribe, el motor lee al pedir el estado. Nada pasa por colas. Con las 1024 ocupadas, la task siguiente corre sin vigilar.
- **Lote de varios elementos**: el worker apunta cada elemento que termina. Siempre vigilado, la función no hace nada.
- **Lote de uno**: la task llama a `ms_flow.engine.report(fraccion)`. Solo se vigila si el job tiene como mucho dos elementos por worker, o si se envía con `progress=True`. Sin vigilar, `report` no hace nada.
- **Función que recibe el lote entero** (`batched=True`): `report(fraccion)` es la fracción del lote.
- **Ejecutor de hilos**: sin fracción, cuenta elementos terminados.
- Contrato del ejecutor: `progress(owner)` opcional; con él, `submit` y `submit_many` aceptan `owner=`.
- Comprobado en `tests/test_engine_core.py`: 2 elementos a la mitad dan 0,5; dos lotes de 4 con tres terminados y uno a la mitad dan 0,875 sin ningún elemento entregado; 6 elementos sin vigilar dan 0 y con `progress=True` dan 1/6.
- Límite conocido: una task que termina deja de contar en su casilla un instante antes de que sus elementos cuenten como hechos (cuentan al entregarse), así que el valor puede retroceder un poco. En Windows el fichero temporal se queda al cerrar.
- Sin hacer: progreso por lote en HPC (va con el ejecutor remoto), y el monitor, que todavía lee el motor viejo.
- Coste registrado el 2026-10-09 (paso "table sources"): igual que el paso anterior. Lo añadido por elemento en un lote es una escritura en memoria (217 ns medidos sueltos).

## Hecho: relevo, primer corte (2026-10-08)

Rama `refactor/facade-new-engine`. La fachada tiene el motor nuevo al lado del viejo; nada de lo viejo cambia. (El motor viejo se borró después: ver «Hecho: borrado del motor anterior».)

```python
job_id = molsuite.submit(docking_job, {"exhaustiveness": 8})   # executor=, batch=, after=, progress=, key=
molsuite.engine.wait(job_id)                                   # status, cancel y jobs son del motor
```

- `MolSuite.engine`: se crea al primer uso con las CPU y GPU de los ajustes, ejecutores `process` y `thread`, y el bloqueo de un motor por usuario (`~/.molsuite/engine.lock`). Un segundo proceso recibe `Busy` al enviar, no al arrancar.
- `MolSuite.submit`: un `Flow` por proyecto activo (así valen las dependencias por tabla entre sus jobs). Todo job va al diario del proyecto.
- Cerrar o cambiar de proyecto interrumpe sus jobs, espera a que lo entregado esté escrito y cierra el diario. Enviar otra vez con la misma `key` lo continúa. `shutdown` cierra el motor.
- Comprobado en `tests/test_engine_flow.py`: 300 ligandos de una tabla por el ejecutor de procesos; un segundo job cortado al cerrar el proyecto y continuado al reabrirlo, sin filas repetidas.

Las tres decisiones que quedaron a revisar (`engine.db`, dos motores a la vez, nombres solo en `ms_flow.engine`) se cerraron al borrar el motor viejo.

## Hecho: borrado del motor anterior (2026-10-08)

Rama `refactor/delete-old-engine`. Decidido por Mario: se borra ya, el diario es el `executor.db` del proyecto y AMDockVS queda roto hasta que se reescriban sus jobs.

Borrado (unas 14.000 líneas de `src` y 33 ficheros de test):

- `core/executor` salvo `tools.py` y `provisioning.py`: manager, servicios, adaptadores local, HPC y Ray, despacho, handlers de resultados, monitorización.
- `core/database/executor.py` y `executor_models.py` (el `executor.db` con SQLModel).
- `tasking.py`, `job_templates.py`, `specs/workflow.py`, `specs/processor.py`, `progress.py`, `core/callable_refs.py`.
- De la fachada: `run`, `submit_job`, `molsuite.task`, `molsuite.job`, `wait_for_job`, `get_executor_job`, `get_job_chunks`, eventos y salidas por job, `cancel_job`, `resubmit_job`, estado y matriz de ejecutores, `activate_compute_backend`, registro de HPC y Ray, `get_runtime_healthcheck`, `list_executor_jobs`, `cancel_executor_jobs`, `get_project_switch_status`, `can_switch_project`, `executor_db`, `executor_manager`.
- Dependencia `tqdm` y el extra `ray`.

Lo que hay ahora:

- `MolSuite.submit(job, params, **opciones)` y `MolSuite.engine` son la única forma de ejecutar. `status`, `wait`, `cancel`, `jobs` y `recorded` son del motor.
- El diario es `<proyecto>/executor.db`, abierto al primer `submit`. Un fichero de otra versión da error con el mensaje «is not a version 3 journal: move it away to start a new one». No se migra (decisión cerrada en el documento 02).
- `close_project` interrumpe los jobs del proyecto (no los cancela); con `cancel_running_tasks=False` falla si queda alguno. `get_project_activity` sale de `engine.jobs()`.
- `from ms_flow import MolSuite, job, report, task, table_sink, graph_sink, file_sink`, cargados al primer uso. `ms_flow.api` exporta también `JobSnapshot`.
- `core/executor` queda con `tools.py` y `provisioning.py`, que no son del motor. Su sitio final es `ms_env`.

Se fue con el motor viejo. Estas piezas no eran inútiles: vivían dentro del handler de resultados, de las definiciones de job y de las tablas del motor viejo, así que no se podían conservar sueltas; se rehacen sobre el nuevo según hacen falta. Estado a 2026-10-09 (rama `feat/job-writers-and-finish`):

1. **Sink de fichero y writer propio: hechos.** `output=` acepta además un `file_sink` (`append`: un fichero que crece; `per_batch`: un fichero numerado por grupo de resultados; ninguno: un fichero escrito al completar) y una función `(params, project)` que devuelve el writer del job: `write(resultados)`, y opcionales `finish()` y `tables`. Es lo que sustituye a `result_handler_factory` (los tres writers de shards de AMDockVS).
2. **Jobs que esperan a una instalación** (`depends_on=["tool:<nombre>@<destino>"]`). `tools.install_tool` sigue devolviendo la clave y `get_install` el future; el motor nuevo solo espera a otros jobs (`after=`).
3. **Ejecutores remotos** (HPC, Ray) y su registro desde los ajustes. Van con el ejecutor remoto (paso 6).
4. **Final del job: hecho, en dos piezas.** `finish()` del writer (en el motor, `on_finish`) se llama una vez cuando todos los elementos han terminado, antes de que el job cuente como completado y de que arranquen los que esperan por él; si lanza, el job falla. `@job(finalize=fn)` recibe `(params, project, snapshot)` cuando el job ha terminado, acabe como acabe: lo que hay que limpiar si falló o se canceló. **Pendientes:** `setup` (con el ejecutor remoto), `workflow()` y las entradas declaradas (`InputSource`, `FileInputSpec`): `specs/input.py`, `specs/output.py` y `core/data` siguen en el paquete, sin nadie del motor que los use.
5. **`watch_job` y `watch_jobs`: hechos** (`ms_flow.progress`, una barra de `tqdm` por job sobre `engine.status`). **Log por job: pendiente**, se diseña con el ejecutor remoto (documento 06), que es quien necesita traerlo de otra máquina.
6. **Monitor** (`ms_components`): llamaba a 14 métodos de la fachada que ya no existen. Lo que lee ahora: `MolSuite.jobs(limit)` (los jobs del proyecto, del diario; cada uno como lo tiene el motor en memoria si lo corrió en esta sesión) y `MolSuite.job_failures(key)` (intento y traza de los chunks fallados). El estado vivo sale de la memoria del motor; el fichero solo da el historial y los errores, y se lee sin tomar su cerrojo, así que también sirve para mirar un proyecto que corre otro proceso. El diario no guarda el payload; de un chunk que falla guarda una etiqueta corta de su entrada (`reprlib`, 80 caracteres por valor), que `job_failures` devuelve en `input` y el monitor muestra junto a la traza. No la hay cuando el chunk corrió y lo que falló fue escribir su resultado.

Restos, revisados el 2026-10-09 (rama `refactor/settings-follow-the-engine`), buscando quién lee cada ajuste en `ms_flow`, `ms_components` y AMDockVS:

| Resto | Qué se hizo |
|---|---|
| Ajustes `operational_limits` (14 opciones del motor viejo: spool, payload en línea, buffers del sink) | **Borrados.** Nadie los leía. Un `config.toml` que los tenga los pierde con un aviso (`_LEGACY_KEYS`) |
| `resources.local.max_processes`, `max_threads`, `memory_gb` | **Conectados.** Solo se mostraban en la UI; ahora son el tamaño del pool de procesos, el del de hilos y el presupuesto de memoria del motor (sin valor: RAM física menos una reserva) |
| `resources.workers` (definiciones `process_pool`, `thread`, `ray`, `hpc`) y su editor en `ms_components` | **Se queda sin leer.** Es donde se definirán los ejecutores remotos (documento 06); se rehace entonces con los campos que ese diseño fije |
| `general.poll_interval`, `database.connection_timeout` | **Pendiente de borrar.** Nadie los lee (el motor no sondea); `poll_interval` tiene un control en el navegador de proyectos, así que se quita junto con la revisión de `ms_components` |
| Tabla `project_job_index` de `projects.db` | **Se queda de momento.** Nadie la escribe ni la lee, pero `~/.molsuite/projects.db` es el mismo fichero que usa `main`: borrarla pide subir la versión del esquema, y eso se hace al integrar, no en una rama local |
| Benchmarks sueltos de `benchmarks/*.py` | Importan el manager viejo y no están en git. Son ficheros de Mario: no se tocan |
| Documentación de usuario de `docs/` | Describe la API vieja (incluida `reference/operational_profiles.md`, de algo que ya no existe). Se reescribe cuando todo funcione, según lo acordado; solo el README está al día |

