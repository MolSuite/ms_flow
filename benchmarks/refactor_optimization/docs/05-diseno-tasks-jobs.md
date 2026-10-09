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

1. Tamaño de lote en HPC: Mario tiene detalles que contar cuando se configure.
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
- **Total que cambia.** `total=` se fija al enviar; falta poder corregirlo durante la ejecución.
- **Progreso.** El fichero de ranuras está probado suelto (carrera y coste), no unido al motor ni a los workers.
- **Monitor.** Lee el `executor.db` del motor viejo; `JobSnapshot` no lleva fase, progreso fino ni ritmo.

Sin probar por decisión (van con el ejecutor remoto): pasos `setup`, resolución de `FileInputSpec` en el hilo del job, progreso por lote en HPC.

## Hecho en el motor a raíz de este documento

- `refactor/engine-pack-target`: objetivo del lote automático de 100 ms y reparto del final del job.
- `refactor/engine-resume-count`: sin aviso al reanudar; lo hecho cuenta aunque la fuente no lo traiga.
- `refactor/engine-grouped-delivery`: escritura agrupada. `on_results(pares)` recibe de una vez todos los resultados del job que están esperando (hasta 256) y se dan por hechos cuando vuelve. Si falla, se ofrecen de uno en uno y solo fallan los malos, así que tiene que ser todo o nada (una transacción). Guardar una fila por resultado cuesta 0,004 ms por elemento, frente a 0,02-0,03 ms con una transacción por resultado. Límite: resultados de dos jobs que llegan intercalados salen de uno en uno.
- `refactor/engine-forced-batch`: `batch=N` fija cuántos elementos van por tarea (la tarea espera a tener N; solo la última lleva menos) y `batched=True` hace que la función reciba la lista y devuelva un resultado por elemento. Si una función por lotes falla, falla su lote entero. Solo en ejecutores que agrupan (el de procesos); el de hilos lo rechaza. Medida en el informe pendiente: la máquina estaba ocupada al terminar.
