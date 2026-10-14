# ms_flow — Informe de auditoría: hallazgos, plan de acciones y tareas

2026-10-07

## Resumen ejecutivo

ms_flow tiene una base funcional sólida y bien probada, pero todavía no se puede publicar como "eficiente y estable bajo carga". Los dos bloqueos son la robustez ante fallos y un coste por chunk que crece con el tamaño del job. Ambos tienen arreglo conocido y acotado.

| Área | Hallazgo principal | Medida |
| --- | --- | --- |
| Robustez | La muerte de un worker deja el pool inservible: todos los jobs posteriores fallan | 100 % de fallos en 0,12 s hasta reiniciar la aplicación |
| Robustez | No hay reanudación, reintentos ni tiempo máximo por chunk | Tras `kill -9`: 148 de 200 chunks sin ejecutar, 7 procesos huérfanos, reenvío imposible |
| Overhead | La base de datos es la fuente de verdad del motor | 4,7 a 6 ms por chunk, frente a 0,11 ms de loky directo |
| Escalabilidad | El coste por chunk crece con el tamaño del job | De 2,1 ms a 9,2 ms entre 2.700 y 40.000 chunks; 1 millón serían unas 27 h de pura orquestación |
| ORM | SQLModel multiplica por 4,4 el coste de escritura frente a `sqlite3` directo | 738 frente a 166 µs por chunk |
| Persistencia | Una base en memoria apenas ayuda; lo caro no es el disco | 9 % más rápida |
| HPC | Un trabajo de clúster por chunk, sin ssh ni arrays | No utilizable desde un PC sin disco compartido |
| Estructura | La capa de aplicación de MolSuite vive dentro del núcleo | 23.880 líneas; un fichero de 1.904; `submit_job` con 35 parámetros |

**Qué hacer.** Mantener SQLite y el fichero único, que son la ventaja. Cambiar quién manda: estado del motor en memoria y la base de datos como diario diferido, escrito con `sqlite3` directo. Eso da la persistencia opcional, baja el coste a 0,2 ms por chunk o menos y lo hace plano con el tamaño.

**Por dónde empezar.** Las fases 0 y 1 (secciones 10 y 11): reinicio del pool, índice compuesto, bloqueo por fichero, vigilancia de huérfanos, reintentos y tiempos máximos. Son días o pocas semanas sobre el motor actual y eliminan los fallos que hoy pueden perder una campaña.

**Posición.** El nicho es real: campañas masivas reanudables desde un PC, sin servidor, con el estado en un fichero. Parsl y submitit son los vecinos más cercanos y ninguno ofrece esa combinación. Para ocuparlo hace falta que "reanudable" sea cierto.

Todas las cifras se midieron en esta máquina (16 CPU lógicas) con tareas vacías, salvo donde se indica.

## 1. Overhead

Cada chunk cuesta 4,7 ms de orquestación en el executor de hilos y 5,2–6 ms en el de procesos, frente a 0,013 ms de un `ThreadPool` y 0,11 ms de loky directo. Casi todo ese tiempo es SQL ejecutado por el hilo del motor, no trabajo útil.

Medido en esta máquina (16 CPU lógicas, ms_flow configurado con 14 CPU, 28 hilos, 14 procesos), con tareas vacías.

### Costes fijos

| Operación | Tiempo |
| --- | --- |
| `import ms_flow.api` | 380 ms |
| `MolSuite()` | 36 ms |
| Crear proyecto / reabrir / cerrar | 26 / 10 / 8 ms |
| Primer job en `compute` (arranque de loky) | 950 ms |
| Primer job en `thread` | 120 ms |
| Llamada a `submit_job` | 2–4 ms |
| Job de 1 chunk vacío, de creado a terminado | 85 ms |
| `shutdown` | 480 ms |

El proyecto no es el problema: 26 ms una vez. El coste está en el motor, por chunk.

### Coste según la duración de la tarea (executor `compute`)

| Carga | ms_flow | loky directo | Factor |
| --- | --- | --- | --- |
| 200 tareas de 10 ms | 1,30 s | 0,16 s | 8,0× |
| 100 tareas de 100 ms | 1,24 s | 0,81 s | 1,5× |
| 40 tareas de 1 s | 3,24 s | 3,00 s | 1,08× |

Por debajo de ~100 ms por chunk el orquestador domina. Con lotes mejora, pero no desaparece: 50.000 ítems triviales en 500 chunks de 100 cuestan 59 µs por ítem, frente a 1,3 µs de `loky.map(chunksize=100)`.

### Dónde se va el tiempo

Perfil del hilo del motor en un job de 500 chunks vacíos (3,56 s en total). Los tiempos son acumulados y se solapan.

| Función | Acumulado | Nota |
| --- | --- | --- |
| `Session.execute` de SQLAlchemy | 1,38 s | De ellos, SQLite real: 0,14 s de ejecución y 0,17 s de commit |
| `refresh_job_status` | 1,09 s | 96 llamadas, 11 ms cada una |
| `finalize_dispatch_success` | 1,06 s | 2 sesiones, 2 selects y 2 commits por chunk |
| `feed_window` | 1,03 s | Incluye un refresh |
| `build_job_runtime_metrics` | 0,72 s | 8 agregados sobre todos los chunks del job |
| Flush de persistencia | 0,58 s | Ya va por lotes |
| Espera del tick | 0,56 s | 30 % en hilos, 2 % en procesos |
| `record_scheduler_reason` | 0,29 s | Escribe un motivo de diagnóstico en la DB |
| Flush de eventos | 0,24 s | 2 filas de evento por chunk |

En total salen entre 12 y 16 sentencias SQL y unos 3 commits por chunk. SQLite solo consume el 9 % del tiempo; el resto es la capa ORM y lecturas que no hacen falta.

### Causas

1. **La base de datos es la fuente de verdad del motor.** En cada tick se relee de SQLite lo que el motor ya sabe: los ids activos, los candidatos a despachar (filas ORM completas, con su payload) y la fila del job.
2. **Una transacción por chunk al despachar.** El paso de pendiente a en ejecución no se agrupa.
3. **El estado del job se recalcula con agregados.** Ocho consultas sobre todos los chunks, varias veces por tick. Crece con el tamaño del job (ver Escalabilidad).
4. **Telemetría persistida por defecto.** Dos eventos por chunk, latidos cada 0,5 s y el motivo del planificador.
5. **Bucle por sondeo.** El tick de 20–100 ms añade unos 50 ms al arranque de cada job y hasta un tick por finalización.
6. **Ventana por defecto menor que el pool.** `max_inflight_tasks=16` con 28 hilos o 14 procesos deja workers sin alimentar.

### Qué mueven los ajustes actuales

| Configuración | `thread` | `compute` |
| --- | --- | --- |
| Por defecto | 4,74 ms | 6,06 ms |
| `executor.db` en tmpfs | 4,53 ms | 5,90 ms |
| Tick de 5 ms | 3,53 ms | 5,92 ms |
| Ventana de 64 | 2,34 ms | 5,62 ms |
| Todo a la vez | 1,80 ms | 5,84 ms |

En `compute` ningún ajuste lo mueve: el hilo del motor está saturado de CPU, no esperando. La solución no es de configuración.

### Cómo eliminarlo

- Estado del motor en memoria como fuente de verdad; SQLite pasa a ser un diario que se escribe por detrás, una transacción por tick.
- Contadores del job incrementales en vez de consultas agregadas.
- Eventos por chunk desactivados por defecto; se guardan solo los fallos y los cambios de estado del job.
- Despertar el bucle por evento (finalización, envío) en lugar de por tick fijo.
- Ventana por defecto proporcional a los workers (2–4×).
- Agrupación automática de ítems cuando la tarea es corta.

Objetivo razonable: 0,1–0,2 ms por chunk con persistencia y menos de 0,05 ms sin ella. El microbenchmark de la sección del ORM muestra que SQLite lo permite.

## 2. Persistencia opcional y base de datos en memoria

Poner `executor.db` en memoria hoy solo ahorra un 9 %: 2,26 ms por chunk frente a 2,49 ms en un job de 10.000 chunks. La persistencia opcional merece la pena, pero como modo del motor, no como cambio de fichero.

| Dónde vive `executor.db` | ms por chunk | Diferencia |
| --- | --- | --- |
| Fichero en disco (WAL, `synchronous=NORMAL`) | 2,49 | — |
| Fichero en tmpfs | — | −4 % (medido en jobs de 500 chunks) |
| SQLite en memoria (`sqlite://` con una conexión compartida) | 2,26 | −9 % |

El medio de almacenamiento es una parte pequeña del coste. Mientras el motor haga 12–16 sentencias por chunk a través del ORM, da igual dónde se escriban.

### Por qué hoy no se puede apagar

- `submit_job` del motor lanza un error si no hay `executor_db` ("Open a project first").
- Los candidatos a despachar salen de un `SELECT` sobre chunks y jobs.
- La ventana de alimentación cuenta los chunks activos con otro `SELECT` por tick.
- El estado del job se deriva de agregados sobre la tabla de chunks.
- `store_results=False` solo evita guardar la salida; el resto del registro se escribe igual.

El motor usa la base de datos como su memoria de trabajo. Por eso la persistencia no es una capa que se pueda quitar.

### Propuesta: un motor, tres niveles de durabilidad

| Nivel | Qué se guarda | Qué sobrevive a una caída | Uso |
| --- | --- | --- | --- |
| `none` | Nada. Estado en memoria, resultados al llamador | Nada | `map` al estilo de `multiprocessing`, scripts, notebooks |
| `journal` (por defecto) | Jobs, estado final de cada chunk y errores, escritos por detrás en un lote por tick | Todo salvo las transiciones del último tick, que se reejecutan | Campañas |
| `full` | Lo anterior más eventos por chunk y métricas de tiempos | Igual, con traza completa | Depuración y auditoría |

La pieza que lo permite es una interfaz de diario con dos implementaciones, una nula y una SQLite. El motor guarda su estado en diccionarios y solo llama al diario para registrar.

Con ese diseño la base de datos en memoria deja de tener sentido como opción: `none` es más rápido que SQLite en memoria y `journal` en disco cuesta unos 0,1 ms por chunk.

### Atajo disponible ya

El motor funciona sin cambios sobre un SQLite en memoria; lo comprobé sustituyendo la fábrica del engine y completando 10.000 chunks. Sirve para ofrecer un modo "sin proyecto y sin ficheros" en días, pero no da velocidad.

Sobre el proyecto: el motor no lo necesita, solo necesita un `ExecutorStore`. Quien lo exige es la fachada `MolSuite` (`active_context`), porque del proyecto salen la ruta de `executor.db`, el `project.db` de los sinks, los recursos y el log.

## 3. Coste del ORM

El ORM cuesta 4,4 veces más que `sqlite3` directo en las tres transiciones básicas de un chunk: 738 µs frente a 166 µs. Para `executor.db` no compensa; para la capa de datos del proyecto, sí.

Microbenchmark con 8.000 chunks y ventana de 16, mismas tablas y mismos pragmas. Valores en µs por chunk.

| Variante | Insertar | Pendiente → en ejecución | En ejecución → completado | Total |
| --- | --- | --- | --- | --- |
| SQLModel ORM, fichero (lo de hoy) | 186 | 476 | 76 | 738 |
| SQLAlchemy Core, fichero | 69 | 219 | 17 | 305 |
| `sqlite3` directo, fichero | 76 | 78 | 13 | 166 |
| `sqlite3` directo, fichero, despacho agrupado por tick | 71 | 17 | 11 | 99 |
| `sqlite3` directo, en memoria | 9 | 7 | 4 | 20 |

La columna central es la que más pesa porque hoy lleva un commit por chunk. Agruparla por tick la baja de 78 a 17 µs sin tocar la durabilidad útil.

### Lectura

- Las tres transiciones con ORM suman 0,74 ms de los 4,7–6 ms por chunk. El ORM no es la causa principal del overhead; lo son las lecturas redundantes y los agregados. Pero es lo que impide bajar de 1 ms.
- `sqlalchemy` añade 98 ms al import, `pydantic` y `sqlmodel` otros 31 ms. El import completo de `ms_flow.api` son 372 ms.
- SQLModel sigue en versión 0.0.x. La 0.0.45 ya rompió los campos de fecha sin zona y obligó a tocar todos los modelos.
- La limpieza de historial ya reconoce el problema de volumen: una fila por unidad de trabajo son millones de filas en un cribado.

### Recomendación

1. **`executor.db` sin ORM.** Son 5 tablas de esquema fijo y un único motor de base de datos. Esquema como DDL en un módulo, versión con `PRAGMA user_version`, escrituras con `executemany` desde un único hilo escritor.
2. **Modelos como dataclasses o pydantic.** `JobSnapshot` ya es pydantic; es suficiente para tipar lo que se devuelve.
3. **Mantener SQLAlchemy en la capa de datos.** Sinks, `graph_sink`, consultas y los modelos de dominio de las aplicaciones (AMDockVS define los suyos con SQLModel) sí se benefician. Ahí las escrituras ya van por lotes.
4. **Dependencia opcional a medio plazo.** Si el núcleo de ejecución no importa SQLAlchemy, `pip install ms_flow` para paralelizar queda con `loky` como única dependencia real.

Hoy 13 módulos de `core/executor` y 8 de `core/database` importan SQLAlchemy o SQLModel. El cambio se concentra en los primeros.

## 4. Código muerto, complejo o mal estructurado

Hay poco código muerto: unas 500 líneas de 23.880, un 2 %. El problema es de estructura: un objeto central de 1.904 líneas, varias formas de enviar trabajo y una capa de aplicación de escritorio dentro de la biblioteca.

| Hallazgo | Evidencia | Acción |
| --- | --- | --- |
| Objeto central sobredimensionado | `manager/_manager.py`: 1.904 líneas y 147 métodos, 80 de ellos de una sola sentencia que delega en un servicio | El manager compone servicios; los servicios no vuelven a él |
| Servicios acoplados de forma implícita | `JobStatusService` y `job_commands` resuelven cualquier atributo en el manager con `__getattr__` | Dependencias explícitas en el constructor |
| SQL repartido por todo el motor | 87 `get_session()` y 48 `commit()`; 13 módulos de `core/executor` importan SQLAlchemy | Un único módulo de almacén |
| Demasiadas puertas de entrada | `MolSuite.run`, `MolSuite.submit_job`, `task()` y `job()`, `JobDefinition.submit` y `.run` en `tasking.py` (771 líneas), `WorkflowSpec`, `job_templates.py`, y el `submit_job` del motor con 35 parámetros | Una API pequeña (`map`, `submit`) y lo demás construido encima |
| Superficie pública grande | `MolSuite` expone 61 métodos públicos; `ms_flow.api` exporta 36 nombres | Separar núcleo y extras |
| Funciones largas | 21 funciones de más de 100 líneas: `insert_graph` 216, los dos `submit_job` 195 y 163, `get_healthcheck` 176, `build_job_runtime_metrics` 169 | Partir al tocarlas, no antes |
| Capa de aplicación dentro del núcleo | `core/apps.py`, `core/app_settings.py`, `core/configuration.py` (433), `core/settings/` (832), `core/project/` (1.071), `tools.py` (recetas de instalación, 351) y `provisioning.py` (ssh, conda y clústeres Ray, 484) | Moverla a un subpaquete que el núcleo no importe |
| Tres módulos para una decisión de admisión | `local_scheduler.py`, `local_compute_scheduler.py` y `resource_manager.py`, 350 líneas en total | Unirlos en uno |
| Nombres heredados | La clase pública es `MolSuite`; los loggers son `molsuite.*`; el estado vive en `~/.molsuite`; el directorio de HPC es `_molsuite_runtime` | Nombres propios de `ms_flow`, con alias de compatibilidad |
| Idiomas mezclados | Al menos 23 mensajes en español en el código ("No se encontro job", "Executor no disponible") y guías de `docs/guides` en español | Todo en inglés |
| Índices sobrantes | `executor_job_chunks` tiene 7 índices de una columna (entre ellos `cpu_required`, `gpu_required` y `updated_at`) y ninguno compuesto | Un índice `(job_id, status)` y quitar el resto |

### Código muerto

Búsqueda por nombre en `ms_flow`, AMDockVS, `ms_components`, `ms_contactmap` y `ms_projects`. Es una heurística: marca candidatos, no certezas.

- 51 símbolos sin ninguna referencia en código de producción, 500 líneas en total.
- 14 de ellos solo los usan tests, benchmarks o ejemplos (294 líneas).
- Se concentran en `artifacts.py` (166 líneas), `core/database/project.py` (103) y `core/settings/models.py` (82, parte son validadores de pydantic y no cuentan).
- Ejemplos claros: `resolve_project_store_ops`, `run_with_options`, `configure_runtime_limits`, `purge_job_history`, `dispose_cached_engines`.
- `advanced.py` tiene 5 líneas. Hay 49 menciones a compatibilidad o código heredado.

### Estado del repositorio

- Conviven `test/` (1 fichero) y `tests/` (61 ficheros, 357 tests).
- `chunk_payloads/` y `__pycache__` están en la raíz.
- Sin versionar: `.github/workflows/ci.yml`, `benchmarks/`, `examples/`, `docs/guides/`, `docs/reference/`, `site/`, `MANIFEST.in`. El repositorio tiene 15 commits.
- `benchmarks/history/overhead_latest.json` apunta a una ruta antigua y da 18 ms por chunk, que ya no es cierto.
- Un test falla por una lista `__all__` que creció de 3 a 8 nombres.

## 5. Escalabilidad

El coste por chunk crece de forma lineal con el tamaño del job, así que el tiempo total crece con el cuadrado. Un job de 40.000 chunks vacíos tarda 203 s, 5,07 ms de media por chunk.

Coste de orquestación por chunk en un único job de tareas vacías (hilos, ventana de 64), medido por bloques. La serie con índice se midió hasta 20.000 chunks.

| Chunks completados | Sin cambios (ms por chunk) | Con índice `(job_id, status)` (ms por chunk) |
| --- | --- | --- |
| 2.666 | 2,10 | 2,11 |
| 5.332 | 2,34 | 2,31 |
| 7.998 | 2,90 | 2,61 |
| 10.664 | 3,50 | 3,08 |
| 13.330 | 3,88 | 3,31 |
| 15.996 | 4,44 | 3,46 |
| 18.662 | 4,86 | 3,86 |
| 21.328 | 5,12 | — |
| 23.994 | 5,23 | — |
| 26.660 | 5,66 | — |
| 29.326 | 6,12 | — |
| 31.992 | 6,35 | — |
| 34.658 | 7,00 | — |
| 37.324 | 7,37 | — |
| 39.990 | 9,18 | — |

Añadir un índice compuesto `(job_id, status)` reduce la pendiente un tercio, pero no la elimina: el resto viene de agregados que recorren todos los chunks del job en cada refresco de estado.

### Qué implica

Extrapolación lineal de la pendiente medida (0,19 ms más por cada 1.000 chunks). No está medido más allá de 40.000.

| Chunks en un job | Orquestación estimada |
| --- | --- |
| 40.000 (medido) | 3,4 min |
| 100.000 | unos 19 min |
| 1.000.000 | unas 27 h |

Hoy la única forma de escalar es agrupar muchos ítems por chunk. Funciona (59 µs por ítem con lotes de 100), pero obliga al usuario a elegir el tamaño de lote y hace más gruesa la unidad de reintento y de progreso.

### Otros límites

- **Un solo hilo de motor.** Hace el despacho, la persistencia, el estado y el sondeo. El techo actual está entre 200 y 500 chunks por segundo, con cualquier número de workers.
- **Tamaño de `executor.db`.** 54 MB para 40.000 chunks sin payload, 1,35 KB por chunk, más 2 filas de evento por chunk (81.251 eventos). Un millón de chunks son 1,3 GB antes de contar payloads.
- **Limpieza solo por antigüedad.** `purge_finished_jobs` borra chunks y eventos de jobs terminados hace días, y solo al abrir el proyecto. No hay compactación al terminar el job.
- **Filas de chunk pesadas, no por el payload.** El job solo guarda la referencia al generador; las filas de chunk se crean por ventana, justo antes de despachar, y un payload de hasta 512 KB va dentro de la fila (por encima, a un fichero). Lo que ocupa los 1,35 KB medidos con payloads de 10 bytes es la fila en sí: 22 columnas, 7 índices de una columna, el único de `chunk_id`, la clave primaria UUID y 2 eventos por chunk. Las filas terminadas, con su payload, se quedan hasta la purga por antigüedad.
- **Muchos jobs a la vez.** Los candidatos se piden con `LIMIT 200` por tick. No lo he medido con cientos de jobs concurrentes.

### Lo que ya escala bien

- La alimentación es por ventana y admite generadores: con 40.000 chunks el proceso se mantuvo en 76 MB de memoria.
- La admisión por tokens de CPU y GPU funciona y está probada.
- Los sinks escriben por lotes en un pool aparte.

## 6. Robustez

Es el punto más débil hoy. Un worker que muere deja el pool de procesos roto para todos los jobs siguientes, y un proceso interrumpido pierde el job sin forma de reanudarlo.

Cada escenario se ejecutó contra el motor real, con `executor.db` nuevo.

| Escenario | Resultado medido | Gravedad |
| --- | --- | --- |
| Un worker muere (`os._exit`, equivale a segfault u OOM) en 1 de 40 chunks | Entre 29 y 36 chunks marcados fallidos, con un solo culpable. Los tres jobs siguientes en el mismo executor fallan al 100 % en 0,12 s, también 3 s después | Crítica |
| `kill -9` del proceso principal con un job de 200 chunks en curso | Los 7 procesos hijos siguen vivos 30 s después. Al reabrir, el job queda `failed` (`runtime_interrupted`): 36 hechos, 16 marcados fallidos y 148 nunca emitidos. `resubmit_job` lanza `ValueError` | Crítica |
| Dos procesos abren el mismo `executor.db` | El segundo, al arrancar, marca como interrumpido el job del primero. Quedan 16 chunks `pending` huérfanos | Alta |
| El 10 % de los chunks lanza una excepción | Job `failed` con 90 hechos y 10 fallidos; `job.error` vacío. El traceback sí queda por chunk | Media |
| Chunks colgados, luego cancelación | No existe límite de tiempo por chunk. Cancelar sí funciona: `canceled` en 0,06 s, workers eliminados y el job siguiente completa | Media |

### Causas en el código

1. **El pool roto no se reconstruye.** `_reset_pool_locked` solo se llama desde `cancel` (`local_adapters.py:428`). Un `BrokenProcessPool` en `submit` o `poll` deja el mismo pool para siempre.
2. **No hay reintentos.** Un fallo de chunk es definitivo ("Chunk failed permanently"). Los chunks que caen por culpa de un vecino tampoco se reencolan.
3. **No hay reanudación.** Un job interrumpido pasa a `failed`. `resubmit_job` exige un `_chunker_ref` persistido, que los jobs enviados con una lista no tienen, y cuando existe vuelve a empezar desde el cursor 0.
4. **No hay propiedad entre procesos.** Ni fichero de bloqueo ni identificador de dueño en el job. Cualquier manager que arranca asume que todo lo activo es suyo y está muerto.
5. **Los workers no vigilan al padre.** Tras un `kill -9` siguen ocupando CPU y memoria.
6. **No hay tiempo máximo por chunk.** Los `timeout_s` existentes son el de inactividad de loky y el de la llamada de envío.
7. **El estado final no distingue matices.** Un job con el 90 % correcto es `failed`, igual que uno que no arrancó, y sin mensaje a nivel de job.
8. **Errores silenciados.** Hay 92 `except Exception` en el paquete, 27 de ellos con `pass`.

### Lo que sí funciona

- La cancelación es rápida y deja el pool usable.
- El corte temprano por ratio de fallos (`chunk_fail_fast_*`) existe y está probado.
- Los jobs de HPC se readoptan tras un reinicio gracias a `handle.json`.
- La suite pasa: 356 tests correctos y 1 fallo (`test_public_api`, una lista `__all__` desfasada), en 117 s.

### Qué hace falta

- Reconstruir el pool ante `BrokenProcessPool` y reencolar una vez los chunks que estaban en vuelo.
- `max_retries`, espera entre reintentos y `chunk_timeout_s` por job, con contador de intentos por chunk.
- Estado `interrupted` reanudable: los chunks en curso vuelven a pendiente y el feed sigue desde su cursor.
- Bloqueo de `executor.db` con pid y host; un segundo proceso abre en modo solo lectura.
- Vigilancia del padre en los workers, para que mueran con él.
- Estado final `completed_with_errors` y un resumen del primer error en el job.

## 7. Procesos locales, Ray y HPC

El backend local es el único listo para campañas. Ray funciona pero hereda el overhead del motor, y HPC solo sirve hoy si el PC y el clúster comparten sistema de ficheros y entorno.

| Backend | Estado real | ms por chunk vacío | Hueco principal |
| --- | --- | --- | --- |
| `thread` | Sólido | 4,7 (1,8 afinado) | Overhead del motor |
| `compute` (loky) | Sólido en el camino feliz | 5,2–6,0 | No se recupera de un worker muerto |
| Ray nativo | Funciona; marcado `experimental` | 5,1 (2,6 con ventana de 64) | 13–25 veces el coste de Ray directo (0,20 ms) |
| HPC por comandos | Funciona con comandos falsos en tests; marcado `stable` | No medido | Un trabajo del planificador por chunk, sin transporte remoto |

### Local

- **Solo funciones importables.** Lambdas, funciones anidadas y las definidas en `__main__` se rechazan (`runner_refs.py`). Es el mayor freno de uso en notebooks; loky ya sabe serializarlas con cloudpickle.
- **Un proceso gestor extra.** El adapter de loky crea un `multiprocessing.Manager` para la cola de progreso y las marcas de cancelación. Cada acceso es una llamada entre procesos.
- **Workers efímeros.** El pool se crea con 10 s de inactividad máxima. Tras una pausa, el siguiente job vuelve a pagar el arranque (0,95 s).
- **Solo Linux probado.** El CI declarado usa `ubuntu-latest` con Python 3.12. No hay nada específico de Windows o macOS en el motor.

### Ray

- `ray.init` local tarda 7,2 s y el primer job 3,9 s. Después rinde como el backend local.
- **Trampa en el registro.** `register_ray_executor(...)` con `native=False`, que es el valor por defecto, registra un adapter sin `submit`. Todos los chunks fallan con "has no submit implementation".
- **Una tarea de Ray por chunk y un `ray.wait` por chunk y tick.** No se agrupan las esperas.
- El transporte de ficheros (`ray.put` de entradas, recogida de artefactos, `shared_fs`) está bien planteado.
- `provisioning.py` levanta clústeres por ssh y conda. Existe y tiene tests; no lo he ejercitado contra máquinas reales.
- La documentación cita un adapter nativo de Dask que no existe en el código.

### HPC

- **Un `submit` por chunk.** Cada chunk genera su `submit.sh` y una llamada al comando de envío. No hay job arrays ni agrupación de chunks en una reserva. Con 10.000 chunks son 10.000 trabajos en la cola del clúster.
- **Sin transporte remoto.** El directorio de control (`manifest.json`, `status.json`, `result.json`) se crea con llamadas locales al sistema de ficheros. No hay ssh ni copia en el adapter. Desde un PC exige montar el disco del clúster y envolver los comandos en ssh a mano.
- **Entorno idéntico supuesto.** El script exporta el `PYTHONPATH` y el intérprete del PC. En un nodo de cómputo esas rutas normalmente no existen.
- **Sondeo por chunk en el hilo del motor.** Cada chunk en curso lanza un subproceso del comando de consulta cada 2 s, de forma síncrona. Con 1.000 chunks en curso son 500 procesos por segundo bloqueando todo lo demás, jobs locales incluidos.
- **Un fallo transitorio es definitivo.** Si el comando de consulta devuelve un código distinto de cero, el chunk pasa a `FAILED`. En un clúster cargado eso ocurre con frecuencia.
- **Envío con límite de 30 s.** Si el comando de envío tarda más, el chunk se marca fallido aunque el trabajo haya entrado en la cola.
- **Sin plantillas.** No hay presets para Slurm, PBS, SGE ni LSF, ni traducción de CPU, memoria, tiempo o GPU a directivas del planificador. El usuario escribe los tres comandos.
- **Resultados en JSON.** `result.json` usa `default=str`: un resultado no serializable se convierte en texto sin avisar.
- **Lo que está bien.** La readopción tras reinicio con `handle.json` funciona y es justo lo que diferencia a ms_flow: cerrar el portátil y retomar la campaña.

### Qué hace falta para HPC desde un PC

1. Un trabajo del planificador por lote de chunks: job arrays, o un trabajo "piloto" que ejecuta N chunks dentro de una reserva.
2. Transporte por ssh (o rsync) del directorio de control y de los resultados.
3. Entorno remoto declarado: intérprete, activación de conda o módulos, ruta del paquete.
4. Un único sondeo por job (`squeue -j a,b,c` o `sacct`), fuera del hilo del motor, con reintentos.
5. Presets de Slurm y PBS con recursos por job.
6. Resultados en ficheros (por ejemplo los `.mshard` que ya existen) en vez de JSON por chunk.

## 8. Valor de mercado

El hueco existe y es defendible: campañas masivas reanudables desde un PC, con todo el estado en un fichero SQLite y sin levantar ningún servicio. Hoy ms_flow no lo ocupa porque le faltan justo las garantías que ese público da por supuestas.

La comparación sale de lo que conozco de estas herramientas; no la he contrastado hoy con su documentación.

| Herramienta | Sin servidor | Estado persistente y reanudable | HPC | Unidad de trabajo |
| --- | --- | --- | --- | --- |
| `multiprocessing`, `joblib`, loky | Sí | No | No | Función Python |
| Dask distributed | Necesita planificador | No | Con dask-jobqueue | Función o grafo |
| Ray | Necesita clúster | No | Limitado | Función o actor |
| Parsl | Sí | Caché y checkpoints | Sí, con pilotos | Función Python |
| submitit | Sí | Por trabajo | Slurm, con arrays | Función Python |
| Snakemake, Nextflow | Sí | Sí, por ficheros | Sí | Regla sobre ficheros |
| FireWorks, jobflow | Necesita MongoDB | Sí | Sí | Tarea |
| Prefect, Airflow, Dagster | Necesitan servicio | Sí | No es su foco | Tarea de servicio |
| **ms_flow** | **Sí** | **Registro sí, reanudación no** | **Solo con disco compartido** | **Función Python sobre lotes de datos** |

Los vecinos reales son Parsl y submitit por el lado de HPC, y Snakemake por el de reanudación. Ninguno combina función Python, estado en un solo fichero y salida directa a tablas.

### Lo que ya diferencia a ms_flow

- Todo el estado de la campaña en un fichero que se copia, se inspecciona con cualquier cliente SQLite y se archiva con los datos.
- Entrada y salida de datos integradas: consultas como entrada y sinks por lotes a tablas, no solo cómputo.
- Alimentación por ventana con memoria plana, sirve para entradas de millones de ítems.
- Admisión por tokens de CPU y GPU en una sola máquina.
- El mismo job sobre hilos, procesos, Ray o HPC.
- Readopción de trabajos de clúster tras cerrar la aplicación.
- Componentes Qt de monitorización ya hechos en `ms_components`.

### Lo que falta para poder venderlo

1. **Garantías básicas.** Reintentos, tiempo máximo por chunk, reanudación y aislamiento de fallos. Sin esto, "persistencia" significa solo que queda constancia de lo que se perdió.
2. **Reanudar como función estrella.** Volver a lanzar el mismo script y que solo se ejecute lo que falta. Es el argumento de venta frente a joblib y Dask.
3. **Un hola mundo de 5 líneas.** Sin proyecto, sin registro de aplicación, con lambdas, devolviendo resultados.
4. **Rendimiento defendible.** A menos de 2 veces de loky en tareas cortas, o agrupación automática que lo esconda.
5. **Línea de comandos.** Estado, listado, errores, cancelar, reanudar y purgar sobre un fichero de campaña, sin Qt.
6. **HPC desde el portátil.** ssh, job arrays y presets de Slurm y PBS.
7. **Compatibilidad amplia.** Hoy exige Python 3.12 o superior y solo se prueba en Linux. Muchos entornos de clúster siguen en 3.10 o 3.11, y los laboratorios usan Windows y macOS.
8. **Migraciones de esquema.** Hoy una versión de esquema distinta lanza un error. Una actualización no puede dejar inservible una campaña a medias.
9. **Documentación en inglés.** Inicio rápido, conceptos, recetas (barrido de parámetros, campaña de docking) y una página honesta de cuándo no usarlo.
10. **Credibilidad científica.** Benchmarks reproducibles frente a joblib y Parsl, un artículo corto (JOSS) con DOI, y AMDockVS como caso de uso real.

### Lo que sobra en el núcleo

No es para borrar, es para sacar del paquete que se anuncia.

- Proyectos, registro de aplicaciones, ajustes y configuración: son de las aplicaciones de escritorio de MolSuite.
- Recetas de instalación de herramientas y aprovisionamiento por ssh y conda.
- Perfiles operativos, chequeo de salud extenso, motivo del planificador persistido y tabla de latidos.
- Eventos por chunk activados por defecto.
- Las puertas de entrada duplicadas y el `submit_job` de 35 parámetros.
- El nombre `MolSuite` como clase principal de una biblioteca genérica.

## 9. Otras observaciones

- **Observar frena.** `wait_for_job` y cualquier interfaz que llame a `get_job` consultan la base de datos. Sondear cada 2 ms duplicó el coste por chunk en mis pruebas. El estado debería servirse desde memoria y notificarse por eventos; `blinker` ya es dependencia.
- **Semántica sin documentar.** La ejecución es "al menos una vez" y la idempotencia queda a cargo del dominio. Hay que decirlo en la primera página, y ofrecer una clave de deduplicación.
- **Pickle en disco.** Los resultados grandes se vuelcan con `pickle` (`result_handlers.py`). Es frágil entre versiones y no debe leerse de directorios compartidos.
- **Fechas sin zona.** Hay 42 usos de `datetime.now()` frente a 2 en UTC. Mezclar ambas rompe duraciones y purgas en cambios de hora y entre PC y clúster.
- **Dependencias menores.** `tqdm` y `blinker` se usan en un fichero cada una y `toml` en cuatro, todos de configuración. Si la capa de aplicación sale del núcleo, el núcleo se queda con `loky`.
- **La suite es un activo.** 357 tests en 117 s. Faltan justo los de los fallos de este informe: muerte de worker, `kill -9`, dos procesos sobre el mismo fichero y jobs de más de 10.000 chunks.
- **Benchmarks como contrato.** Ya existen `benchmarks/` y tests de benchmark. Conviene fijar un presupuesto (ms por chunk, pendiente con el tamaño) y hacer que el CI falle si se supera.
- **README de 45 líneas.** Es la primera impresión del paquete en PyPI.

## 10. Plan de acciones

Seis fases, cada una con una puerta medible. Las fases 0 y 1 se hacen sobre el motor actual y son las que desbloquean el uso en campañas reales. La fase 2 es la única reescritura; todo lo demás se apoya en ella o es independiente.

| Fase | Objetivo | Contenido | Puerta de salida | Tamaño estimado |
| --- | --- | --- | --- | --- |
| 0. Arreglos rápidos | Quitar los fallos y costes que se arreglan sin rediseño | Reinicio del pool roto, índice compuesto, ventana proporcional a los workers, eventos por chunk opcionales, trampa `native=False` de Ray, test roto, literales en español | Un job tras la muerte de un worker termina bien. 20.000 chunks a 3 ms o menos de media | Días |
| 1. Robustez | Que una campaña sobreviva a fallos normales | Reintentos, tiempo máximo por chunk, bloqueo de un solo proceso por fichero, vigilancia de muerte del padre, estado `completed_with_errors`, resumen de error en el job | `kill -9` no deja procesos huérfanos a los 5 s. Un segundo proceso recibe un error claro. Un chunk colgado se corta solo | 1 a 2 semanas |
| 2. Núcleo del motor | Coste por chunk bajo y plano | Estado en memoria como fuente de verdad, diario diferido con `sqlite3` directo, contadores incrementales, bucle por eventos, agrupación automática, reanudación desde el diario | 0,2 ms por chunk o menos con persistencia y 0,05 ms sin ella. Coste plano (±10 %) hasta 1 millón de chunks. `kill -9` y reabrir termina 200 de 200 | 3 a 5 semanas |
| 3. API simple | Que se use en 5 líneas | `Flow(ruta).map(fn, items)`, niveles de durabilidad, funciones locales y lambdas, memoización por clave de entrada, línea de comandos, migraciones de esquema | El hola mundo cabe en 5 líneas sin proyecto. Relanzar un script ejecuta solo lo que falta | 2 a 3 semanas |
| 4. HPC y Ray | Clúster desde el portátil | Job arrays o piloto, transporte ssh, presets de Slurm y PBS, sondeo agrupado fuera del hilo del motor, tolerancia a fallos transitorios de sondeo, tareas Ray agrupadas | 1.000 chunks generan 1 array y 1 sondeo por intervalo. Funciona sin disco compartido | 3 a 4 semanas |
| 5. Publicación | Paquete presentable | Capa de aplicación fuera del núcleo, nombres propios, documentación en inglés, CI en 3 sistemas y Python 3.10 a 3.13, benchmarks publicados, artículo | Instalación limpia en los 3 sistemas. Página de benchmarks reproducible. DOI | 2 a 3 semanas |

Los tamaños son una estimación mía para una persona que conoce el código, no una medida.

### Orden y dependencias

- **0 y 1 primero, sin esperar a nada.** Arreglan lo que hoy puede perder una campaña en AMDockVS.
- **La reanudación va en la fase 2, no en la 1.** Hacerla sobre el motor actual obliga a persistir el generador de chunks y a rehacerla después. En la fase 1 basta con que el job quede marcado como interrumpido y con sus chunks terminados intactos.
- **La fase 3 depende de la 2.** Los niveles de durabilidad son la interfaz del diario; sin él solo serían un parámetro que no cambia nada.
- **La fase 4 es casi independiente.** Puede ir en paralelo a la 2 si hay dos personas; toca los adaptadores, no el motor.
- **La fase 5 se puede adelantar en parte.** Separar la capa de aplicación y pasar los textos a inglés no depende del motor.

### Qué no hacer

- No sustituir SQLite ni añadir un servidor. El fichero único es la ventaja.
- No reescribir la capa de datos (sinks, consultas, modelos de dominio). Su coste es por lote, no por chunk, y ahí SQLAlchemy sí aporta.
- No añadir Dask ni más backends hasta que los cuatro actuales cumplan sus puertas.
- No optimizar el motor actual más allá de la fase 0. El coste viene del diseño, no de detalles.

## 11. Tareas

Cada tarea lleva su criterio de aceptación y la sección del hallazgo que resuelve.

### Fase 0. Arreglos rápidos

| ID | Tarea | Criterio de aceptación | Sección |
| --- | --- | --- | --- |
| F0-1 | Recrear el pool de procesos al detectar `BrokenProcessPool` y reencolar los chunks que no llegaron a ejecutarse | Tras matar un worker, solo falla el chunk culpable y el siguiente job termina con 20 de 20 | 6 |
| F0-2 | Índice compuesto `(job_id, status)` en `executor_job_chunks` y retirar los índices de una columna que no usa ninguna consulta | 20.000 chunks a 3 ms o menos de media; inserciones no más lentas | 5 |
| F0-3 | `max_inflight_tasks` por defecto proporcional a los workers del ejecutor | Con 28 hilos, el valor por defecto iguala al ajuste manual de 64 | 1 |
| F0-4 | Eventos por chunk desactivados por defecto; se conservan los de job | Un job de 40.000 chunks escribe menos de 100 eventos | 1, 5 |
| F0-5 | `register_ray_executor` sin adaptador nativo falla al registrar, no al ejecutar | Error claro en el registro, con el parámetro que falta | 7 |
| F0-6 | Corregir `test_top_level_package_is_a_minimal_entrypoint` | 357 de 357 tests en verde | 4 |
| F0-7 | Pasar a inglés los literales en español del código y retirar de la documentación el adaptador Dask que no existe | Búsqueda de literales en español sin resultados en `src/` | 4, 7 |
| F0-8 | Tests de regresión para muerte de worker, dos gestores sobre un fichero y job de 20.000 chunks | Los tres fallan en la rama actual y pasan con las correcciones | 6, 9 |

### Fase 1. Robustez

| ID | Tarea | Criterio de aceptación | Sección |
| --- | --- | --- | --- |
| F1-1 | Reintentos por chunk con número máximo y espera creciente, distinguiendo error de la función de fallo de infraestructura | Un chunk que falla 2 veces y acierta a la tercera cuenta como terminado, con 3 intentos registrados | 6 |
| F1-2 | Tiempo máximo por chunk | Un chunk colgado se corta al límite, se marca `timeout` y el job continúa | 6 |
| F1-3 | Bloqueo de propietario por fichero de estado | Un segundo proceso recibe un error con el PID del propietario, o abre en solo lectura | 6 |
| F1-4 | Vigilancia de muerte del padre en los workers | Tras `kill -9` al proceso principal, 0 workers vivos a los 5 s | 6 |
| F1-5 | Estado `interrupted` para jobs cortados, distinto de `failed`, sin tocar los chunks terminados | Tras reabrir, el job figura como interrumpido con sus 36 chunks hechos intactos | 6 |
| F1-6 | Estado `completed_with_errors` y resumen de errores en `job.error` | Un job con 10 % de fallos muestra el estado nuevo y los 3 errores más frecuentes | 6 |
| F1-7 | Fechas en UTC en todo el paquete | 0 usos de `datetime.now()` sin zona | 9 |

### Fase 2. Núcleo del motor

| ID | Tarea | Criterio de aceptación | Sección |
| --- | --- | --- | --- |
| F2-1 | Estado de jobs y chunks en memoria como fuente de verdad; el bucle no hace ninguna lectura a la base de datos | 0 sentencias `SELECT` por chunk en régimen estable | 1, 5 |
| F2-2 | Interfaz de diario con dos implementaciones: nula y SQLite | El motor pasa la suite con ambas | 2 |
| F2-3 | Diario SQLite con `sqlite3` directo, un hilo escritor y `executemany` por lotes; esquema por DDL y `PRAGMA user_version` | 0,2 ms por chunk o menos con persistencia | 2, 3 |
| F2-4 | Contadores de job incrementales en lugar de agregados sobre todos los chunks | Coste por chunk plano (±10 %) entre 1.000 y 1.000.000 de chunks | 5 |
| F2-5 | Bucle despertado por eventos en lugar de sondeo a intervalo fijo | Job de 1 chunk en menos de 10 ms tras el arranque | 1 |
| F2-6 | Agrupación automática de ítems por tarea según su duración medida | Tareas vacías a menos de 2 veces de loky directo | 1, 8 |
| F2-7 | Reanudación desde el diario: al reabrir se reconstruye el estado y solo se ejecuta lo pendiente | `kill -9` a mitad y reabrir termina con 200 de 200 y ningún chunk terminado se repite | 6 |
| F2-8 | Filas de chunk compactas y purga automática de chunks terminados en jobs completos | Menos de 0,3 KB por chunk en disco | 5 |
| F2-9 | Retirar SQLModel del motor; se queda en la capa de datos | Importar el motor sin capa de datos tarda menos de 100 ms | 1, 3 |
| F2-10 | Unificar los tres módulos de planificación en uno y partir `_manager.py` | Ningún fichero del motor supera 800 líneas; sin delegadores de una línea | 4 |

### Fase 3. API simple

| ID | Tarea | Criterio de aceptación | Sección |
| --- | --- | --- | --- |
| F3-1 | Fachada `Flow` con `map` y `submit`, sin proyecto ni registro de aplicación | Hola mundo de 5 líneas en el README, ejecutado en CI | 1, 8 |
| F3-2 | Niveles de durabilidad `none`, `journal` y `full` como único parámetro | `none` no crea ningún fichero; `journal` es el valor por defecto | 2 |
| F3-3 | Funciones locales, lambdas y de `__main__` mediante cloudpickle | Un script de un solo fichero funciona con el ejecutor de procesos | 4, 8 |
| F3-4 | Memoización por clave de entrada | Relanzar el mismo script ejecuta 0 chunks ya terminados | 8 |
| F3-5 | Línea de comandos: `status`, `ls`, `tail`, `errors`, `cancel`, `resume`, `purge` | Todos funcionan sobre un fichero de campaña sin Qt | 8 |
| F3-6 | Migraciones de esquema por pasos | Un fichero de la versión anterior se abre y se actualiza sin pérdida | 8, 9 |
| F3-7 | Estado servido desde memoria y por suscripción | Observar un job no cambia su coste por chunk (±5 %) | 9 |
| F3-8 | Reducir `submit_job` a los parámetros de uso común y agrupar el resto en un objeto de opciones | 10 parámetros o menos en la firma pública | 4 |

### Fase 4. HPC y Ray

| ID | Tarea | Criterio de aceptación | Sección |
| --- | --- | --- | --- |
| F4-1 | Envío por job arrays o por piloto en lugar de un trabajo de clúster por chunk | 1.000 chunks generan 1 envío | 7 |
| F4-2 | Sondeo agrupado y fuera del hilo del motor | 1 consulta al planificador por intervalo, sea cual sea el número de chunks | 7 |
| F4-3 | Fallo transitorio de sondeo no marca el chunk como fallido | 3 sondeos fallidos seguidos y uno correcto dejan el chunk en ejecución | 7 |
| F4-4 | Transporte ssh para directorio de control, envío y resultados | Campaña lanzada desde un portátil sin disco compartido | 7 |
| F4-5 | Presets de Slurm y PBS | Registrar un clúster Slurm con nombre de partición y poco más | 7 |
| F4-6 | Formato de resultados que conserve tipos, en lugar de JSON con `default=str` | Un array numérico vuelve como array | 7 |
| F4-7 | Comprobación de entorno remoto antes de enviar | Error claro si falta el paquete o la versión no coincide | 7 |
| F4-8 | Ray: tareas agrupadas y una sola espera sobre todas las referencias | Tareas vacías a menos de 3 veces de Ray directo | 7 |

### Fase 5. Publicación

| ID | Tarea | Criterio de aceptación | Sección |
| --- | --- | --- | --- |
| F5-1 | Mover proyectos, aplicaciones, ajustes, configuración, herramientas y aprovisionamiento a un subpaquete de aplicación | El núcleo se instala e importa sin ese subpaquete | 4, 8 |
| F5-2 | Nombres propios: clase principal, loggers y directorio de usuario | Ninguna referencia a `molsuite` en el núcleo | 4 |
| F5-3 | Eliminar el código muerto confirmado | Los 51 símbolos sin referencias revisados uno a uno y retirados o justificados | 4 |
| F5-4 | Documentación en inglés: inicio rápido, conceptos, recetas, comparación y cuándo no usarlo | Sitio publicado; cada ejemplo se ejecuta en CI | 8 |
| F5-5 | CI en Linux, macOS y Windows con Python 3.10 a 3.13 | Matriz en verde | 8 |
| F5-6 | Página de benchmarks reproducible frente a loky, joblib y Parsl, con presupuesto en CI | El CI falla si el coste por chunk supera el presupuesto | 8, 9 |
| F5-7 | Paquete en conda-forge, versionado semántico y registro de cambios | Instalable con `conda install` | 8 |
| F5-8 | Artículo corto y DOI, con AMDockVS como caso de uso | Envío a JOSS | 8 |
