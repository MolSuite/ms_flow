# ms_flow — Auditoría del diseño de tasks y jobs

2026-10-08. Borrador para discutir antes del relevo (C1). Todo lo que se afirma está comprobado contra el código de `ms_flow` y contra el uso real en `AMDockVS/src`. Las propuestas del final son eso, propuestas.

## Resumen

- Hay **cinco formas** de declarar un job y AMDockVS usa dos. Una de las cinco no funciona.
- Task y job son **dos objetos para una sola cosa**, con campos repetidos.
- La definición de un job tiene **27 campos**; varios no los lee nadie.
- Enviar un job admite **24 parámetros**; el motor nuevo necesita unos seis.
- **El job no declara qué datos lee ni qué escribe.** De ahí salen tres problemas que hoy se resuelven por fuera: las dependencias se adivinan por el nombre del job, el total de chunks no se conoce, y al reanudar el conjunto de entrada cambia (tu punto 4.1).

## 1. Qué hay hoy

| Forma de declarar | Dónde | Usos en AMDockVS |
|---|---|---|
| `@task` + `@job` | `tasking.py` | 10 tasks, 14 jobs |
| Clase `JobSpec` | `tasking.py` | 9 clases |
| `streaming_job`, `batch_job`, `build_streaming_job_definition` | `job_templates.py` | 0 |
| `workflow()` / `WorkflowSpec` + `molsuite.run(input=, process=, output=)` | `specs/workflow.py`, `job_api.py` | 0 |
| `molsuite.task(...)` / `molsuite.job(...)` | `job_api.py` | 0 |

La última está rota: llama a `task(name=, handler=)` y a `job(name=, task=, chunker=)`, y ninguna de las dos funciones tiene esos parámetros (`TypeError: task() got an unexpected keyword argument 'handler'`).

Todas acaban en el mismo `JobDefinition`, que a su vez contiene un `TaskDefinition`.

## 2. Hallazgos

**H1. Task y job repiten campos.** `TaskDefinition` tiene `executor`, `supported_executors`, `cpu_required`, `description` y `tags`; `JobDefinition` los vuelve a tener y los pisa. En AMDockVS una task no vive sin su job: lo único que se comparte entre jobs es la función del worker, y para eso no hace falta un objeto.

**H2. 27 campos, seis de ellos por duplicado.** Cada función (`chunker`, `chunk_counter`, `setup`, `stage_chunk`, `finalize`, `result_handler_factory`) va acompañada de su cadena `*_ref` para poder reimportarla. `with_options` existe solo para mantener las parejas alineadas.

**H3. Campos que nadie lee o nadie usa.**

| Campo | Lectores en `ms_flow` | Uso en AMDockVS |
|---|---|---|
| `required`, `produces` (`RequirementSpec`, `CapabilitySpec`) | 0 | 8 jobs los fijan a `()` |
| `setup`, `stage_chunk` (y con ellos `stage_fail_policy`, `max_stage_failures`) | sí | 0 |
| `chunk_counter` / `count_chunks` | sí | 1 job |
| `finalize` | sí | 2 jobs |
| `result_handler_factory` | sí | 4 usos |

**H4. `store_results` tiene dos valores por defecto.** `False` en `_build_job_definition` y en `molsuite.run`; `True` en `JobDefinition`, `JobSpec` y `@job`. AMDockVS lo escribe a mano en cada job, siempre `False` (18 veces, ninguna `True`).

**H5. Las firmas se adivinan.** `fn(params, config)` o `fn(params)`, y `fn(payload, progress)` o `fn(payload)`, se deciden contando parámetros con `inspect.signature`. Para la task se hace en cada chunk cuando hay `progress`.

**H6. `config` es un saco.** La fachada mete en él objetos vivos (`project_store`, `project_db`, `project_context`, `project_resources`) y cada chunker los saca por nombre. Los chunkers de AMDockVS empiezan igual: sacar `project_db` de `config` y un `raise` si falta (17 veces en 9 ficheros). El job no puede decir «necesito la base del proyecto».

**H7. Enviar un job tiene 24 parámetros.** Entre ellos tres frenos que se pisan (`max_inflight_tasks`, `max_inflight_items`, `operational_profile`), tres umbrales de corte por fallos (`chunk_fail_fast_*`) y `queue_policy`. `molsuite.run` tiene otros 23. El motor nuevo regula con `window` y las fichas.

**H8. Los recursos se declaran a medias.** El job solo declara `cpu_required`. GPU y memoria viajan dentro de cada payload (`_cpu_required`, `_gpu_required`, vía `chunk_resources`). No hay dónde declarar reintentos ni tiempo máximo. El motor nuevo ya los acepta por job (`cpu`, `gpu`, `mem_mb`, `retries`, `timeout`).

**H9. Los resultados salen por cuatro puertas.** `output_spec` (sink), `result_handler_factory`, `store_results` (copia en `executor.db`) y `finalize`. AMDockVS usa sobre todo sinks (11), y las otras tres como excepción.

**H10. El troceo se escribe a mano en cada chunker.** Cada uno agrupa filas en lotes con su `batch_size`. En `docking/jobs.py` el diccionario del chunk está escrito dos veces seguidas (lote lleno y resto final), con 17 claves cada vez.

**H11. La entrada no se declara: es lo que el chunker consulte cuando corra.** Es la raíz de la sección siguiente.

**H12. Las dependencias se adivinan por el nombre.** `AMDockVS/runtime.py` clasifica cada job buscando subcadenas en su nombre (`"docking"`, `"prepare_"`, `"load_"`…) y espera a los jobs activos de las categorías previas. Hay además un canal lateral, `set_forced_dependencies`, para lo que la regla no cubre. `required`/`produces` nacieron para esto y nunca se conectaron.

## 3. El conjunto de entrada (punto 4.1)

Un job de AMDockVS recibe un filtro, no una lista: «ligandos preparados, de este set, sin docking hecho». El filtro se evalúa cuando el chunker corre. Eso da lo que querías (encadenar import → preparación → docking de una vez, sin futures), y tiene tres consecuencias.

**a) Hay dos clases de fuente y el motor solo distingue una.**

| | Fuente estable | Fuente filtrada por estado |
|---|---|---|
| Ejemplo | lista de ficheros, shards, ids explícitos | «preparados y no hechos» |
| Al repetirla | trae los mismos payloads | trae solo lo que falta, agrupado de otra forma |
| Quién recuerda lo hecho | el diario del motor (clave por chunk) | la propia tabla de resultados |
| Registro por chunk en el diario | necesario | inútil: los chunks hechos no vuelven a aparecer |

Lo segundo es el caso del docking: con `skip_existing` (activo por defecto) la fuente no vuelve a traer los pares hechos, así que ninguna clave registrada reaparece. Con el motor nuevo el resultado sería correcto (no se salta nada), pero guardaría una fila por chunk para nada y avisaría de que «la fuente no trajo ninguno de los chunks hechos».

**b) Posición frente a clave.** Guardar «voy por el chunk 1.000» en lugar de la lista de hechos ahorra las filas (37 bytes por chunk) y las búsquedas (5 a 7 µs por chunk). Solo es válido si la fuente es estable *y* ordenada. Y como los chunks terminan desordenados (14 a la vez), en realidad sería «todo lo anterior a N, más unos pocos sueltos por encima». El riesgo es asimétrico: si la fuente no era tan estable como se creía, la posición se salta trabajo sin hacer y nadie se entera; la clave, en el mismo caso, simplemente no coincide y repite.

**c) El conjunto cambia entre el envío y la reanudación.** Si se cancela a los 200 y mientras tanto se preparan 50 ligandos nuevos, al continuar el job hace 850, no 800. Tres formas de tratarlo:

| Opción | Qué congela | Coste |
|---|---|---|
| Nada (hoy) | nada: continuar es «haz todo lo que cumpla el filtro ahora» | ninguno |
| Fecha de corte (`created_at <= inicio del job`) | las filas añadidas después | una columna de fecha en cada tabla de origen. **No congela los cambios de estado**: un ligando que ya existía y se preparó después pasa el filtro |
| Lista de miembros al arrancar (`INSERT INTO job_items SELECT id FROM <filtro>`, una sola sentencia) | exactamente el conjunto | unos bytes por elemento en `project.db`; a cambio da el total exacto para la barra de progreso |

El corte tiene que ser «cuando el job empieza a leer», no «cuando se envía»: un docking encadenado detrás de la preparación debe ver lo que esa preparación produjo.

## 4. Propuesta para discutir

1. **Una sola declaración.** Un job es: la función del worker, la fuente de chunks y lo que declara. Sin `TaskDefinition` aparte. Dos formas de escribirlo como mucho (decorador para lo pequeño, clase para lo grande), un solo objeto debajo. Fuera plantillas, `workflow()`, `molsuite.run` y `molsuite.task/job`.
2. **El job declara qué lee y qué escribe** (`reads=("ligands",)`, `writes=("docking_results",)`). Las dependencias salen de ahí y desaparecen las subcadenas del nombre y `set_forced_dependencies`.
3. **La fuente declara de qué clase es.** Estable (por defecto): el diario registra cada chunk y reanuda exacto. «Ya excluye lo hecho»: el diario no guarda filas por chunk, solo los fallos. Con esto un docking interrumpido no deja nada que limpiar.
4. **Congelar el conjunto, opcional por job**, con la lista de miembros. No lo haría por fecha.
5. **Recursos y política de fallos en la declaración**: `cpu`, `gpu`, `mem_mb`, `retries`, `timeout`. El payload solo los pisa cuando un chunk concreto difiere.
6. **Una puerta para los resultados**: `output` (un sink) y, si hace falta, `finalize`. Un handler a medida es un sink más. `store_results` desaparece.
7. **Contexto explícito**: la fuente recibe `(params, project)`. Sin saco `config` y sin contar parámetros.
8. **Enviar se queda en**: `params`, `after`, `priority`, `executor`, `window` y lo que se quiera pisar de la declaración.

No propongo que el framework trocee por su cuenta (H10): el motor ya agrupa los chunks cortos, y el tamaño del lote de docking es una decisión del dominio.

## 5. Preguntas

1. ¿Decorador y clase, o solo una de las dos formas?
2. Al continuar un job cancelado, ¿qué espera el usuario: «termina lo que lancé» (conjunto congelado) o «haz todo lo que cumpla ahora» (como hoy)?
3. ¿`reads`/`writes` por tabla es suficiente para las dependencias, o hace falta más detalle (por ejemplo, por set de ligandos)?
4. ¿`setup` y `stage_chunk` se quedan? AMDockVS no los usa; tienen sentido para HPC (preparar ficheros antes de enviar).

## 6. Discusión del 2026-10-08

Mario explica el modelo que había detrás: el **job** es el trabajo global y su estado; la **task** es la base funcional (correr un docking); el **chunk** es la misma task aplicada a un lote, de 1 en local y de 1.000 en HPC. Job y task tienen progreso y log propios, y el de las tasks tributa al del job.

Esto corrige dos puntos de arriba:

- **Propuesta 1 y H1.** Task y job no son lo mismo y la task no se elimina. Lo que sobra es que `TaskDefinition` repita la configuración del job.
- **H10 y la nota final de la sección 4.** Si el tamaño del lote depende del ejecutor, no es una decisión del dominio y no debe escribirse en cada chunker.

**Dónde no coincide el código con ese modelo.** El modelo tiene tres niveles (job, lote, elemento) y el código da nombre a dos. La «task» de hoy recibe el lote entero y lo recorre a mano, así que cada una reescribe tres cosas: el troceo (en el chunker), el progreso dentro del lote (`index / len(rows) * 100`, 8 sitios en AMDockVS) y qué hacer cuando falla un elemento (contarlo y seguir, con un `print`). El docking normal, además, no informa de progreso: su `run_chunk` no recibe el callback.

**Reparto propuesto.**

| Nivel | Qué es | Qué declara |
|---|---|---|
| Task | la función sobre un elemento (o sobre un lote, si la herramienta es por lotes) | lo que cuesta un elemento: `cpu`, `gpu`, `mem_mb`, `timeout`, `retries`; si informa de progreso |
| Job | una task aplicada a una fuente de elementos | fuente, destino de resultados, qué lee y qué escribe |
| Envío | una ejecución del job | parámetros, ejecutor, dependencias |
| Lote (chunk) | cuántos elementos viajan juntos | nada: lo decide el ejecutor (local: el motor, que ya agrupa; HPC: N por tarea) |

Progreso del job = (elementos hechos + fracción de los que están corriendo) / total. El elemento es lo que el autor del job decida seguir: un par en un docking normal, un shard en HTP.

Pendiente de construir para esto: en el motor nuevo no hay canal de progreso del worker al motor. El motor actual usa una cola de un proceso `Manager`.

**Conjunto de entrada: ni congelar ni guardar payloads.** Fuente viva con tres anclas baratas (medido con 1 millón de filas en SQLite):

| Ancla | Para qué | Coste |
|---|---|---|
| La tabla de resultados | saber qué está hecho (la fuente lo excluye) | nada nuevo |
| `count(*)` del filtro al arrancar | total para el progreso; el final de la fuente lo corrige | 37 ms |
| `max(id)` de cada tabla de origen al arrancar | que el job no crezca con filas nuevas | 0,03 ms y un entero por tabla |

La lista de miembros queda como último recurso: 0,22 s y 10 bytes por elemento. El `max(id)` no cubre un elemento que ya existía y pasa a cumplir el filtro después (un ligando preparado más tarde).

### Segunda ronda (2026-10-08)

Mario acepta la task sobre un elemento con el lote decidido por el ejecutor. AMDockVS se adapta después: sirve de referencia, no de oráculo.

**Progreso: es un estado, no un evento** (propuesta, sin construir). Nadie necesita todas las actualizaciones, solo la última, así que no va por ninguna cola:

- Un fichero mapeado en memoria con una casilla (un `float`) por tarea en vuelo. El motor asigna la casilla al lanzar; el worker escribe en ella; el motor la lee solo cuando alguien pide el estado del job.
- Medido con loky: 217 ns por actualización, frente a 24 µs por la cola del `Manager` actual. Leer las casillas cuesta 232 ns. No pasa por la cola de eventos ni por el hilo que espera a los workers, así que no puede saturarlos.
- Lote de un elemento: la task escribe su fracción con `report(0.4)`, una función del paquete, sin parámetro extra en la firma. Lote de varios: el bucle del worker escribe solo «hechos / tamaño del lote»; la task no hace nada.
- El motor dice a la task si alguien quiere el progreso fino. Regla de Mario: sí cuando el job tiene pocos elementos (hasta 2 × workers), no por encima. Sirve para que la task no pague la vigilancia de la salida de su herramienta cuando es irrelevante.
- Vigilar la salida de una herramienta (Vina) para sacar el progreso es cosa de la task. `ms_flow` puede dar un ayudante pequeño que lea la salida de un subproceso línea a línea; no se construye hasta que haya quien lo use.
- HPC: las casillas no cruzan máquinas. Allí el progreso es el de los elementos terminados dentro de cada lote, leído al sondear. Queda con HPC.

**Reutilizar entre elementos (lo que hoy justifica el lote de docking).** Hoy las cachés de Vina (`vina_cache`, `ad4_map_cache`) son diccionarios locales del chunk: viven lo que dura un lote. Con una caché por worker (un `lru_cache` a nivel de módulo; los workers de loky se reutilizan) la reutilización cruza lotes y el tamaño del lote deja de importar. Límite: cada worker carga cada receptor una vez (14 cargas, no 1), porque loky no deja elegir worker; la afinidad llegaría con el pool propio. Lo equivalente al `put` de Ray, datos preparados una vez para todos, es ya una ruta a un fichero en el payload.

### Tercera ronda (2026-10-08)

**Casillas de progreso con varios workers.** Cada worker escribe solo su casilla (8 bytes), así que dos nunca tocan los mismos bytes y no hace falta candado. Medido: 8 workers, 71 millones de escrituras, 8,9 millones de lecturas del padre, ningún valor a medias. El motor pone la casilla a cero al reasignarla y recorta lo leído a [0, 1].

**Tope del conjunto de entrada.** El `max(id)` por tabla se guarda con el job (un entero por tabla), así que al reanudar es el mismo: las filas nuevas no entran ni durante la ejecución ni al continuar. Total al reanudar = hechos + pendientes. Lo que el tope no fija: una fila que ya existía y cambia de estado durante la ejecución (se prepara, se borra). Con dependencias declaradas eso no ocurre dentro de un pipeline; si ocurre, el total se corrige al agotarse la fuente y queda a la vista (declarado frente a final). Fijarlo del todo exige la lista de miembros; no se construye salvo que haga falta.

**HPC** (acordado por Mario). Nunca progreso a nivel de task: solo elementos terminados dentro del lote, leídos en el sondeo, con intervalo configurable por ejecutor. Se construye con HPC.

**Una forma de declarar** (propuesta). Las 9 clases `JobSpec` de AMDockVS son atributos más dos `staticmethod`: no tienen instancia ni estado. La herencia se usa una vez (`_PrepareEntitiesJobSpec`, dos hijas). Clase y decorador compilan al mismo `JobDefinition`; mantener las dos obliga a añadir cada campo en tres sitios. Con el reparto nuevo: `@task` sobre la función y el job como una llamada (`task` + fuente + destino). Compartir configuración entre jobs hermanos es pasar la misma task o los mismos argumentos.

**Dependencias por tablas** (propuesta). Hoy `runtime.py` de AMDockVS clasifica por trozos del nombre (`"docking"`, `"prepare_"`, `"load_"`…) y espera por categorías. `amdock_interaction_job`, `amdock_diagram_job` y `amdock_receptor_prediction_job` no casan con ninguna regla y no esperan a nadie. Regla nueva: un job espera a todo job activo que escribe una tabla que él lee. Lo que escribe sale del destino declarado; lo que lee, de la fuente. Sigue siendo por tabla entera, igual de gruesa que hoy.

**`setup` y `stage_chunk`: qué son.** Los dos corren en el proceso padre, en el pool de staging. `setup(payload, context)` una vez por job antes del primer chunk; devuelve `setup_data`, que reciben cada `stage_chunk` y `finalize`. `stage_chunk(payload, context)` una vez por chunk antes de despacharlo; devuelve el payload que verá el worker. Usos reales: ninguno (solo tests de juguete). Lo que sí se usa es el staging por defecto: si el payload trae un `FileInputSpec`, se materializa para el ejecutor; AMDockVS lo usa en 9 módulos vía `worker_file`. Propuesta: no llevar los dos ganchos a la API nueva (preparar una vez es un job previo o la primera línea de la fuente, que ya corre en el padre) y dejar la materialización de entradas como trabajo del ejecutor.
