# refactor_optimization

Refactor del motor de ms_flow: el banco de pruebas, lo que midió cada paso y los documentos del plan. Todo vive aquí y no en `docs/`, porque el sitio publica cualquier `.md` que haya en `docs/`.

| Fichero | Qué es |
| --- | --- |
| [docs/01-auditoria.md](docs/01-auditoria.md) | Auditoría del 2026-10-07: hallazgos, plan por fases y tareas (F0-1 a F5-8) |
| [docs/02-diseno-motor.md](docs/02-diseno-motor.md) | Diseño del motor: componentes, cambios en orden y decisiones abiertas |
| [docs/03-impacto.html](docs/03-impacto.html) | Qué movió cada paso: gráficas, tablas y comprobaciones. Lo genera `report.py` |
| [docs/04-auditoria-tasks-jobs.md](docs/04-auditoria-tasks-jobs.md) | Auditoría del diseño de tasks y jobs contra el uso real en AMDockVS |
| [docs/05-diseno-tasks-jobs.md](docs/05-diseno-tasks-jobs.md) | Diseño de tasks y jobs sobre el motor nuevo, estado de cada pieza y restos del motor viejo |
| [docs/06-diseno-ejecutor-remoto.md](docs/06-diseno-ejecutor-remoto.md) | Propuesta del ejecutor remoto: Ray dentro del job del HPC, runner de `ms_flow` y decisiones abiertas |
| `bench.py` | Mide el coste de orquestación y las comprobaciones de robustez; escribe `results/<paso>.json` |
| `report.py` | Lee `results/*.json` y escribe el informe |
| `workers.py` | Funciones de chunk del banco de pruebas |

## Un cambio, una rama, un PR

1. Rama desde `main` con el identificador de la tarea: `refactor/f0-1-pool-reset`.
2. El cambio, con su test.
3. Medir en la rama, con la máquina sin otra carga (tarda unos minutos):

   ```bash
   python -m benchmarks.refactor_optimization.bench --label F0-1 2>/dev/null
   ```

4. Añadir la línea del paso a `STEPS` en `report.py` (número de PR, qué cambia, `True` mientras esté abierto) y regenerar:

   ```bash
   python -m benchmarks.refactor_optimization.report
   ```

5. El PR lleva el código, `results/F0-1.json` y el informe regenerado. La descripción copia la fila del paso de la tabla "Which step moved what".
6. Al fusionar, el `True` del paso pasa a `False` en el PR siguiente.

El informe no rechaza nada: enseña el efecto. Un paso que sube un coste aparece en "What needs a look" con su explicación.

Si cambia la API del motor, en `bench.py` solo se toca la sección "Engine adapter". Las medidas no se cambian, para que las columnas sigan siendo comparables; una medida nueva se añade y toma como base el paso en que aparece.

Para comparar un paso del motor nuevo con el anterior valen las tres medidas "long" (trabajos de 200.000 chunks, que duran segundos y varían un 5 % entre repeticiones). Las de 2.000 y 20.000 chunks duran decenas de milisegundos en el motor nuevo y varían hasta un 60 %: sirven contra el motor viejo, no entre pasos. Aun así, dos sesiones distintas difieren hasta un 10 %; una diferencia menor se confirma midiendo los dos commits alternados. Las medidas "long" de los pasos anteriores a "resume count" se tomaron el 2026-10-08 desde el commit de cada paso (`long_jobs` en su JSON).
