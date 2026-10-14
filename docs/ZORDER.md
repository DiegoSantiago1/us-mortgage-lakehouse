# Z-ORDER por estado: medição (gerado por `python -m hmda.zorder`)

Cópia da silver de 2022 (a silver de verdade não é tocada), consulta `SELECT COUNT(*) WHERE estado = 'TX'`, melhor tempo de 3 execuções.

| | Arquivos | Arquivos lidos | Tempo | Linhas |
|---|---|---|---|---|
| Antes (cada arquivo com todos os estados) | 14 | 14 (100%) | 1.16 s | 1,388,354 |
| Depois de `OPTIMIZE ... ZORDER BY (estado)` | 14 | 2 (14%) | 0.54 s | 1,388,354 |

O número de linhas é o mesmo: o Z-ORDER só reorganiza. Depois dele, cada arquivo cobre uma faixa de estados, e o mínimo e o máximo de `estado` que o Delta guarda por arquivo deixam pular os que não podem ter o estado pedido (data skipping). Antes, todo arquivo tinha todos os estados e nada podia ser pulado.

**Por que não foi aplicado na silver:** as análises do projeto agregam o país inteiro (todos os estados), então o ganho não aparece nelas, e o Z-ORDER reescreve todos os arquivos a cada carga. Vale para uma tabela consultada estado a estado.
