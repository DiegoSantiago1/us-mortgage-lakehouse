# Decisões — us-mortgage-lakehouse

Cada decisão com o motivo e a alternativa descartada. "Escolha do Diego" = respondida por ele no grill de 06/10/2026. "**Proposta**" = recomendação minha que ainda precisa da validação dele.

**D1. Tema: HMDA dos EUA (financiamento imobiliário).** Escolha do Diego. É o maior dado público de crédito linha a linha que existe: ~10 mi pedidos por ano, por estado, condado e região metropolitana. Ele também tem um problema de engenharia real: o mesmo ano é republicado em várias versões, sem chave. A ligação com o trabalho do Diego é direta: na concessionária ele vê financiamento de veículo aprovado e recusado. Descartados:
- imóveis do Reino Unido (seria o 3º projeto britânico);
- CNPJ da Receita (ele preferiu imóveis);
- imóveis do Brasil (não há base nacional aberta de transações, só ITBI de algumas prefeituras, pequeno demais para Spark);
- França (DVF).

**D2. PySpark + Delta Lake local, em Docker; Databricks Free só como demonstração final.** Escolha do Diego. Spark e lakehouse são a lacuna do portfólio para vagas de engenharia de dados. Docker porque o PC não tem Java e o Python 3.14 provavelmente não é suportado pelo PySpark. Databricks não é a base porque o plano gratuito tem cota diária (estourou, desliga), é só serverless, restringe a internet a domínios aprovados e não documenta o armazenamento: um projeto que depende dele pode parar de funcionar.

**D3. Sem Airflow: comandos por etapa + GitHub Actions.** Escolha do Diego. Um orquestrador para um pipeline que roda poucas vezes por ano (quando sai versão nova) é overengineering, e seria a terceira tecnologia nova ao mesmo tempo. Cada etapa é um comando idempotente; o CI roda os testes.

**D4. Análise de equidade: sim, com cuidado.** Escolha do Diego. O HMDA existe justamente para isso (a lei foi criada para fiscalizar crédito justo). Mas o dado público não tem score de crédito. Por isso:
- a comparação é feita dentro de células de mesma renda, DTI, LTV, valor, tipo de empréstimo e estado;
- só é publicado grupo com n mínimo;
- a página diz explicitamente que diferença não prova discriminação.

Descartado: taxa de negativa bruta por raça, que é enganosa.

**D5. Código em português; página e README bilíngues, com glossário.** Escolha do Diego. Mantém o padrão dos Projetos 1 a 5 e o Diego estuda melhor em português. Para o avaliador estrangeiro: README em inglês e português, página com botão EN/PT e um glossário PT→EN das tabelas e colunas no README em inglês. Descartado: código em inglês (minha recomendação inicial, pelo peso das vagas de Londres) e "traduzir depois" (renomear tudo no fim custaria 4–6 h, com risco de quebrar).

**D6. Dado bruto fora do OneDrive (`C:\dados\hmda`); WSL com 10 GB.** Escolha do Diego. O OneDrive tentaria sincronizar dezenas de GB. O bruto pode ser baixado de novo, então não precisa de backup; o que importa (código, manifesto, decisões) fica no OneDrive e no git. O caminho fica no `.env`. O `.wslconfig` com 10 GB evita que o Spark caia por falta de memória e deixa 6 GB para o Windows.

**D7. Se faltar disco, as versões têm prioridade sobre a série histórica.** Escolha do Diego. O CDC entre versões é o diferencial raro do projeto. A série histórica fica com uma versão por ano (a mais madura).

**D8. Começar em 2018. Proposta.** A regra do HMDA de 2018 mudou os campos: DTI, LTV, juros e motivo de negativa passam a vir padronizados. 2017 tem outro formato. Incluir 2017 dobraria a silver para ganhar um ano sem as colunas mais importantes.

**D9. Diferença entre versões por hash da linha com multiplicidade, não por `MERGE` com chave.** O dado público não tem o ULI (removido por privacidade), então não existe chave do empréstimo. Linhas idênticas podem repetir legitimamente, por isso o hash sozinho não basta: compara-se a contagem de cada hash entre as versões. Limitação assumida e dita na página: uma correção aparece como "saiu uma, entrou outra", e o projeto não afirma qual linha virou qual. Descartados:
- chave sintética com várias colunas (não é única e daria falsos "mesmo empréstimo");
- `DISTINCT` antes de comparar (apagaria pedidos reais).

**D10. Separar a versão do governo da versão da tabela Delta. Proposta.** A versão do governo (Snapshot/One Year/Three Year) é dado e fica na coluna `versao`. O commit do Delta é o histórico da nossa carga. A tabela `silver.vigente` recebe as versões do governo em ordem, um commit por troca, e o time travel responde "o que se via antes da versão final". Misturar as duas noções é o erro mais provável de explicar mal em entrevista.

**D11. Bronze guarda tudo como texto, mais metadados. Proposta.** Nenhuma linha se perde por erro de tipo na entrada, o hash é calculado sobre o texto original e a tipagem acontece uma vez só, na silver, onde cada valor especial (`Exempt`, `NA`, códigos de "não se aplica") é tratado de propósito. É o mesmo princípio do bruto intocado do Projeto 4.

**D12. Conferência contra os números oficiais é bloqueante. Proposta.** Se o total por ano, versão e resultado não bater com o publicado pelo FFIEC, a gold não é publicada e o exportador se recusa, como as checagens `dq` do Projeto 3. A fonte exata do número oficial (API do Data Browser ou relatório) é conferida na fase 0.

**D13. Só o Projeto 6 até terminar.** Escolha do Diego. Minha recomendação era intercalar ~2 h por semana de sessões de explicação dos Projetos 2 a 5, porque o REC'n'Play é em novembro e hoje ele ainda não sabe explicar todas as decisões desses projetos. Risco registrado (R7 no PLAN). Para não acumular a mesma dívida aqui, cada fase deste projeto termina com a sua seção de aprendizado.

**D14. Repositório `us-mortgage-lakehouse`.** Escolha do Diego. Diz o dado e a arquitetura. Criar só com pedido explícito; público só com pedido explícito. Descartados: `hmda-lakehouse` (sigla que só o setor conhece) e `us-mortgage-pipeline` (esconde o Delta Lake).

**D15. Imagem Docker própria com versões fixadas. Proposta.** Parte de uma imagem oficial do Python e instala Java, PySpark e delta-spark com versões fixas no Dockerfile. Assim o Diego entende cada camada e a combinação não muda sozinha. As versões compatíveis são conferidas na documentação oficial do Delta na fase 0. O CI usa a mesma imagem, para teste local e CI rodarem igual.

**D16. Tabelas Delta por caminho, sem metastore. Proposta.** Localmente, um metastore (Hive/Derby) é mais uma peça para manter sem ganho real. Um módulo mapeia nome → caminho. No Databricks (fase 7), o mesmo nome vira tabela do Unity Catalog.

**D17. Comparar uma vez com DuckDB, para responder "por que Spark?". Proposta, o Diego decide.** Com ~10 mi linhas por ano, DuckDB ou Polars dariam conta numa máquina, e um entrevistador experiente vai perguntar isso. A resposta honesta: o Spark é o padrão de lakehouse e o código migra para cluster/Databricks sem reescrever. Uma medição da mesma agregação nas duas ferramentas (~1 h, dependência só no script de medição) transforma essa resposta em número.

**D18. Mapa da página com d3-geo + us-atlas. Proposta, decidir na fase 5.** A geometria dos estados e condados (Census, domínio público) é carregada de CDN permitido. Alternativa mais leve: SVG dos estados pronto, sem condados.

---

## Decisões da fase 0 (06/10/2026)

O Diego aprovou todas as propostas (D8, D10–D12, D15–D18) em 06/10/2026 e autorizou seguir sem pedir confirmação a cada passo, explicando depois.

**D19. Versões fixas da imagem: Python 3.13, Java 21, PySpark 4.2.0, delta-spark 4.4.1.** Conferidas no PyPI e no Maven Central em 06/10/2026: o delta-spark 4.4.1 aceita PySpark de 4.0.1 a 4.2.0, e o Spark 4 pede Java 17 ou 21. Python 3.13 em vez de 3.14 (o do PC) porque é a versão mais nova com todas as dependências maduras; dentro do container a versão do PC não importa.

**D20. Jars do Delta embutidos na imagem; o Spark roda sem internet.** O jeito padrão (`configure_spark_with_delta_pip`) resolve os jars no Maven a cada execução, e o Ivy consulta a internet mesmo com cache (medido: falha com `--network none`). No build, os jars são resolvidos uma vez, e só os **11 que o PySpark ainda não tem** vão para `/opt/delta-jars` (os outros 22, como Hadoop, Parquet e Jackson, duplicariam classes do Spark). Testado: ler e escrever Delta com `--network none`. O certificado do antivírus entra só como segredo do build e não fica em nenhuma camada (conferido).

**D21. O download guarda o ETag junto do pedaço parcial.** Bug achado por teste antes de chegar ao dado real: se a conexão cai e o FFIEC troca o arquivo antes da retomada, o `Range` juntava o começo do arquivo antigo com o fim do novo. Quando os dois tinham o mesmo tamanho, o zip corrompido passava sem erro. Agora o pedaço antigo é descartado se o ETag mudou.

**D22. O bruto é o zip, guardado com o hash no nome; o CSV extraído é transitório.** O FFIEC republica arquivos: o Snapshot de 2022, congelado em maio/2023, está no servidor com data de novembro/2025. Guardar o zip (1,5 GB contra 10 GB do CSV) permite reconstruir a bronze sem depender do que o servidor tem hoje, o mesmo princípio do bruto intocado do Projeto 4. Se o arquivo for republicado, o antigo fica, e o manifesto guarda o histórico.

**D23. Ignorar o lixo do macOS dentro do zip.** O zip do FFIEC traz `__MACOSX/._<arquivo>.csv` (233 bytes, metadado do Finder). A leitura aceita exatamente um `.csv` fora de `__MACOSX/`; qualquer outra coisa é erro.

**D24. A conferência oficial só existe para a versão vigente de cada ano.** A API do Data Browser usa "the latest available static dataset" de cada ano (documentação oficial). Ela confere a versão mais madura (ex.: 2021 = Three Year), mas não o Snapshot nem o One Year antigos. Para essas versões, a checagem é interna: contagens do arquivo e consistência entre camadas. O README diz isso.

**D25. Tabelas Delta no volume Docker; zips e manifesto na pasta do Windows.** Medido com 2021 Three Year (26,3 mi linhas): gravar o Delta levou 393 s na pasta do Windows montada × 288 s no volume; ler e agregar, 6,1–6,4 s × 2,6–3,0 s. O volume é 2 vezes mais rápido na leitura, que é o que se repete nas análises. A divisão segue o que cada coisa é: o **bruto** (zips + manifesto) precisa durar e fica em `C:\dados\hmda`, visível no Windows. As **tabelas** são derivadas, dá para reconstruí-las a partir dos zips, e ficam no volume `lake`. Cuidado registrado: `docker compose down -v` apaga o volume. Não perde dado, mas exige recarregar tudo (~2 h).

**D26. Escopo de carga: 12 arquivos, ~225 mi linhas na bronze.** Medido: Delta só com texto = 12% do CSV (1,21 GB para 26,3 mi linhas), zips de 0,6 a 1,5 GB (HEAD em todos). Cabem no disco (55 GB livres):
- **série vigente 2018–2025**, com a versão mais madura de cada ano: Three Year 2018–2022, One Year 2023–2024 e Snapshot 2025, este marcado como preliminar;
- **três versões de 2021 e de 2022** (Snapshot → One Year → Three Year), para as revisões.

Desenho que economiza disco: a **bronze guarda todas as versões** (é onde se comparam as revisões, sobre o texto original). A **silver é só a vigente**, tipada, e recebe as versões em ordem, então as versões antigas tipadas continuam acessíveis por time travel. Não há uma silver por versão: duplicaria ~85 mi linhas sem ganho.

## Decisões das fases 1 a 3 (06/10/2026)

**D27. Cabeçalho fixo: 99 colunas, nome e ordem; diferente = carga recusada.** Medido: o cabeçalho é idêntico em 2018, 2019, 2020 e 2021 (Snapshot e Three Year); só o nome do CSV dentro do zip muda (o de 2018 tem até `__` duplicado). Se um ano vier diferente, a bronze para com a lista do que sobra e do que falta, em vez de adivinhar o mapeamento.

**D28. A silver recebe as versões na ordem em que o FFIEC as congelou.** As datas de congelamento estão no código do site oficial (`freezeDate`). Carregando nessa ordem, cada commit da silver é um retrato do que existia naquela data, e o time travel responde "o que um analista via em maio de 2023". Limite assumido: o retrato só tem o que está no escopo (ex.: não há o Snapshot de 2019).

**D29. Dois níveis de problema na silver: rejeição × alerta.**
- **Rejeição** é o que fere o formato oficial: linha malformada, número ilegível, código fora do domínio, ano diferente do arquivo. Vai para `silver/rejeitados` com o motivo e o texto original.
- **Alerta** é incoerência de negócio, por exemplo motivo de negativa em empréstimo originado. A linha **fica**, porque o número oficial do governo a conta. Rejeitar por incoerência quebraria a conferência oficial e esconderia o problema em vez de medi-lo.

As 9 CHECK constraints do Delta espelham as regras de rejeição, e um teste prova que barram escrita direta na tabela.

**D30. "Qual campo foi corrigido" com hash por coluna combinado por XOR.** A primeira versão calculava, para cada coluna, o hash das outras 98. O resultado eram ~10 mil nós de expressão, e o Java estourou a memória (**OutOfMemoryError com 1 GB, em teste de 3 linhas**). Agora há um hash por coluna (com a posição) e o total por XOR. "A linha sem a coluna X" = total XOR hash(X), porque XOR é a própria inversa: mesmo resultado, ~100 vezes menos expressão.

**D31. O hash da linha não fica guardado na bronze; é calculado na hora das revisões.** Guardar 8–16 bytes de hash aleatório (que não comprime) em ~225 mi linhas somaria 2–4 GB à bronze, para servir só aos 2 anos com várias versões. Calcular na hora custa segundos por versão. A chave é de 96 bits (xxhash64 + murmur3), com uma marca de nulo para `("a", nulo)` não colidir com `(nulo, "a")`. A exatidão é provada contra o `exceptAll` do Spark (diferença exata, sem hash) num estado inteiro, em toda execução.

## Decisões das fases 4 e da carga real da silver (06/10/2026)

**D32. Equidade por razão observado ÷ esperado, com perfil controlado.** Cada pedido de compra decidido (originado, aprovado ou negado) cai numa célula de perfil parecido: estado × tipo de empréstimo × faixa de renda × faixa de DTI × faixa de LTV × faixa de valor. As negativas **esperadas** de um grupo são a soma, nas células em que ele aparece, de (pedidos do grupo × taxa de negativa de todos na célula). Razão 1,30 = 30% mais negativas do que o perfil explica. A página mostra lado a lado a taxa bruta e a ajustada, para o leitor ver quanto o perfil explica. Regras: célula só entra com ≥ 30 pedidos; grupo só é publicado com ≥ 1.000; "Não informado" não é publicado. Descartado: regressão logística, mais difícil de explicar e sem o score de crédito, que é a variável que mais pesa; o ganho de precisão não compensa.

**D33. Silver em três estágios (limpar → tipar → validar), sem persist.** A primeira versão repetia a limpeza ("", NA, Exempt → nulo) dentro de cada regra de valor, rejeição e isenção. O código gerado ficou grande demais, o Spark caiu para o modo interpretado (`CaseWhen.eval` no despejo de threads) e processava **~1.900 linhas/s por núcleo** (medido: 1 milhão de linhas em 550 s por tarefa). Agora cada valor é calculado uma vez, e os estágios seguintes só referenciam colunas prontas. O `persist()` do conjunto inteiro também saiu: com o texto original junto, seriam ~10 GB para 26 milhões de linhas. Os rejeitados só ganham uma segunda passada (com o texto original) quando existem.

**D34. Número legível, mesmo absurdo, não é rejeitado; os códigos 1111 são isenção também nos motivos de negativa.** A primeira carga real de 2018 rejeitou 303.002 linhas (2%). **Todas eram erro meu**, não do dado:
- 302.921 tinham `denial_reason_1 = 1111` (banco isento, mesmo código dos outros campos);
- 14 tinham juros como 260000.0 ou spread como −9999997.0, que não cabiam no `decimal(8,3)`;
- 80 tinham idade 9999 (código do cosolicitante, fora da documentação do solicitante).

O FFIEC publica e **conta** essas linhas: rejeitá-las quebraria a conferência oficial. Regras novas: tipos largos (`decimal(18,x)`); 1111 = isenção nos motivos; número implausível vira **alerta** ("juros implausível", "spread implausível") e as análises filtram; idade 9999 vira nulo com alerta. Lição para entrevista: a tabela de rejeitados com o texto original mostrou o erro em minutos.

**D35. Segunda conferência oficial: registros por banco.** O Transmittal Sheet do FFIEC traz o `lar_count` de cada instituição. A soma bate exatamente com a bronze (26.269.980 em 2021 e 16.125.975 em 2022, Three Year). O `dq` compara banco por banco (~4.400) nos anos com a referência em `docs/referencia/`.

**D36. Nada de funções com lambda no caminho quente da silver.** `exists`, `aggregate` e `array_compact` (funções de ordem superior) rodam sempre **interpretadas** no Spark, sem geração de código. Eu usava `exists` sobre 99 colunas e `array_compact` sobre ~60 motivos em toda linha. Troquei por uma cadeia de `OR` com `eqNullSafe` e por `concat_ws` (que pula nulos), ambos compiláveis. O texto só é dividido em lista (`split`) quando existe. Medido no ano de 2018 (15,1 mi linhas): 803 s → 548 s. Também testei, sem ganho que justificasse: compilar métodos grandes no JIT (`-XX:-DontCompileHugeMethods`, ficou mais lento) e desligar as estatísticas do Delta (−8%, e as estatísticas servem para pular arquivos nas consultas, então ficaram ligadas). O resto do tempo é o custo real de tipar ~60 colunas por linha.

**D37. Campo descritivo fora da documentação vira alerta; códigos de score 11–15 são válidos.** A segunda carga real achou mais dois erros meus:
- 952 mil linhas de 2022, crescendo até 1,03 mi em 2025, com `applicant_credit_score_type` de 11 a 15 (FICO 9, FICO 8, FICO 10, FICO 10T, VantageScore 4.0). A documentação lista esses códigos, e eu os tinha lido como só do cosolicitante;
- 1 linha de 2019 (Ohio, negada) com `total_units = "0"`.

Regra: nos campos **descritivos** (unidades, limite conforme, idade), valor fora da documentação vira **nulo + alerta**, e a linha fica, porque o oficial a conta. Os **códigos centrais** da análise (resultado, finalidade, tipo, garantia, ocupação, motivos) continuam rejeitando: um valor inválido ali quebraria as contas, e a conferência oficial avisaria.

**D38. A lista de estados da consulta oficial inclui FM, MH e PW.** Na primeira rodada do `dq`, 2018, 2020 e 2023 falharam por 1 ou 2 linhas em **FM, MH e PW** (Micronésia, Ilhas Marshall e Palau, Estados Associados aos EUA). Eu não tinha incluído esses códigos na consulta à API. A checagem fez o papel dela: barrou a publicação por **uma** linha. Depois de incluir os três, os 8 anos bateram célula por célula (472 células por ano), e 2021 e 2022 bateram banco por banco.
