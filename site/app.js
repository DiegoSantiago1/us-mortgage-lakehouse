/* Página de resultados do lakehouse do HMDA. Lê dados/resultados.json (gerado por
   python -m hmda.exportar_site) e desenha tudo com D3. Texto de dado entra sempre
   por textContent (nunca innerHTML). */
(() => {
  "use strict";

  // ---------- estado, idioma e tema ----------
  const guardar = (k, v) => { try { localStorage.setItem(k, v); } catch (_) { /* sem storage */ } };
  const ler = (k) => { try { return localStorage.getItem(k); } catch (_) { return null; } };

  let idioma = ler("idioma") || ((navigator.language || "pt").toLowerCase().startsWith("pt") ? "pt" : "en");
  const temaSalvo = ler("tema");
  if (temaSalvo) document.documentElement.dataset.theme = temaSalvo;

  const t = (k) => (window.TEXTOS[idioma] && window.TEXTOS[idioma][k]) || window.TEXTOS.pt[k] || k;
  const rot = (s) => (idioma === "en" && window.ROTULOS_EN[s]) || s;
  const fin = (s) => t("fin." + s);
  const campo = (c) => (idioma === "pt" ? window.CAMPOS_PT[c] || c : c.replaceAll("_", " "));
  const loc = () => (idioma === "pt" ? "pt-BR" : "en-US");
  const num = (v, casas = 0) => Number(v).toLocaleString(loc(), { minimumFractionDigits: casas, maximumFractionDigits: casas });
  const pct = (v, casas = 1) => (v * 100).toLocaleString(loc(), { minimumFractionDigits: casas, maximumFractionDigits: casas }) + "%";
  const mi = (v, casas = 1) => num(v / 1e6, casas) + " " + t("milhoes");
  const dinheiro = (v) => "US$ " + (v >= 1e6 ? num(v / 1e6, 2) + " mi" : num(Math.round(v / 1000)) + " mil")
    .replace(" mi", idioma === "pt" ? " mi" : "M").replace(" mil", idioma === "pt" ? " mil" : "k");
  const cor = (v) => getComputedStyle(document.documentElement).getPropertyValue(v).trim();

  const ESTADOS_FIPS = {
    "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA", "08": "CO", "09": "CT", "10": "DE",
    "11": "DC", "12": "FL", "13": "GA", "15": "HI", "16": "ID", "17": "IL", "18": "IN", "19": "IA",
    "20": "KS", "21": "KY", "22": "LA", "23": "ME", "24": "MD", "25": "MA", "26": "MI", "27": "MN",
    "28": "MS", "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH", "34": "NJ", "35": "NM",
    "36": "NY", "37": "NC", "38": "ND", "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI",
    "45": "SC", "46": "SD", "47": "TN", "48": "TX", "49": "UT", "50": "VT", "51": "VA", "53": "WA",
    "54": "WV", "55": "WI", "56": "WY", "72": "PR"
  };

  // ---------- tooltip ----------
  const tip = document.getElementById("tooltip");
  function mostrarTip(evento, titulo, linhas) {
    tip.replaceChildren();
    const h = document.createElement("div");
    h.className = "t-titulo";
    h.textContent = titulo;
    tip.appendChild(h);
    for (const l of linhas) {
      const d = document.createElement("div");
      d.className = "t-linha";
      if (l.cor) {
        const k = document.createElement("span");
        k.className = "t-chave";
        k.style.background = l.cor;
        d.appendChild(k);
      }
      const b = document.createElement("b");
      b.textContent = l.valor;
      d.appendChild(b);
      const s = document.createElement("span");
      s.textContent = l.rotulo;
      d.appendChild(s);
      tip.appendChild(d);
    }
    tip.hidden = false;
    const x = Math.min(evento.clientX + 14, window.innerWidth - tip.offsetWidth - 8);
    const y = Math.min(evento.clientY + 14, window.innerHeight - tip.offsetHeight - 8);
    tip.style.left = x + "px";
    tip.style.top = y + "px";
  }
  const esconderTip = () => { tip.hidden = true; };

  // ---------- componentes de gráfico ----------
  function svgEm(el, largura, altura) {
    el.replaceChildren();
    return d3.select(el).append("svg").attr("viewBox", `0 0 ${largura} ${altura}`).attr("role", "img");
  }

  /** Linhas com crosshair. series: [{nome, cor, pontos: [{x, y}]}] */
  function linhas(el, series, { fy = (v) => num(v), fx = (v) => String(v), yZero = true, altura = 260 } = {}) {
    const largura = Math.max(320, el.clientWidth || 600);
    const m = { t: 12, r: 110, b: 28, l: 52 };
    const svg = svgEm(el, largura, altura);
    const xs = [...new Set(series.flatMap((s) => s.pontos.map((p) => p.x)))].sort((a, b) => a - b);
    const x = d3.scalePoint().domain(xs).range([m.l, largura - m.r]).padding(0.2);
    const ys = series.flatMap((s) => s.pontos.map((p) => p.y));
    const lo = yZero ? 0 : d3.min(ys) * 0.96;
    const y = d3.scaleLinear().domain([lo, d3.max(ys) * 1.06]).nice().range([altura - m.b, m.t]);
    svg.append("g").attr("class", "grade").selectAll("line").data(y.ticks(5)).join("line")
      .attr("x1", m.l).attr("x2", largura - m.r).attr("y1", (d) => y(d)).attr("y2", (d) => y(d));
    svg.append("g").attr("class", "eixo").attr("transform", `translate(${m.l - 6},0)`)
      .call(d3.axisLeft(y).ticks(5).tickSize(0).tickFormat(fy)).call((g) => g.select(".domain").remove());
    svg.append("g").attr("class", "eixo").attr("transform", `translate(0,${altura - m.b})`)
      .call(d3.axisBottom(x).tickSize(0).tickPadding(8).tickFormat(fx)).call((g) => g.select(".domain").attr("stroke", cor("--eixo")));
    for (const s of series) {
      const pts = s.pontos.filter((p) => p.y != null).sort((a, b) => a.x - b.x);
      svg.append("path").datum(pts).attr("fill", "none").attr("stroke", s.cor).attr("stroke-width", 2)
        .attr("stroke-linejoin", "round").attr("stroke-linecap", "round")
        .attr("d", d3.line().x((p) => x(p.x)).y((p) => y(p.y)));
      const ult = pts[pts.length - 1];
      if (ult) {
        svg.append("circle").attr("cx", x(ult.x)).attr("cy", y(ult.y)).attr("r", 4)
          .attr("fill", s.cor).attr("stroke", cor("--superficie")).attr("stroke-width", 2);
        svg.append("text").attr("class", "rotulo-serie").attr("x", x(ult.x) + 8).attr("y", y(ult.y) + 4).text(s.nome);
      }
    }
    // crosshair: a linha vertical acha o X mais próximo; o tooltip lista todas as séries
    const cruz = svg.append("line").attr("stroke", cor("--eixo")).attr("y1", m.t).attr("y2", altura - m.b).style("opacity", 0);
    svg.append("rect").attr("x", m.l).attr("y", m.t).attr("width", largura - m.l - m.r).attr("height", altura - m.t - m.b)
      .attr("fill", "transparent").attr("tabindex", 0)
      .on("pointermove", (ev) => {
        const [px] = d3.pointer(ev);
        const alvo = xs.reduce((a, b) => (Math.abs(x(b) - px) < Math.abs(x(a) - px) ? b : a));
        cruz.attr("x1", x(alvo)).attr("x2", x(alvo)).style("opacity", 1);
        mostrarTip(ev, fx(alvo), series.map((s) => {
          const p = s.pontos.find((q) => q.x === alvo);
          return { cor: s.cor, valor: p && p.y != null ? fy(p.y) : "—", rotulo: s.nome };
        }));
      })
      .on("pointerleave", () => { cruz.style("opacity", 0); esconderTip(); });
    if (series.length > 1) {
      const leg = document.createElement("div");
      leg.className = "legenda";
      for (const s of series) {
        const sp = document.createElement("span");
        const i = document.createElement("i");
        i.style.background = s.cor;
        sp.append(i, document.createTextNode(s.nome));
        leg.appendChild(sp);
      }
      el.prepend(leg);
    }
    return { svg, x, y };
  }

  /** Barras horizontais. itens: [{rotulo, valor, partes?: [{valor, cor, nome}], dica}] */
  function barras(el, itens, { fv = (v) => num(v), corUnica = cor("--serie-1"), larguraRotulo = 200 } = {}) {
    const largura = Math.max(320, el.clientWidth || 600);
    const passo = 30;
    const m = { t: 4, r: 64, b: 4, l: Math.min(larguraRotulo, largura * 0.42) };
    const altura = m.t + m.b + itens.length * passo;
    const svg = svgEm(el, largura, altura);
    const max = d3.max(itens, (d) => d.valor) || 1;
    const x = d3.scaleLinear().domain([0, max]).range([m.l, largura - m.r]);
    itens.forEach((d, i) => {
      const y0 = m.t + i * passo;
      const g = svg.append("g").attr("tabindex", 0).style("cursor", "default");
      g.append("text").attr("class", "rotulo-barra").attr("x", m.l - 8).attr("y", y0 + passo / 2 + 4)
        .attr("text-anchor", "end").text(d.rotulo.length > 34 ? d.rotulo.slice(0, 33) + "…" : d.rotulo);
      const partes = d.partes || [{ valor: d.valor, cor: corUnica }];
      let acumulado = 0;
      partes.forEach((p, j) => {
        const xa = x(acumulado);
        const w = Math.max(0, x(acumulado + p.valor) - xa - (j < partes.length - 1 ? 2 : 0));
        const ultimo = j === partes.length - 1;
        g.append("path").attr("fill", p.cor).attr("d", barraArredondada(xa, y0 + 7, w, Math.min(16, passo - 12), ultimo ? 4 : 0));
        acumulado += p.valor;
      });
      g.append("text").attr("class", "rotulo-valor").attr("x", x(d.valor) + 6).attr("y", y0 + passo / 2 + 4).text(fv(d.valor));
      g.append("rect").attr("x", 0).attr("y", y0).attr("width", largura).attr("height", passo).attr("fill", "transparent")
        .on("pointermove", (ev) => mostrarTip(ev, d.rotulo, d.dica || [{ valor: fv(d.valor), rotulo: "" }]))
        .on("pointerleave", esconderTip);
    });
  }
  function barraArredondada(x, y, w, h, r) {
    r = Math.min(r, w, h / 2);
    return `M${x},${y}H${x + w - r}Q${x + w},${y} ${x + w},${y + r}V${y + h - r}Q${x + w},${y + h} ${x + w - r},${y + h}H${x}Z`;
  }

  function tabela(el, colunas, linhasT) {
    el.replaceChildren();
    const caixa = document.createElement("div");
    caixa.className = "tabela-rolagem";
    const tb = document.createElement("table");
    const cab = tb.createTHead().insertRow();
    for (const c of colunas) {
      const th = document.createElement("th");
      th.textContent = c.titulo;
      if (c.n) th.className = "n";
      cab.appendChild(th);
    }
    const corpo = tb.createTBody();
    for (const l of linhasT) {
      const tr = corpo.insertRow();
      colunas.forEach((c, i) => {
        const td = tr.insertCell();
        td.textContent = l[i];
        if (c.n) td.className = "n";
      });
    }
    caixa.appendChild(tb);
    el.appendChild(caixa);
  }

  function botoesAno(el, anos, atual, aoMudar) {
    el.replaceChildren();
    for (const a of anos) {
      const b = document.createElement("button");
      b.type = "button";
      b.textContent = a;
      b.setAttribute("aria-pressed", String(a === atual));
      b.addEventListener("click", () => aoMudar(a));
      el.appendChild(b);
    }
  }

  // ---------- capítulos ----------
  let D = null;
  let geo = null;
  const sel = { anoMapa: null, anoEquidade: null, anoRevisao: 2021 };

  function tiles() {
    const el = document.getElementById("tiles");
    el.replaceChildren();
    const linhasBronze = d3.sum(D.cargas, (c) => c.linhas_bronze);
    const malformadas = d3.sum(D.cargas, (c) => c.corrompidas) + d3.sum(D.versoes_silver, (v) => v.rejeitadas);
    const oficiais = D.qualidade.filter((q) => q.checagem === "oficial" || q.checagem === "instituicoes");
    const ok = oficiais.filter((q) => q.passou).length;
    const dados = [
      [mi(linhasBronze, 1), t("tile.registros")],
      [String(new Set(D.cargas.map((c) => c.ano)).size), t("tile.anos")],
      [oficiais.length ? pct(ok / oficiais.length, 0) : "—", t("tile.oficial")],
      [num(malformadas), t("tile.perdidas")]
    ];
    for (const [v, r] of dados) {
      const d = document.createElement("div");
      d.className = "tile";
      const a = document.createElement("div"); a.className = "valor"; a.textContent = v;
      const b = document.createElement("div"); b.className = "rotulo"; b.textContent = r;
      d.append(a, b);
      el.appendChild(d);
    }
  }

  function capVersoes() {
    // Pequenos múltiplos: cada ano com a sua escala (2021 tem 26 mi, 2022 tem 16 mi).
    const el = document.getElementById("linha-tempo");
    el.replaceChildren();
    const ordem = ["snapshot", "one_year", "three_year"];
    const grade = document.createElement("div");
    grade.className = "dupla";
    for (const ano of [2021, 2022]) {
      const caixa = document.createElement("div");
      const titulo = document.createElement("div");
      titulo.className = "legenda";
      titulo.textContent = String(ano);
      const alvo = document.createElement("div");
      alvo.className = "grafico";
      caixa.append(titulo, alvo);
      grade.appendChild(caixa);
      const cargas = D.cargas.filter((c) => c.ano === ano).sort((a, b) => ordem.indexOf(a.versao) - ordem.indexOf(b.versao));
      requestAnimationFrame(() => {
        const pontos = cargas.map((c) => c.linhas_bronze);
        const { svg, x, y } = linhas(alvo, [{ nome: String(ano), cor: cor("--serie-1"), pontos: pontos.map((v, i) => ({ x: i, y: v })) }],
          { fy: (v) => mi(v, 2), fx: (i) => t("versao." + ordem[i]), yZero: false, altura: 200 });
        // Quanto cada versão acrescentou à anterior (rótulo seletivo, só nas mudanças).
        pontos.forEach((v, i) => {
          if (i === 0) return;
          const dif = v - pontos[i - 1];
          svg.append("text").attr("class", "rotulo-valor").attr("text-anchor", "middle")
            .attr("x", x(i)).attr("y", y(v) - 12).text((dif >= 0 ? "+" : "") + num(dif));
        });
      });
    }
    el.appendChild(grade);
  }

  function capPreco() {
    const pr = D.preco_renda;
    const anos = [...new Set(pr.estados.map((r) => r.ano))].sort();
    if (sel.anoMapa == null) sel.anoMapa = anos[anos.length - 1];
    botoesAno(document.getElementById("anos-mapa"), anos, sel.anoMapa, (a) => { sel.anoMapa = a; capPreco(); });
    document.getElementById("ano-mapa-rotulo").textContent = String(sel.anoMapa);

    // mapa coroplético: uma tonalidade, claro -> escuro, escala fixa em todos os anos
    const el = document.getElementById("mapa");
    const largura = 975, altura = 610;
    const svg = svgEm(el, largura, altura);
    const doAno = new Map(pr.estados.filter((r) => r.ano === sel.anoMapa).map((r) => [r.lugar, r]));
    const valores = pr.estados.map((r) => r.preco_renda);
    const limites = d3.quantile(valores.sort(d3.ascending), 0.02);
    const escala = d3.scaleQuantize().domain([limites, d3.max(valores)]).range(["--seq-0", "--seq-1", "--seq-2", "--seq-3", "--seq-4"].map(cor));
    const path = d3.geoPath();
    svg.append("g").selectAll("path").data(topojson.feature(geo, geo.objects.states).features).join("path")
      .attr("class", "estado").attr("d", path).attr("tabindex", 0)
      .attr("fill", (f) => { const r = doAno.get(ESTADOS_FIPS[f.id]); return r ? escala(r.preco_renda) : cor("--grade"); })
      .on("pointermove focus", (ev, f) => {
        const uf = ESTADOS_FIPS[f.id];
        const r = doAno.get(uf);
        if (!r) return;
        const e = ev.type === "focus" ? { clientX: ev.target.getBoundingClientRect().x, clientY: ev.target.getBoundingClientRect().y } : ev;
        mostrarTip(e, `${f.properties.name} · ${sel.anoMapa}`, [
          { valor: num(r.preco_renda, 2) + "x", rotulo: t("tt.razao") },
          { valor: dinheiro(r.valor_imovel_mediano), rotulo: t("tt.valor") },
          { valor: dinheiro(r.renda_mediana), rotulo: t("tt.renda") },
          { valor: num(r.compras), rotulo: t("tt.compras") }
        ]);
      })
      .on("pointerleave blur", esconderTip);
    svg.append("path").datum(topojson.mesh(geo, geo.objects.states, (a, b) => a !== b))
      .attr("fill", "none").attr("stroke", cor("--superficie")).attr("stroke-width", 1).attr("d", path);
    const leg = document.getElementById("legenda-mapa");
    leg.replaceChildren();
    const [d0, d1] = escala.domain();
    const a = document.createElement("span"); a.textContent = num(d0, 1) + "x";
    const barra = document.createElement("span"); barra.className = "barra";
    barra.style.background = `linear-gradient(90deg, ${escala.range().join(",")})`;
    const b = document.createElement("span"); b.textContent = num(d1, 1) + "x";
    leg.append(a, barra, b);

    linhas(document.getElementById("linha-preco"),
      [{ nome: "US", cor: cor("--serie-1"), pontos: pr.nacional.map((r) => ({ x: r.ano, y: r.preco_renda })) }],
      { fy: (v) => num(v, 1) + "x", yZero: false });

    const ultimoMsa = d3.max(pr.msa, (r) => r.ano);
    const topo = pr.msa.filter((r) => r.ano === ultimoMsa && r.compras >= 1000)
      .sort((x, y) => y.preco_renda - x.preco_renda).slice(0, 12);
    barras(document.getElementById("barras-msa"), topo.map((r) => ({
      rotulo: `${r.nome}, ${r.uf}`, valor: r.preco_renda,
      dica: [{ valor: num(r.preco_renda, 2) + "x", rotulo: t("tt.razao") }, { valor: dinheiro(r.valor_imovel_mediano), rotulo: t("tt.valor") },
        { valor: dinheiro(r.renda_mediana), rotulo: t("tt.renda") }, { valor: num(r.compras), rotulo: t("tt.compras") }]
    })), { fv: (v) => num(v, 1) + "x", larguraRotulo: 260 });

    tabela(document.getElementById("tabela-estados"),
      [{ titulo: t("tab.estado") }, { titulo: t("tab.razao"), n: 1 }, { titulo: t("tab.valor"), n: 1 }, { titulo: t("tab.renda"), n: 1 }, { titulo: t("tab.compras"), n: 1 }],
      [...doAno.values()].sort((x, y) => y.preco_renda - x.preco_renda)
        .map((r) => [r.lugar, num(r.preco_renda, 2), dinheiro(r.valor_imovel_mediano), dinheiro(r.renda_mediana), num(r.compras)]));
  }

  const FINALIDADES = [["compra", "--serie-1"], ["refinanciamento", "--serie-2"], ["refinanciamento com saque", "--serie-3"]];

  function seriesPor(linhasDado, campoValor) {
    return FINALIDADES.map(([f, c]) => ({
      nome: fin(f), cor: cor(c),
      pontos: linhasDado.filter((r) => r.nivel === "nacional" && r.finalidade_nome === f).map((r) => ({ x: r.ano, y: r[campoValor] }))
    }));
  }

  function capJuros() {
    linhas(document.getElementById("linha-volume"), seriesPor(D.mercado, "originacoes"), { fy: (v) => num(v / 1e6, 1) });
    linhas(document.getElementById("linha-juros"), seriesPor(D.mercado, "juros_mediano"), { fy: (v) => num(v, 2) + "%", yZero: false });
  }

  function capNegativas() {
    linhas(document.getElementById("linha-negativa"), seriesPor(D.negativas, "taxa_negativa"), { fy: (v) => pct(v, 0) });
    const ultimo = d3.max(D.motivos, (r) => r.ano);
    document.getElementById("ano-motivos").textContent = String(ultimo);
    const itens = D.motivos.filter((r) => r.ano === ultimo && r.finalidade_nome === "compra")
      .sort((a, b) => b.pct_dos_negados - a.pct_dos_negados);
    barras(document.getElementById("barras-motivos"), itens.map((r) => ({
      rotulo: rot(r.motivo_nome), valor: r.pct_dos_negados,
      dica: [{ valor: pct(r.pct_dos_negados), rotulo: "" }, { valor: num(r.citacoes), rotulo: t("tt.negados") }]
    })), { fv: (v) => pct(v, 0), larguraRotulo: 220 });
  }

  function capEquidade() {
    const anos = [...new Set(D.equidade.map((r) => r.ano))].sort();
    if (sel.anoEquidade == null) sel.anoEquidade = anos[anos.length - 1];
    botoesAno(document.getElementById("anos-equidade"), anos, sel.anoEquidade, (a) => { sel.anoEquidade = a; capEquidade(); });
    const linhasAno = D.equidade.filter((r) => r.ano === sel.anoEquidade)
      .sort((a, b) => (a.dimensao === b.dimensao ? b.razao_obs_esp - a.razao_obs_esp : a.dimensao.localeCompare(b.dimensao)));

    // Gráfico de pontos: razão com controle (cheio) e sem controle (vazado). Divergente em torno de 1.
    const el = document.getElementById("barras-equidade");
    const largura = Math.max(320, el.clientWidth || 600);
    const passo = 30, m = { t: 26, r: 24, b: 28, l: Math.min(250, largura * 0.42) };
    const altura = m.t + m.b + linhasAno.length * passo;
    const svg = svgEm(el, largura, altura);
    const bruta = (r) => r.taxa_bruta / r.taxa_geral;
    const ext = d3.extent(linhasAno.flatMap((r) => [r.razao_obs_esp, bruta(r), 1]));
    const x = d3.scaleLinear().domain([Math.min(0.5, ext[0]), Math.max(1.5, ext[1])]).nice().range([m.l, largura - m.r]);
    svg.append("g").attr("class", "grade").selectAll("line").data(x.ticks(6)).join("line")
      .attr("x1", (d) => x(d)).attr("x2", (d) => x(d)).attr("y1", m.t).attr("y2", altura - m.b);
    svg.append("g").attr("class", "eixo").attr("transform", `translate(0,${altura - m.b})`)
      .call(d3.axisBottom(x).ticks(6).tickSize(0).tickPadding(8).tickFormat((v) => num(v, 1))).call((g) => g.select(".domain").remove());
    svg.append("line").attr("x1", x(1)).attr("x2", x(1)).attr("y1", m.t - 6).attr("y2", altura - m.b).attr("stroke", cor("--tinta-2")).attr("stroke-width", 1.5);
    svg.append("text").attr("class", "rotulo-dado").attr("x", x(1) + 4).attr("y", m.t - 10).text("1,0".replace(",", idioma === "pt" ? "," : "."));
    const corDe = (v) => (v > 1.02 ? cor("--div-alto") : v < 0.98 ? cor("--div-baixo") : cor("--mudo"));
    linhasAno.forEach((r, i) => {
      const yc = m.t + i * passo + passo / 2;
      const g = svg.append("g").attr("tabindex", 0);
      g.append("text").attr("class", "rotulo-barra").attr("x", m.l - 10).attr("y", yc + 4).attr("text-anchor", "end").text(rot(r.grupo));
      g.append("line").attr("x1", x(bruta(r))).attr("x2", x(r.razao_obs_esp)).attr("y1", yc).attr("y2", yc).attr("stroke", cor("--eixo")).attr("stroke-width", 2);
      g.append("circle").attr("cx", x(bruta(r))).attr("cy", yc).attr("r", 5).attr("fill", cor("--superficie")).attr("stroke", cor("--mudo")).attr("stroke-width", 2);
      g.append("circle").attr("cx", x(r.razao_obs_esp)).attr("cy", yc).attr("r", 6).attr("fill", corDe(r.razao_obs_esp)).attr("stroke", cor("--superficie")).attr("stroke-width", 2);
      g.append("rect").attr("x", 0).attr("y", yc - passo / 2).attr("width", largura).attr("height", passo).attr("fill", "transparent")
        .on("pointermove", (ev) => mostrarTip(ev, `${rot(r.grupo)} · ${sel.anoEquidade}`, [
          { valor: num(r.razao_obs_esp, 2), rotulo: t("tab.razao_oe") },
          { valor: pct(r.taxa_bruta), rotulo: t("tab.bruta") },
          { valor: pct(r.taxa_ajustada), rotulo: t("tab.ajustada") },
          { valor: num(r.pedidos), rotulo: t("tt.pedidos") }
        ]))
        .on("pointerleave", esconderTip);
    });
    const leg = document.createElement("div");
    leg.className = "legenda";
    const itensLeg = idioma === "pt"
      ? [["vazado", "sem controlar o perfil"], ["cheio", "com o perfil controlado"]]
      : [["vazado", "without profile controls"], ["cheio", "with profile controls"]];
    for (const [tipo, texto] of itensLeg) {
      const s = document.createElement("span");
      const i = document.createElement("i");
      i.style.cssText = tipo === "vazado"
        ? `width:10px;height:10px;border-radius:50%;border:2px solid ${cor("--mudo")};background:transparent`
        : `width:12px;height:12px;border-radius:50%;background:${cor("--div-alto")}`;
      s.append(i, document.createTextNode(texto));
      leg.appendChild(s);
    }
    el.prepend(leg);

    tabela(document.getElementById("tabela-equidade"),
      [{ titulo: t("tab.grupo") }, { titulo: t("tab.pedidos"), n: 1 }, { titulo: t("tab.bruta"), n: 1 }, { titulo: t("tab.ajustada"), n: 1 }, { titulo: t("tab.razao_oe"), n: 1 }],
      linhasAno.map((r) => [rot(r.grupo), num(r.pedidos), pct(r.taxa_bruta), pct(r.taxa_ajustada), num(r.razao_obs_esp, 2)]));
  }

  function capRevisoes() {
    const rev = D.revisoes;
    const st = rev.resumo.find((r) => r.ano === 2021 && r.de_versao === "snapshot" && r.para_versao === "three_year");
    const inst = rev.instituicoes.find((r) => r.ano === 2021);
    const campoTop = rev.campos.filter((c) => c.ano === 2021 && c.de_versao === "snapshot" && c.para_versao === "three_year")
      .sort((a, b) => b.pares - a.pares)[0];
    const el = document.getElementById("tiles-revisoes");
    el.replaceChildren();
    const dados = [];
    if (st) dados.push([num(st.entraram), t("rev.entraram")], [num(st.sairam), t("rev.sairam")]);
    if (inst) dados.push([pct(inst.instituicoes_que_mudaram / inst.instituicoes, 0), t("rev.bancos")]);
    if (campoTop) dados.push([campo(campoTop.campo), num(campoTop.pares) + " " + t("rev.campo")]);
    for (const [v, r] of dados) {
      const d = document.createElement("div");
      d.className = "tile";
      const a = document.createElement("div"); a.className = "valor"; a.textContent = v;
      const b = document.createElement("div"); b.className = "rotulo"; b.textContent = r;
      d.append(a, b);
      el.appendChild(d);
    }

    botoesAno(document.getElementById("anos-revisao"), [2021, 2022], sel.anoRevisao, (a) => { sel.anoRevisao = a; capRevisoes(); });
    const top = rev.top_instituicoes.filter((r) => r.ano === sel.anoRevisao).sort((a, b) => b.mexidas - a.mexidas).slice(0, 10);
    barras(document.getElementById("barras-bancos"), top.map((r) => ({
      rotulo: r.nome, valor: r.mexidas,
      partes: [{ valor: r.entraram, cor: cor("--serie-1") }, { valor: r.sairam, cor: cor("--serie-2") }],
      dica: [{ cor: cor("--serie-1"), valor: num(r.entraram), rotulo: t("tt.entraram") }, { cor: cor("--serie-2"), valor: num(r.sairam), rotulo: t("tt.sairam") }]
    })), { fv: (v) => num(v), larguraRotulo: 240 });
    const legB = document.createElement("div");
    legB.className = "legenda";
    for (const [c, k] of [["--serie-1", "tt.entraram"], ["--serie-2", "tt.sairam"]]) {
      const s = document.createElement("span");
      const i = document.createElement("i");
      i.style.cssText = `width:12px;height:10px;border-radius:2px;background:${cor(c)}`;
      s.append(i, document.createTextNode(t(k)));
      legB.appendChild(s);
    }
    document.getElementById("barras-bancos").prepend(legB);

    const campos = rev.campos.filter((c) => c.ano === sel.anoRevisao && c.de_versao === "snapshot" && c.para_versao === "three_year")
      .sort((a, b) => b.pares - a.pares).slice(0, 10);
    barras(document.getElementById("barras-campos"), campos.map((c) => ({ rotulo: campo(c.campo), valor: c.pares })),
      { fv: (v) => num(v), corUnica: cor("--serie-1"), larguraRotulo: 200 });

    const ordem = ["snapshot", "one_year", "three_year"];
    const imp = rev.impacto.slice().sort((a, b) => a.ano - b.ano || ordem.indexOf(a.versao) - ordem.indexOf(b.versao));
    const metricas = [["registros", (v) => num(v)], ["originados", (v) => num(v)], ["preco_renda", (v) => num(v, 3) + "x"], ["taxa_negativa_compra", (v) => pct(v, 2)]];
    tabela(document.getElementById("tabela-impacto"),
      [{ titulo: t("tab.ano") }, { titulo: t("tab.versao") }, ...metricas.map(([m]) => ({ titulo: t("met." + m), n: 1 }))],
      imp.map((r) => [String(r.ano), t("versao." + r.versao), ...metricas.map(([m, f]) => (r[m] == null ? "—" : f(r[m])))]));
  }

  function capComo() {
    const el = document.getElementById("arquitetura");
    const largura = Math.max(320, el.clientWidth || 900);
    const estreito = largura < 700;
    const caixas = [["ffiec", false], ["bronze", false], ["silver", true], ["dq", true], ["gold", false], ["pagina", false]];
    const w = estreito ? largura - 40 : (largura - 40) / caixas.length - 18;
    const h = 64;
    const altura = estreito ? caixas.length * (h + 22) + 10 : 150;
    const svg = svgEm(el, largura, altura);
    svg.append("defs").append("marker").attr("id", "seta").attr("viewBox", "0 0 10 10").attr("refX", 9).attr("refY", 5)
      .attr("markerWidth", 7).attr("markerHeight", 7).attr("orient", "auto")
      .append("path").attr("d", "M0,0L10,5L0,10z").attr("fill", cor("--mudo"));
    caixas.forEach(([k, destaque], i) => {
      const x = estreito ? 20 : 20 + i * (w + 18);
      const y = estreito ? 10 + i * (h + 22) : 30;
      svg.append("rect").attr("class", "caixa" + (destaque ? " destaque" : "")).attr("x", x).attr("y", y).attr("width", w).attr("height", h).attr("rx", 10);
      svg.append("text").attr("x", x + 12).attr("y", y + 26).attr("font-weight", 700).text(t("arq." + k));
      svg.append("text").attr("class", "sub").attr("x", x + 12).attr("y", y + 46).text(t("arq." + k + "2"));
      if (i < caixas.length - 1) {
        const linha = estreito
          ? [x + w / 2, y + h, x + w / 2, y + h + 20]
          : [x + w, y + h / 2, x + w + 16, y + h / 2];
        svg.append("line").attr("class", "seta").attr("x1", linha[0]).attr("y1", linha[1]).attr("x2", linha[2]).attr("y2", linha[3]).attr("marker-end", "url(#seta)");
      }
    });
    if (!estreito) {
      // ramo das revisões, saindo da bronze
      const xb = 20 + 1 * (w + 18);
      svg.append("text").attr("class", "sub").attr("x", xb).attr("y", 120).text("↳ " + t("arq.rev") + ": " + t("arq.rev2"));
    }
    const fatos = document.getElementById("fatos");
    fatos.replaceChildren();
    for (let i = 1; i <= 8; i++) {
      const li = document.createElement("li");
      li.innerHTML = t("fato." + i); // texto fixo da própria página, sem dado externo
      fatos.appendChild(li);
    }
  }

  // ---------- montagem ----------
  function aplicarTextos() {
    document.documentElement.lang = idioma === "pt" ? "pt-BR" : "en";
    for (const el of document.querySelectorAll("[data-i18n]")) el.textContent = t(el.dataset.i18n);
    document.getElementById("idioma").textContent = idioma === "pt" ? "EN" : "PT";
    const nav = document.getElementById("nav-capitulos");
    nav.replaceChildren();
    for (let i = 1; i <= 7; i++) {
      const a = document.createElement("a");
      a.href = "#c" + i;
      a.textContent = t("cap." + i);
      a.dataset.cap = String(i);
      nav.appendChild(a);
    }
    if (D) document.getElementById("gerado-em").textContent = new Date(D.gerado_em).toLocaleDateString(loc());
  }

  function desenhar() {
    if (!D || !geo) return;
    tiles();
    capVersoes();
    capPreco();
    capJuros();
    capNegativas();
    capEquidade();
    capRevisoes();
    capComo();
  }

  function navegacao() {
    const capitulos = [...document.querySelectorAll(".capitulo")];
    const obs = new IntersectionObserver((entradas) => {
      for (const e of entradas) {
        if (!e.isIntersecting) continue;
        for (const a of document.querySelectorAll("#nav-capitulos a")) a.classList.toggle("ativo", a.dataset.cap === e.target.dataset.cap);
      }
    }, { rootMargin: "-45% 0px -50% 0px" });
    capitulos.forEach((c) => obs.observe(c));
    const barra = document.getElementById("progresso");
    window.addEventListener("scroll", () => {
      const total = document.documentElement.scrollHeight - window.innerHeight;
      barra.style.width = (total > 0 ? (window.scrollY / total) * 100 : 0) + "%";
    }, { passive: true });
  }

  document.getElementById("idioma").addEventListener("click", () => {
    idioma = idioma === "pt" ? "en" : "pt";
    guardar("idioma", idioma);
    aplicarTextos();
    desenhar();
  });
  document.getElementById("tema").addEventListener("click", () => {
    const escuroAgora = document.documentElement.dataset.theme
      ? document.documentElement.dataset.theme === "dark"
      : window.matchMedia("(prefers-color-scheme: dark)").matches;
    document.documentElement.dataset.theme = escuroAgora ? "light" : "dark";
    guardar("tema", document.documentElement.dataset.theme);
    desenhar();
  });
  let larguraAnterior = window.innerWidth;
  window.addEventListener("resize", () => {
    if (Math.abs(window.innerWidth - larguraAnterior) < 40) return;
    larguraAnterior = window.innerWidth;
    clearTimeout(window.__redesenho);
    window.__redesenho = setTimeout(desenhar, 200);
  });
  window.matchMedia("(prefers-color-scheme: dark)").addEventListener("change", desenhar);

  aplicarTextos();
  navegacao();
  Promise.all([
    fetch("dados/resultados.json").then((r) => r.json()),
    fetch("https://cdn.jsdelivr.net/npm/us-atlas@3/states-albers-10m.json").then((r) => r.json())
  ]).then(([dados, mapa]) => {
    D = dados;
    geo = mapa;
    aplicarTextos();
    desenhar();
  }).catch((erro) => {
    const p = document.createElement("p");
    p.className = "aviso";
    p.textContent = "Não foi possível carregar os dados (" + erro.message + "). Abra a página por um servidor: python -m http.server -d site";
    document.querySelector("main").prepend(p);
  });
})();
