const statusEl = document.getElementById("status");
const queryInput = document.getElementById("queryInput");
const searchButton = document.getElementById("searchButton");
const resultsHeadingEl = document.getElementById("resultsHeading");
const matchesEl = document.getElementById("matches");
const selectedEl = document.getElementById("selected");
const selectedTitleEl = document.getElementById("selectedTitle");
const selectedMetaEl = document.getElementById("selectedMeta");
const recsBodyEl = document.getElementById("recsBody");

let data = null;
let worksById = new Map();
let targetIds = [];
let searchable = [];

function setStatus(text) {
  statusEl.textContent = text;
}

function fmtInt(value) {
  if (value == null || Number.isNaN(value)) return "-";
  return new Intl.NumberFormat().format(value);
}

function scoreMatch(entry, query, queryNum) {
  if (!query) return Math.log10(entry.kudosEdges + 10);

  let score = 0;
  if (queryNum != null && entry.id === queryNum) score += 10000;
  if (entry.title.startsWith(query)) score += 800;
  else if (entry.title.includes(query)) score += 350;
  if (entry.author.startsWith(query)) score += 260;
  else if (entry.author.includes(query)) score += 120;
  if (String(entry.id).startsWith(query)) score += 180;
  score += Math.log10(entry.kudosEdges + 10);
  return score;
}

function findMatches(rawQuery) {
  const query = rawQuery.trim().toLowerCase();
  const queryNum = /^\d+$/.test(query) ? Number(query) : null;
  const ranked = searchable
    .map((entry) => ({ entry, score: scoreMatch(entry, query, queryNum) }))
    .filter((row) => row.score > 0)
    .sort((a, b) => b.score - a.score)
    .slice(0, 18)
    .map((row) => row.entry);
  return ranked;
}

function renderMatches(matches) {
  matchesEl.textContent = "";
  for (const match of matches) {
    const li = document.createElement("li");
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "match-item";

    const title = document.createElement("strong");
    title.textContent = match.titleDisplay || `(work ${match.id})`;

    const meta = document.createElement("span");
    meta.className = "match-meta";
    meta.textContent = `${match.authorDisplay || "Unknown"} | id ${match.id} | edges ${fmtInt(match.kudosEdges)}`;

    btn.append(title, meta);
    btn.addEventListener("click", () => selectWork(match.id, { scrollToSelection: true }));
    li.appendChild(btn);
    matchesEl.appendChild(li);
  }
}

function setResultsVisible(isVisible) {
  resultsHeadingEl.hidden = !isVisible;
  matchesEl.hidden = !isVisible;
}

function setEmptyRecommendations(message) {
  recsBodyEl.textContent = "";
  const row = document.createElement("tr");
  const cell = document.createElement("td");
  cell.colSpan = 6;
  cell.className = "empty";
  cell.textContent = message;
  row.appendChild(cell);
  recsBodyEl.appendChild(row);
}

function recommendationRow(rank, rec) {
  const tr = document.createElement("tr");
  const work = worksById.get(rec.id);

  const rankTd = document.createElement("td");
  rankTd.textContent = String(rank);

  const workTd = document.createElement("td");
  const link = document.createElement("a");
  link.textContent = work?.title || `(work ${rec.id})`;
  link.href = work?.url || `https://archiveofourown.org/works/${rec.id}`;
  link.target = "_blank";
  link.rel = "noopener noreferrer";

  const meta = document.createElement("div");
  meta.className = "match-meta";
  meta.textContent = `${work?.author || "Unknown"} | id ${rec.id}`;

  workTd.append(link, meta);

  const scoreTd = document.createElement("td");
  scoreTd.textContent = Number(rec.score).toFixed(4);

  const cosineTd = document.createElement("td");
  cosineTd.textContent = Number(rec.cosine).toFixed(4);

  const overlapTd = document.createElement("td");
  overlapTd.textContent = fmtInt(rec.overlap);

  const kudosTd = document.createElement("td");
  kudosTd.textContent = fmtInt(rec.kudos_edges);

  tr.append(rankTd, workTd, scoreTd, cosineTd, overlapTd, kudosTd);
  return tr;
}

function selectWork(workId, options = {}) {
  const scrollToSelection = Boolean(options.scrollToSelection);
  const work = worksById.get(workId);
  if (!work) {
    setStatus(`Work ${workId} is not in the exported data.`);
    return;
  }

  selectedEl.hidden = false;
  selectedTitleEl.textContent = work.title || `(work ${work.id})`;
  selectedMetaEl.textContent = "";
  const metaPrefix = document.createTextNode(
    `${work.author || "Unknown"} | id ${work.id} | edges ${fmtInt(work.kudos_edges)} | `
  );
  const workLink = document.createElement("a");
  workLink.textContent = "open on AO3";
  workLink.href = work.url || `https://archiveofourown.org/works/${work.id}`;
  workLink.target = "_blank";
  workLink.rel = "noopener noreferrer";
  selectedMetaEl.append(metaPrefix, workLink);

  const recs = data.recommendations[String(workId)] || [];
  if (!recs.length) {
    setEmptyRecommendations("No recommendations matched export filters for this work.");
    if (scrollToSelection) {
      selectedEl.scrollIntoView({ behavior: "smooth", block: "start" });
    }
    return;
  }

  recsBodyEl.textContent = "";
  recs.forEach((rec, idx) => recsBodyEl.appendChild(recommendationRow(idx + 1, rec)));
  if (scrollToSelection) {
    selectedEl.scrollIntoView({ behavior: "smooth", block: "start" });
  }
}

function searchAndRender() {
  if (!data) return;
  const query = queryInput.value.trim();
  if (!query) {
    setResultsVisible(false);
    matchesEl.textContent = "";
    return;
  }

  setResultsVisible(true);
  renderMatches(findMatches(queryInput.value));
}

async function init() {
  try {
    const res = await fetch("./data/recommendations.json", { cache: "no-store" });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    data = await res.json();

    worksById = new Map(data.works.map((work) => [work.id, work]));
    targetIds = data.target_work_ids || [];
    searchable = targetIds.map((id) => {
      const work = worksById.get(id) || {};
      return {
        id,
        title: (work.title || "").toLowerCase(),
        author: (work.author || "").toLowerCase(),
        titleDisplay: work.title || "",
        authorDisplay: work.author || "",
        kudosEdges: work.kudos_edges || 0,
      };
    });

    setStatus(
      `Loaded ${fmtInt(data.stats?.target_work_count || targetIds.length)} target works from ${new Date(data.generated_at_utc).toLocaleString()}.`
    );

    setResultsVisible(false);

    const params = new URLSearchParams(window.location.search);
    const idParam = Number(params.get("id"));
    if (!Number.isNaN(idParam) && worksById.has(idParam)) {
      selectWork(idParam);
      queryInput.value = String(idParam);
    }
  } catch (err) {
    setStatus(`Failed to load data: ${String(err)}`);
    setEmptyRecommendations("Run the export script to generate docs/data/recommendations.json.");
  }
}

queryInput.addEventListener("input", searchAndRender);
queryInput.addEventListener("keydown", (event) => {
  if (event.key === "Enter") {
    event.preventDefault();
    if (!queryInput.value.trim()) return;
    const matches = findMatches(queryInput.value);
    if (matches.length) selectWork(matches[0].id, { scrollToSelection: true });
  }
});
searchButton.addEventListener("click", () => {
  if (!queryInput.value.trim()) return;
  const matches = findMatches(queryInput.value);
  if (matches.length) selectWork(matches[0].id, { scrollToSelection: true });
});

init();
