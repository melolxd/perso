// ======================= Main =======================
document.addEventListener("DOMContentLoaded", () => {
  // --- 1) Animation "Machine à écrire" pour le titre ---
  const titleElement = document.getElementById("main-title");
  if (titleElement) {
    const titleText = "ATP PREDICTOR TERMINAL";
    let i = 0;
    titleElement.innerHTML = "";
    (function typeWriter() {
      if (i < titleText.length) {
        titleElement.innerHTML += titleText.charAt(i++);
        titleElement.style.borderRight = "0.15em solid #00ff9c";
        setTimeout(typeWriter, 100);
      } else {
        titleElement.style.borderRight = "none";
      }
    })();
  }

  // --- 2) Particules de fond (si présent) ---
  if (typeof particlesJS !== "undefined") {
    particlesJS("background-canvas", {
      /* ta config ici */
    });
  }

  // --- 3) Réfs UI dossiers (si on est sur la page qui les contient) ---
  const createFolderBtn = document.getElementById("create-folder-btn");
  const predictionsTbody = document.getElementById("predictions-tbody");
  const folderTemplate = document.getElementById("folder-template");
  const hasFoldersUI = !!(
    createFolderBtn &&
    predictionsTbody &&
    folderTemplate
  );
  if (!hasFoldersUI) {
    console.warn(
      "Éléments dossiers non trouvés → on ignore la partie dossiers."
    );
  }

  // ---------- 4) Persistance (partagée) ----------
  const LS_KEYS = {
    folders: "atp_pred_folders",
    matchMap: "atp_pred_match_to_folder",
  };
  const loadFolders = () =>
    JSON.parse(localStorage.getItem(LS_KEYS.folders) || "[]");
  const saveFolders = (arr) =>
    localStorage.setItem(LS_KEYS.folders, JSON.stringify(arr));
  const loadMatchMap = () =>
    JSON.parse(localStorage.getItem(LS_KEYS.matchMap) || "{}");
  const saveMatchMap = (obj) =>
    localStorage.setItem(LS_KEYS.matchMap, JSON.stringify(obj));

  const persistMatchAssignment = (matchId, folderId) => {
    if (!matchId) return;
    const map = loadMatchMap();
    map[matchId] = folderId;
    saveMatchMap(map);
  };
  const removeMatchAssignment = (matchId) => {
    const map = loadMatchMap();
    delete map[matchId];
    saveMatchMap(map);
  };

  // ---------- 5) Dossiers (helpers de pliage) ----------
  const setFolderCollapsed = (folderId, collapsed) => {
    const folders = loadFolders().map((f) =>
      f.id === folderId ? { ...f, collapsed: !!collapsed } : f
    );
    saveFolders(folders);
  };

  const applyFolderCollapse = (folderRow, collapsed) => {
    const folderId = folderRow.dataset.folderId;
    if (!folderId) return;
    folderRow.classList.toggle("collapsed", !!collapsed);

    // cacher/montrer les enfants
    let next = folderRow.nextElementSibling;
    while (
      next &&
      next.dataset &&
      next.dataset.folderId === folderId &&
      next.classList.contains("in-folder")
    ) {
      next.style.display = collapsed ? "none" : "";
      next = next.nextElementSibling;
    }

    // icône caret
    const icon = folderRow.querySelector(".folder-toggle i");
    if (icon) {
      icon.classList.toggle("bi-caret-down-fill", !collapsed);
      icon.classList.toggle("bi-caret-right-fill", !!collapsed);
    }
  };

  const toggleFolder = (folderRow) => {
    const willCollapse = !folderRow.classList.contains("collapsed");
    applyFolderCollapse(folderRow, willCollapse);
    setFolderCollapsed(folderRow.dataset.folderId, willCollapse);
  };

  // ---------- 6) Dossiers (création / actions / DnD / rebuild) ----------
  const createFolder = () => {
    const frag = folderTemplate.content.cloneNode(true);
    const row = frag.querySelector(".folder-row");
    const id = `folder-${Date.now()}`;
    row.dataset.folderId = id;

    const nameSpan = row.querySelector(".folder-name");
    if (nameSpan) nameSpan.textContent = "Nouveau dossier";

    predictionsTbody.prepend(row);

    const folders = loadFolders();
    folders.unshift({ id, name: "Nouveau dossier", collapsed: false });
    saveFolders(folders);
  };

  const handleFolderActions = (e) => {
    // Toggle
    const toggleBtn = e.target.closest(".folder-toggle");
    if (toggleBtn) {
      const folderRow = toggleBtn.closest(".folder-row");
      toggleFolder(folderRow);
      return;
    }
    // Rename / Delete
    const renameBtn = e.target.closest(".action-btn-rename");
    const deleteBtn = e.target.closest(".action-btn-delete-folder");

    if (renameBtn) {
      const folderRow = renameBtn.closest(".folder-row");
      const folderId = folderRow.dataset.folderId;
      const cont = renameBtn.closest(".folder-content");
      const nameSpan = cont.querySelector(".folder-name");
      const nameInput = cont.querySelector(".folder-name-input");

      nameSpan.style.display = "none";
      nameInput.style.display = "inline-block";
      nameInput.value = nameSpan.textContent.trim();
      nameInput.focus();
      nameInput.select();

      const finalizeRename = () => {
        const newName = (nameInput.value || "").trim() || "Dossier sans nom";
        nameSpan.textContent = newName;
        nameInput.style.display = "none";
        nameSpan.style.display = "inline-block";

        const updated = loadFolders().map((f) =>
          f.id === folderId ? { ...f, name: newName } : f
        );
        saveFolders(updated);

        nameInput.removeEventListener("blur", finalizeRename);
        nameInput.removeEventListener("keydown", onKey);
      };
      const onKey = (ev) => {
        if (ev.key === "Enter") finalizeRename();
        if (ev.key === "Escape") {
          nameInput.style.display = "none";
          nameSpan.style.display = "inline-block";
          nameInput.removeEventListener("blur", finalizeRename);
          nameInput.removeEventListener("keydown", onKey);
        }
      };

      nameInput.addEventListener("blur", finalizeRename);
      nameInput.addEventListener("keydown", onKey);
    }

    if (deleteBtn) {
      const folderRow = deleteBtn.closest(".folder-row");
      const folderId = folderRow.dataset.folderId;
      if (
        !confirm("Supprimer ce dossier ? Les matchs ne seront pas supprimés.")
      )
        return;

      // Dégrouper ses enfants
      let next = folderRow.nextElementSibling;
      while (
        next &&
        next.dataset &&
        next.dataset.folderId === folderId &&
        next.classList.contains("in-folder")
      ) {
        next.classList.remove("in-folder");
        delete next.dataset.folderId;
        removeMatchAssignment(next.id);
        next = next.nextElementSibling;
      }
      folderRow.remove();
      saveFolders(loadFolders().filter((f) => f.id !== folderId));
    }
  };

  const getLastChildOfFolder = (folderRow) => {
    const folderId = folderRow.dataset.folderId;
    let last = folderRow;
    let next = folderRow.nextElementSibling;
    while (
      next &&
      next.dataset &&
      next.dataset.folderId === folderId &&
      next.classList.contains("in-folder")
    ) {
      last = next;
      next = next.nextElementSibling;
    }
    return last;
  };

  // DnD
  let draggedItem = null;

  if (hasFoldersUI) {
    predictionsTbody.addEventListener("dragstart", (e) => {
      const row = e.target.closest(".match-row");
      if (!row) return;
      draggedItem = row;
      setTimeout(() => row.classList.add("dragging"), 0);
      e.dataTransfer.effectAllowed = "move";
      e.dataTransfer.setData("text/plain", row.id || "");
    });

    predictionsTbody.addEventListener("dragend", () => {
      if (draggedItem) {
        draggedItem.classList.remove("dragging");
        draggedItem = null;
      }
    });

    predictionsTbody.addEventListener("dragover", (e) => {
      e.preventDefault();
      const targetFolder = e.target.closest(".folder-row");
      document
        .querySelectorAll(".folder-row.drag-over")
        .forEach((r) => r.classList.remove("drag-over"));
      if (targetFolder) targetFolder.classList.add("drag-over");
    });

    predictionsTbody.addEventListener("dragleave", (e) => {
      const targetFolder = e.target.closest(".folder-row");
      if (targetFolder) targetFolder.classList.remove("drag-over");
    });

    predictionsTbody.addEventListener("drop", (e) => {
      e.preventDefault();
      const folderRow = e.target.closest(".folder-row");
      document
        .querySelectorAll(".folder-row.drag-over")
        .forEach((r) => r.classList.remove("drag-over"));
      if (draggedItem && folderRow) {
        if (folderRow.classList.contains("collapsed")) {
          applyFolderCollapse(folderRow, false);
          setFolderCollapsed(folderRow.dataset.folderId, false);
        }
        const lastChild = getLastChildOfFolder(folderRow);
        draggedItem.dataset.folderId = folderRow.dataset.folderId;
        draggedItem.classList.add("in-folder");
        draggedItem.style.display = "";
        lastChild.insertAdjacentElement("afterend", draggedItem);
        persistMatchAssignment(draggedItem.id, folderRow.dataset.folderId);
      }
    });
  }

  const rebuildFromStorage = () => {
    // dossiers
    const folders = loadFolders();
    folders
      .slice()
      .reverse()
      .forEach((f) => {
        const frag = folderTemplate.content.cloneNode(true);
        const row = frag.querySelector(".folder-row");
        row.dataset.folderId = f.id;
        const nameSpan = row.querySelector(".folder-name");
        if (nameSpan) nameSpan.textContent = f.name || "Dossier";
        predictionsTbody.prepend(row);
        if (f.collapsed) row.classList.add("collapsed");
        const icon = row.querySelector(".folder-toggle i");
        if (icon) {
          icon.classList.toggle("bi-caret-down-fill", !f.collapsed);
          icon.classList.toggle("bi-caret-right-fill", !!f.collapsed);
        }
      });

    // affectations
    const map = loadMatchMap();
    Object.entries(map).forEach(([matchId, folderId]) => {
      const match = document.getElementById(matchId);
      const folderRow = [
        ...predictionsTbody.querySelectorAll(".folder-row"),
      ].find((r) => r.dataset.folderId === folderId);
      if (match && folderRow) {
        const last = getLastChildOfFolder(folderRow);
        match.dataset.folderId = folderId;
        match.classList.add("in-folder");
        last.insertAdjacentElement("afterend", match);
      }
    });

    // appliquer affichage replié/affiché
    const mapCollapsed = Object.fromEntries(
      loadFolders().map((f) => [f.id, !!f.collapsed])
    );
    [...predictionsTbody.querySelectorAll(".folder-row")].forEach((fr) => {
      applyFolderCollapse(fr, mapCollapsed[fr.dataset.folderId]);
    });
  };

  // ---------- 7) Init dossiers (seulement si la page possède l'UI) ----------
  if (hasFoldersUI) {
    rebuildFromStorage();
    createFolderBtn.addEventListener("click", createFolder);
    predictionsTbody.addEventListener("click", handleFolderActions);
  }

  // ---------- 8) Lancer les graphes (si présents) ----------
  console.log("Vérification Chart.js:", typeof Chart !== "undefined");
  console.log(
    "Vérification window.CHARTS:",
    typeof window.CHARTS !== "undefined",
    window.CHARTS
  );

  if (typeof Chart !== "undefined" && typeof window.CHARTS !== "undefined") {
    console.log("Initialisation des graphiques...");

    // Graphique par tournoi
    if (window.CHARTS.by_tournament) {
      const el = document.getElementById("chartByTournament");
      console.log("Element chartByTournament trouvé:", !!el);
      if (el) {
        drawHistoryCharts(window.CHARTS.by_tournament);
      }
    }

    // Graphique de calibration
    if (window.CHARTS.calibration) {
      const elCalib = document.getElementById("chartCalibration");
      if (elCalib) drawCalibrationChart(window.CHARTS.calibration);

      const elCounts = document.getElementById("chartCalibrationCounts");
      if (elCounts) drawCalibrationCounts(window.CHARTS.calibration);
    }
  } else {
    console.warn("Chart.js ou window.CHARTS non disponible");
    if (typeof Chart === "undefined") {
      console.warn(
        "Chart.js n'est pas chargé. Vérifiez l'inclusion de la librairie."
      );
    }
    if (typeof window.CHARTS === "undefined") {
      console.warn(
        "window.CHARTS n'est pas défini. Vérifiez le script template."
      );
    }
  }
});

// ======================= Charts =======================
function drawHistoryCharts(chartData) {
  console.log("drawHistoryCharts appelé avec:", chartData);

  if (typeof Chart === "undefined") {
    console.warn("Chart.js non chargé, drawHistoryCharts ignoré.");
    return;
  }

  // chartData peut être:
  // 1. {tournament: percentage, ...} -> format direct
  // 2. {tournament: {wins, losses}, ...} -> format détaillé
  // 3. Array de objets
  let rows = [];

  if (Array.isArray(chartData)) {
    rows = chartData.map((x) => ({
      tournament: x.tournament || x.name || "N/A",
      wins: Number(x.wins || x.success || 0),
      losses: Number(x.losses || x.fail || 0),
      pct: Number(x.pct || x.percentage || 0),
    }));
  } else if (chartData && typeof chartData === "object") {
    rows = Object.entries(chartData)
      .map(([tournament, value]) => {
        // Si la valeur est un nombre (pourcentage direct)
        if (typeof value === "number") {
          return {
            tournament,
            pct: Math.round(value),
            wins: 0,
            losses: 0,
            total: 1, // Pour éviter la division par zéro
          };
        }
        // Si la valeur est un objet avec wins/losses
        else if (value && typeof value === "object") {
          const wins = Number(value.wins || value.success || 0);
          const losses = Number(value.losses || value.fail || 0);
          const total = wins + losses;
          return {
            tournament,
            wins,
            losses,
            total,
            pct: total > 0 ? Math.round((wins / total) * 100) : 0,
          };
        }
        return null;
      })
      .filter((r) => r !== null);
  }

  // Filtrer et trier
  rows = rows
    .filter((r) => r.pct > 0 || r.total > 0)
    .sort((a, b) => b.pct - a.pct);

  console.log("Données préparées pour le graphique:", rows);

  // --- Bar chart par tournoi ---
  const byTournCanvas = document.getElementById("chartByTournament");
  if (byTournCanvas) {
    console.log("Création du graphique...");

    const labels = rows.map((r) => r.tournament);
    const data = rows.map((r) => r.pct);

    // Détruire le graphique existant s'il y en a un
    if (byTournCanvas.chart) {
      byTournCanvas.chart.destroy();
    }

    const chart = new Chart(byTournCanvas.getContext("2d"), {
      type: "bar",
      data: {
        labels,
        datasets: [
          {
            label: "% pronos corrects",
            data,
            backgroundColor: "rgba(0, 255, 156, 0.6)",
            borderColor: "rgba(0, 255, 156, 1)",
            borderWidth: 1,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: { duration: 600 },
        plugins: {
          legend: {
            display: false,
          },
          tooltip: {
            backgroundColor: "rgba(2, 4, 27, 0.9)",
            titleColor: "#fff",
            bodyColor: "#c9d1ec",
            borderColor: "rgba(0, 229, 255, 0.2)",
            borderWidth: 1,
            callbacks: {
              label: (ctx) => {
                const i = ctx.dataIndex;
                const r = rows[i];
                if (r.total > 1) {
                  return `${ctx.parsed.y}% (${r.wins}/${r.total})`;
                } else {
                  return `${ctx.parsed.y}%`;
                }
              },
            },
          },
          title: {
            display: true,
            text: "Taux de réussite par tournoi",
            color: "#fff",
            font: {
              family: "Oxanium",
              size: 16,
            },
          },
        },
        scales: {
          x: {
            ticks: {
              color: "#c9d1ec",
            },
            grid: {
              color: "rgba(201, 209, 236, 0.1)",
            },
          },
          y: {
            beginAtZero: true,
            suggestedMax: 100,
            ticks: {
              callback: (v) => v + "%",
              color: "#c9d1ec",
            },
            grid: {
              color: "rgba(201, 209, 236, 0.1)",
            },
          },
        },
      },
    });

    // Stocker la référence pour pouvoir la détruire plus tard
    byTournCanvas.chart = chart;
    console.log("Graphique créé avec succès");
  } else {
    console.warn("Element chartByTournament non trouvé dans le DOM");
  }

  // --- (Optionnel) Donut global si tu ajoutes <canvas id="chartGlobal"> ---
  const globalCanvas = document.getElementById("chartGlobal");
  if (globalCanvas) {
    const totalWins = rows.reduce((s, r) => s + r.wins, 0);
    const totalLosses = rows.reduce((s, r) => s + r.losses, 0);
    const total = totalWins + totalLosses || 1;
    const pct = Math.round((totalWins / total) * 100);

    // Détruire le graphique existant s'il y en a un
    if (globalCanvas.chart) {
      globalCanvas.chart.destroy();
    }

    const chart = new Chart(globalCanvas.getContext("2d"), {
      type: "doughnut",
      data: {
        labels: ["Bons pronos", "Mauvais pronos"],
        datasets: [
          {
            data: [totalWins, totalLosses],
            backgroundColor: [
              "rgba(0, 255, 156, 0.6)",
              "rgba(255, 45, 85, 0.6)",
            ],
            borderColor: ["rgba(0, 255, 156, 1)", "rgba(255, 45, 85, 1)"],
            borderWidth: 2,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        cutout: "65%",
        plugins: {
          legend: {
            display: true,
            labels: {
              color: "#c9d1ec",
            },
          },
          title: {
            display: true,
            text: `Taux global: ${pct}% (${totalWins}/${total})`,
            color: "#fff",
            font: {
              family: "Oxanium",
              size: 16,
            },
          },
          tooltip: {
            backgroundColor: "rgba(2, 4, 27, 0.9)",
            titleColor: "#fff",
            bodyColor: "#c9d1ec",
            borderColor: "rgba(0, 229, 255, 0.2)",
            borderWidth: 1,
          },
        },
      },
    });

    globalCanvas.chart = chart;
  }
}

function drawCalibrationCounts(calibrationData) {
  const countsCanvas = document.getElementById("chartCalibrationCounts");
  if (!countsCanvas) return;

  if (countsCanvas.chart) countsCanvas.chart.destroy();

  const labels = calibrationData.buckets || [];
  const counts = calibrationData.counts || [];

  countsCanvas.chart = chart;
}

// ======================= Graphique de Calibration =======================
function drawCalibrationChart(calibrationData) {
  console.log("drawCalibrationChart appelé avec:", calibrationData);

  if (typeof Chart === "undefined") {
    console.warn("Chart.js non chargé, drawCalibrationChart ignoré.");
    return;
  }

  const calibCanvas = document.getElementById("chartCalibration");
  if (!calibCanvas) {
    console.warn("Element chartCalibration non trouvé dans le DOM");
    return;
  }

  // Détruire le graphique existant s'il y en a un
  if (calibCanvas.chart) {
    calibCanvas.chart.destroy();
  }

  // Préparer les données de calibration selon votre format
  let calibPoints = [];

  if (
    calibrationData &&
    calibrationData.pred_mean &&
    calibrationData.true_rate
  ) {
    // Format: {pred_mean: [39.4, 46.8, ...], true_rate: [0, 33.3, 85.7, ...]}
    calibPoints = calibrationData.pred_mean.map((predMean, index) => ({
      x: Number(predMean) || 0,
      y: Number(calibrationData.true_rate[index]) || 0,
    }));
  } else if (Array.isArray(calibrationData)) {
    calibPoints = calibrationData;
  } else if (calibrationData && typeof calibrationData === "object") {
    calibPoints = Object.entries(calibrationData).map(([prob, accuracy]) => ({
      x: Number(prob),
      y: Number(accuracy),
    }));
  }

  console.log("Points de calibration préparés:", calibPoints);

  // Ligne de calibration parfaite (diagonale)
  const perfectCalibration = [];
  for (let i = 0; i <= 100; i += 10) {
    perfectCalibration.push({ x: i, y: i });
  }

  const chart = new Chart(calibCanvas.getContext("2d"), {
    type: "line",
    data: {
      datasets: [
        {
          label: "Calibration parfaite",
          data: perfectCalibration,
          borderColor: "rgba(128, 128, 128, 0.5)",
          backgroundColor: "transparent",
          borderDash: [5, 5],
          pointRadius: 0,
          fill: false,
        },
        {
          label: "Calibration actuelle",
          data: calibPoints,
          borderColor: "rgba(0, 255, 156, 1)",
          backgroundColor: "rgba(0, 255, 156, 0.6)",
          pointRadius: 6,
          pointHoverRadius: 8,
          fill: false,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        title: {
          display: true,
          text: "Calibration des probabilités",
          color: "#fff",
        },
        legend: {
          display: true,
          labels: {
            color: "#c9d1ec",
          },
        },
        tooltip: {
          callbacks: {
            label: (context) => {
              const bucket = calibrationData.buckets
                ? calibrationData.buckets[context.dataIndex]
                : "";
              const count = calibrationData.counts
                ? calibrationData.counts[context.dataIndex]
                : "";
              return [
                `${context.dataset.label}: (${context.parsed.x.toFixed(
                  1
                )}%, ${context.parsed.y.toFixed(1)}%)`,
                bucket ? `Intervalle: ${bucket}` : "",
                count ? `Échantillon: ${count} prédictions` : "",
              ].filter((line) => line);
            },
          },
        },
      },
      scales: {
        x: {
          type: "linear", // <<< AJOUTE ÇA
          title: {
            display: true,
            text: "Probabilité prédite (%)",
            color: "#c9d1ec",
          },
          ticks: { color: "#c9d1ec" },
          grid: { color: "rgba(201, 209, 236, 0.1)" },
          min: 0,
          max: 100,
        },
        y: {
          title: {
            display: true,
            text: "Précision observée (%)",
            color: "#c9d1ec",
          },
          ticks: {
            color: "#c9d1ec",
          },
          grid: {
            color: "rgba(201, 209, 236, 0.1)",
          },
          min: 0,
          max: 100,
        },
      },
    },
  });

  calibCanvas.chart = chart;
  console.log("Graphique de calibration créé avec succès");
}
