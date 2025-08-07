document.addEventListener('DOMContentLoaded', function() {
  
  // GADGET: Animation du titre façon terminal
  const titleElement = document.getElementById('main-title');
  if (titleElement) {
    const title = "ATP PREDICTOR";
    let i = 0;
    titleElement.innerHTML = ''; // Clear for rewrite effect
    function typeWriter() {
      if (i < title.length) {
        titleElement.innerHTML += title.charAt(i);
        i++;
        setTimeout(typeWriter, 100);
      }
    }
    typeWriter();
  }

  // GADGET: Animation de fond avec particles.js
  if (document.getElementById('background-canvas')) {
    particlesJS("background-canvas", {
      particles: { number: {value: 50, density: {enable: true, value_area: 800}}, color: {value: "#00e5ff"}, opacity: {value: 0.5, random: true}, size: {value: 2, random: true}, line_linked: {enable: true, distance: 150, color: "#00e5ff", opacity: 0.1, width: 1}, move: {enable: true, speed: 1, direction: "none", out_mode: "out"}},
      interactivity: { detect_on: "canvas", events: { onhover: {enable: true, mode: "grab"}}, modes: { grab: {distance: 140, line_linked: {opacity: 0.3}}}},
      retina_detect: true
    });
  }
  
  // GADGET: Initialisation du Donut Chart (page historique)
  const statsChart = document.getElementById('stats-chart');
  if (statsChart && statsChart.dataset.success){
    const successRate = parseFloat(statsChart.dataset.success);
    setTimeout(() => {
        statsChart.style.setProperty('--p-success', successRate);
        statsChart.style.setProperty('--p-fail', 100);
    }, 500);
  }

  // GADGET: Logique de tri du tableau (page index)
  const sortBtn = document.getElementById('sort-by-prob-btn');
  const tableBody = document.getElementById('predictions-tbody');
  if (sortBtn && tableBody) {
      let currentSortOrder = 'desc'; 
      sortBtn.addEventListener('click', () => {
          const rows = Array.from(tableBody.querySelectorAll('tr'));
          const sortedRows = rows.sort((rowA, rowB) => {
              const probA = parseFloat(rowA.dataset.probability);
              const probB = parseFloat(rowB.dataset.probability);
              return currentSortOrder === 'desc' ? probB - probA : probA - probB; 
          });

          tableBody.innerHTML = '';
          sortedRows.forEach(row => tableBody.appendChild(row));
          currentSortOrder = (currentSortOrder === 'desc') ? 'asc' : 'desc';
      });
  }

  // NOUVEAU : GESTION DES LIGNES DE PRÉDICTION MULTIPLES
  const addMatchBtn = document.getElementById('add-match-btn');
  const rowsContainer = document.getElementById('prediction-rows-container');

  if (addMatchBtn && rowsContainer) {
      const rowTemplate = rowsContainer.querySelector('.row').cloneNode(true);
      
      const createRemoveButton = () => {
          const removeBtn = document.createElement('button');
          removeBtn.type = 'button'; 
          removeBtn.className = 'action-btn-remove-row';
          removeBtn.title = 'Supprimer cette ligne';
          removeBtn.innerHTML = '<i class="bi bi-x-circle"></i>';
          return removeBtn;
      };

      addMatchBtn.addEventListener('click', () => {
          const newRow = rowTemplate.cloneNode(true);
          newRow.querySelectorAll('input, select').forEach(input => input.value = '');
          newRow.appendChild(createRemoveButton());
          rowsContainer.appendChild(newRow);
      });

      rowsContainer.addEventListener('click', function(e) {
          const removeBtn = e.target.closest('.action-btn-remove-row');
          if (!removeBtn) return;
          
          if (rowsContainer.querySelectorAll('.row').length > 1) {
              const rowToRemove = removeBtn.closest('.row');
              rowToRemove.remove();
          } else {
              alert("Impossible de supprimer la dernière ligne.");
          }
      });
      
      const firstRow = rowsContainer.querySelector('.row');
      if (firstRow) {
          firstRow.appendChild(createRemoveButton());
      }
  }
});


/**
 * Met à jour le statut d'un pronostic (succès/échec) via un appel API.
 * @param {string} uid - L'ID unique du pronostic.
 * @param {'success' | 'fail'} status - Le nouveau statut.
 */
async function mark(uid, status) {
  try {
    const response = await fetch(`/update/${uid}`, { method: "POST", headers: {"Content-Type": "application/json"}, body: JSON.stringify({ status }) });
    if (!response.ok) throw new Error("La mise à jour a échoué.");

    const row = document.getElementById(uid);
    if (!row) return;

    const statusCell = row.querySelector(".status-cell");
    statusCell.textContent = status.charAt(0).toUpperCase() + status.slice(1);
    statusCell.className = `status-cell status-${status}`;

    row.querySelectorAll('.action-buttons button').forEach(button => {
        if (!button.classList.contains('action-btn-delete')) button.disabled = true;
    });

  } catch (error) {
    console.error("Erreur de mise à jour:", error);
    alert("Erreur: Impossible de mettre à jour le pronostic.");
  }
}

/**
 * Supprime un pronostic de la BDD et du tableau avec une animation.
 * @param {string} uid - L'ID unique du pronostic.
 */
async function removeRow(uid) {
  // La confirmation par pop-up n'est plus présente ici
  try {
    const response = await fetch(`/delete/${uid}`, { method: "POST" });
    if (!response.ok) throw new Error("La suppression a échoué.");

    const row = document.getElementById(uid);
    if (row) {
        row.style.opacity = '0';
        row.style.transform = 'translateX(20px)';
        row.style.transition = 'opacity 0.4s ease-out, transform 0.4s ease-out';
        setTimeout(() => row.remove(), 400);
    }
  } catch(error) {
    console.error("Erreur de suppression:", error);
    alert("Erreur: Impossible de supprimer le pronostic.");
  }
}