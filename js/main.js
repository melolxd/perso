/**
 * Marque le statut d'un pronostic (success/fail).
 * @param {string} uid - L'ID unique du pronostic.
 * @param {'success' | 'fail'} status - Le nouveau statut.
 */
async function mark(uid, status) {
  try {
    const response = await fetch(`/update/${uid}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ status }),
    });

    if (!response.ok) {
      alert("Erreur lors de la mise à jour.");
      return;
    }

    const row = document.getElementById(uid);
    if (!row) return;

    const statusCell = row.querySelector(".status-cell");
    statusCell.textContent = status.charAt(0).toUpperCase() + status.slice(1);
    
    // Met à jour les classes pour la couleur
    statusCell.classList.remove("text-pending", "text-success", "text-danger");
    statusCell.classList.add(status === "success" ? "text-success" : "text-danger");

    // Désactive les boutons pour éviter les clics multiples
    row.querySelectorAll('.action-buttons button').forEach(button => {
        if (!button.classList.contains('btn-delete')) { // ne pas désactiver le bouton supprimer
            button.disabled = true;
        }
    });

  } catch (error) {
    console.error("Fetch error:", error);
    alert("Une erreur de communication est survenue.");
  }
}

/**
 * Supprime un pronostic du tableau et de la base de données.
 * @param {string} uid - L'ID unique du pronostic.
 */
async function removeRow(uid) {
  if (!confirm("Voulez-vous vraiment supprimer ce pronostic ?")) return;

  try {
    const response = await fetch(`/delete/${uid}`, { method: "POST" });
    if (!response.ok) {
        alert("Erreur lors de la suppression.");
        return;
    }
    
    const row = document.getElementById(uid);
    if (row) {
        // Ajoute une petite animation de sortie
        row.style.opacity = '0';
        row.style.transition = 'opacity 0.3s ease-out';
        setTimeout(() => row.remove(), 300);
    }
  } catch(error) {
      console.error("Fetch error:", error);
      alert("Une erreur de communication est survenue.");
  }
}