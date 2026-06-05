const socket = io();

let currentPredictionId = null;
let predictionPollingTimer = null;

function renderWaiting(id) {
  const resultDiv = document.getElementById("result");

  resultDiv.innerHTML = `
    <div class="alert alert-info">
      Mensaje enviado a Kafka. Esperando predicción por WebSocket...<br>
      <small>ID: ${id}</small>
    </div>
  `;
}

function renderPrediction(prediction) {
  stopPredictionPolling();
  const predictionValue = Number(
    prediction.Prediction ?? prediction.prediction
  );

  let predictionText = "Unknown";

  if (predictionValue === 0) {
    predictionText = "Early / No Delay";
  } else if (predictionValue === 1) {
    predictionText = "On Time";
  } else if (predictionValue === 2) {
    predictionText = "Slightly Late (0-30 Minute Delay)";
  } else if (predictionValue === 3) {
    predictionText = "Very Late (+30 Minute Delay)";
  }

  const uuid = prediction.UUID ?? prediction.uuid ?? "";
  const origin = prediction.Origin ?? prediction.origin ?? "";
  const dest = prediction.Dest ?? prediction.dest ?? "";
  const carrier = prediction.Carrier ?? prediction.carrier ?? "";
  const route = prediction.Route ?? prediction.route ?? "";
  const distance = prediction.Distance ?? prediction.distance ?? "";
  const timestamp = prediction.Timestamp ?? prediction.timestamp ?? "";

  const resultDiv = document.getElementById("result");

  resultDiv.innerHTML = `
    <div class="alert alert-success">
      <h4>${predictionText}</h4>
      <p><strong>UUID:</strong> ${uuid}</p>
      <p><strong>Origin:</strong> ${origin}</p>
      <p><strong>Destination:</strong> ${dest}</p>
      <p><strong>Carrier:</strong> ${carrier}</p>
      <p><strong>Route:</strong> ${route}</p>
      <p><strong>Distance:</strong> ${distance}</p>
      <p><strong>Prediction:</strong> ${predictionValue}</p>
      <p><strong>Timestamp:</strong> ${timestamp}</p>
      <hr>
      <small>Predicción recibida desde Kafka mediante WebSocket.</small>
    </div>
  `;
}

function renderError(message) {
  const resultDiv = document.getElementById("result");

  resultDiv.innerHTML = `
    <div class="alert alert-danger">
      Error enviando la petición a Kafka: ${message}
    </div>
  `;
}


function stopPredictionPolling() {
  if (predictionPollingTimer) {
    clearInterval(predictionPollingTimer);
    predictionPollingTimer = null;
  }
}

function startPredictionPolling(id) {
  stopPredictionPolling();

  predictionPollingTimer = setInterval(async function () {
    try {
      const response = await fetch(`/flights/delays/predict/classify_realtime/response/${id}`);
      const data = await response.json();

      if (data.status === "OK" && data.prediction) {
        stopPredictionPolling();
        renderPrediction(data.prediction);
      }
    } catch (error) {
      console.error("Error consultando predicción por polling:", error);
    }
  }, 2000);
}

document.addEventListener("DOMContentLoaded", function () {
  const form = document.getElementById("flight_delay_classification");

  if (!form) {
    console.error("No se ha encontrado el formulario con id flight_delay_classification");
    return;
  }

  form.addEventListener("submit", async function (event) {
    event.preventDefault();

    const formData = new FormData(form);

    try {
      const response = await fetch("/flights/delays/predict/classify_realtime", {
        method: "POST",
        body: formData
      });

      const text = await response.text();

      let data;

      try {
        data = JSON.parse(text);
      } catch (error) {
        console.error("Flask ha devuelto HTML o texto en vez de JSON:");
        console.error(text);

        throw new Error(
          "Flask ha devuelto HTML en vez de JSON. Revisa la ruta /flights/delays/predict/classify_realtime en predict_flask.py"
        );
      }

      if (!response.ok) {
        throw new Error(data.error || data.message || "Error desconocido en Flask");
      }

      currentPredictionId = data.id || data.uuid || data.UUID;

      if (!currentPredictionId) {
        console.error("Respuesta recibida sin id:", data);
        throw new Error("Flask respondió bien, pero no devolvió ningún id/uuid");
      }

      socket.emit("join", { id: currentPredictionId });

      renderWaiting(currentPredictionId);
      startPredictionPolling(currentPredictionId);

    } catch (error) {
      console.error(error);
      renderError(error.message || error);
    }
  });
});

socket.on("prediction_response", function (prediction) {
  if (!currentPredictionId) {
    return;
  }

  const predictionId = prediction.UUID ?? prediction.uuid;

  if (predictionId === currentPredictionId) {
    renderPrediction(prediction);
  }
});

socket.on("prediction", function (prediction) {
  if (!currentPredictionId) {
    return;
  }

  const predictionId = prediction.UUID ?? prediction.uuid;

  if (predictionId === currentPredictionId) {
    renderPrediction(prediction);
  }
});
function predictionLabel(value) {
  const predictionValue = Number(value);

  if (predictionValue === 0) {
    return "EARLY";
  } else if (predictionValue === 1) {
    return "ON TIME";
  } else if (predictionValue === 2) {
    return "SLIGHT DELAY";
  } else if (predictionValue === 3) {
    return "VERY LATE";
  }

  return "UNKNOWN";
}

function predictionClass(value) {
  const predictionValue = Number(value);

  if (predictionValue === 0 || predictionValue === 1) {
    return "status-ok";
  } else if (predictionValue === 2) {
    return "status-warn";
  } else if (predictionValue === 3) {
    return "status-late";
  }

  return "status-warn";
}

function renderHistoryBoard(predictions) {
  const historyContent = document.getElementById("history-content");

  if (!historyContent) {
    return;
  }

  if (!predictions || predictions.length === 0) {
    historyContent.innerHTML = `
      <div class="airport-empty">
        Todavía no hay predicciones guardadas en Cassandra.
      </div>
    `;
    return;
  }

  const rows = predictions.map((prediction) => {
    const uuid = prediction.UUID ?? prediction.uuid ?? "";
    const origin = prediction.Origin ?? prediction.origin ?? "";
    const dest = prediction.Dest ?? prediction.dest ?? "";
    const carrier = prediction.Carrier ?? prediction.carrier ?? "";
    const route = prediction.Route ?? prediction.route ?? "";
    const distance = prediction.Distance ?? prediction.distance ?? "";
    const predictionValue = prediction.Prediction ?? prediction.prediction ?? "";
    const timestamp = prediction.Timestamp ?? prediction.timestamp ?? "";

    return `
      <tr>
        <td>${timestamp}</td>
        <td>${carrier}</td>
        <td>${origin}</td>
        <td>${dest}</td>
        <td>${route}</td>
        <td>${distance}</td>
        <td>
          <span class="status-pill ${predictionClass(predictionValue)}">
            ${predictionLabel(predictionValue)}
          </span>
        </td>
        <td>${uuid}</td>
      </tr>
    `;
  }).join("");

  historyContent.innerHTML = `
    <div class="airport-table-wrap">
      <table class="airport-table">
        <thead>
          <tr>
            <th>Time</th>
            <th>Carrier</th>
            <th>Origin</th>
            <th>Destination</th>
            <th>Route</th>
            <th>Distance</th>
            <th>Status</th>
            <th>UUID</th>
          </tr>
        </thead>
        <tbody>
          ${rows}
        </tbody>
      </table>
    </div>
  `;
}

async function loadPredictionHistory() {
  const historyContent = document.getElementById("history-content");

  if (historyContent) {
    historyContent.innerHTML = `
      <div class="airport-empty">
        Cargando predicciones desde Cassandra...
      </div>
    `;
  }

  try {
    const response = await fetch("/flights/delays/predictions_history");
    const data = await response.json();

    if (!response.ok || data.status !== "OK") {
      throw new Error(data.error || "No se pudo cargar el historial");
    }

    renderHistoryBoard(data.predictions);

  } catch (error) {
    console.error(error);

    if (historyContent) {
      historyContent.innerHTML = `
        <div class="airport-empty">
          Error cargando el histórico: ${error.message || error}
        </div>
      `;
    }
  }
}

document.addEventListener("DOMContentLoaded", function () {
  const historyToggle = document.getElementById("history-toggle");
  const historyBoard = document.getElementById("history-board");

  if (!historyToggle || !historyBoard) {
    return;
  }

  historyToggle.addEventListener("click", async function () {
    historyBoard.classList.toggle("visible");

    if (historyBoard.classList.contains("visible")) {
      historyToggle.textContent = "Ocultar anteriores predicciones";
      await loadPredictionHistory();
    } else {
      historyToggle.textContent = "Ver anteriores predicciones";
    }
  });
});
