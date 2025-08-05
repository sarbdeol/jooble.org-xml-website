<div class="card card-custom shadow-lg mt-4">
  <div class="card-header border-0 bg-transparent">
    <h5>📊 CPC Distribution</h5>
  </div>
  <div class="card-body">
    <canvas id="cpcChart"></canvas>
  </div>
</div>

<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<script>
  const ctx = document.getElementById('cpcChart').getContext('2d');
  const cpcStats = {{ logs[0][4] | tojson if logs and logs[0][4] else '{}' }};
  
  const labels = Object.keys(cpcStats);
  const data = Object.values(cpcStats);

  new Chart(ctx, {
    type: 'bar',
    data: {
      labels: labels,
      datasets: [{
        label: 'Job Count per CPC',
        data: data,
        backgroundColor: 'rgba(247, 37, 133, 0.7)',
        borderColor: '#f72585',
        borderWidth: 1
      }]
    },
    options: {
      responsive: true,
      plugins: {
        legend: { display: false },
        tooltip: { enabled: true }
      },
      scales: {
        y: { beginAtZero: true }
      }
    }
  });
</script>
