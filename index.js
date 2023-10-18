const express = require('express');
const sqlite3 = require('sqlite3');
const bodyParser = require('body-parser')

const app = express();
const port = 4000;

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

const db = new sqlite3.Database('amm.db');

app.get('/search', (req, res) => {
  const { startDate, endDate } = req.query;
  const query = `
    SELECT * FROM normalized
    WHERE fecha_hora >= ? AND fecha_hora <= ?
  `;

  db.all(query, [startDate, endDate], (err, rows) => {
    if (err) {
      return res.status(500).json({ error: err.message });
    }

    res.json(rows);
  });
});

app.listen(port, () => {
  console.log(`Server is running on http://localhost:${port}`);
});
