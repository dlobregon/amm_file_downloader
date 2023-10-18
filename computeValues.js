const sqlite3 = require('sqlite3').verbose();
const fs = require('fs');
const csv = require('csv-parser');

const db = new sqlite3.Database('amm.db');

const fileAPath = 'POE.csv';
const fileBPath = 'Generacion.csv';

// Function to read and process data from a CSV file in chunks
function processCSVFile(filePath, processDataCallback) {
    const dataChunks = [];
    let dataChunk = [];

    fs.createReadStream(filePath)
        .pipe(csv())
        .on('data', (row) => {
            dataChunk.push(row);
            if (dataChunk.length >= 100) {
                dataChunks.push([...dataChunk]);
                dataChunk = [];
            }
        })
        .on('end', () => {
            if (dataChunk.length > 0) {
                dataChunks.push(dataChunk);
            }

            processDataCallback(dataChunks);
        });
}

function processCostData(dataA, dataB, costData, doneCallback) {
    // Merge data from CSV files with the cost data
    const mergedData = [];

    costData.forEach((costRow) => {
        const matchA = dataA.find((rowA) => rowA.fecha_hora === costRow.fecha_hora);
        const matchB = dataB.find((rowB) => rowB.fecha_hora === costRow.fecha_hora);

        if (matchA && matchB) {
            const tmpIndicador = parseFloat(matchA.POE) > parseFloat(costRow.costo) ? 1 : 0
            const tmpLiquidacionCVG = Math.round(((parseFloat(matchB.generacion) * parseFloat(costRow.costo)) + Number.EPSILON) * 100) / 100
            const tmpLiquidacionPOE = Math.round(((parseFloat(matchB.generacion) * parseFloat(matchA.POE)) + Number.EPSILON) * 100) / 100
            const entry = {
                fecha_hora: costRow.fecha_hora,
                banda: costRow.banda,
                costo: Math.round((parseFloat(costRow.costo) + Number.EPSILON) * 100) / 100,
                POE: Math.round((parseFloat(matchA.POE) + Number.EPSILON) * 100) / 100,
                generacion: matchB.generacion,
                indicador: tmpIndicador,
                liquidacion_POE: tmpLiquidacionPOE,
                liquidacion_CVG: tmpLiquidacionCVG,
                agente_a: Math.round((((tmpLiquidacionPOE - tmpLiquidacionCVG) * tmpIndicador) + Number.EPSILON) * 100) / 100,
                agente_b: Math.round((((tmpLiquidacionPOE - tmpLiquidacionCVG) * (tmpIndicador - 1)) + Number.EPSILON) * 100) / 100
            };
            mergedData.push(entry);
        }
    });

    doneCallback(mergedData);
}

function insertMergedData(mergedData, doneCallback) {
    const createTableQuery = `
    CREATE TABLE IF NOT EXISTS normalized (
      fecha_hora TEXT PRIMARY KEY,
      banda TEXT,
      costo REAL,
      POE REAL,
      generacion REAL,
      indicador REAL,
      liquidacion_POE REAL,
      liquidacion_CVG REAL,
      agente_a REAL,
      agente_b REAL
    )
  `;
    db.run(createTableQuery, (err) => {
        if (err) {
            console.error('creation', err.message);
            return;
        }

        // Insert the merged data into the new table
        const insertQuery = `
      INSERT INTO normalized (fecha_hora, banda, costo, POE, generacion, indicador,liquidacion_POE,liquidacion_CVG, agente_a,  agente_b)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `;
        db.serialize(() => {
            const stmt = db.prepare(insertQuery);
            mergedData.forEach((entry) => {
                stmt.run(entry.fecha_hora, entry.banda, entry.costo, entry.POE, entry.generacion, entry.indicador, entry.liquidacion_POE, entry.liquidacion_CVG, entry.agente_a, entry.agente_b);
            });
            stmt.finalize();

            doneCallback();
        });
    });
}

// Process the CSV files in chunks and handle the data
processCSVFile(fileAPath, (dataChunksA) => {
    processCSVFile(fileBPath, (dataChunksB) => {
        const costQuery = 'SELECT fecha_hora, costo, banda FROM demanda_diaria';
        db.all(costQuery, (err, costData) => {
            if (err) {
                console.error(err.message);
                return;
            }

            const mergedDataChunks = [];
            let completedInsertions = 0;
            const expectedInsertions = dataChunksA.length * dataChunksB.length;

            for (const dataChunkA of dataChunksA) {
                for (const dataChunkB of dataChunksB) {
                    processCostData(dataChunkA, dataChunkB, costData, (mergedData) => {
                        mergedDataChunks.push(mergedData);
                        completedInsertions++;

                        if (completedInsertions === expectedInsertions) {
                            // All chunks have been processed
                            for (const mergedData of mergedDataChunks) {
                                insertMergedData(mergedData, () => {
                                    completedInsertions--;

                                    if (completedInsertions === 0) {
                                        // Close the database connection after all insertions are done
                                        db.close((err) => {
                                            if (err) {
                                                console.error('insertion', err.message);
                                            }
                                            console.log('computation finished')
                                        });
                                    }
                                });
                            }
                        }
                    });
                }
            }
        });
    });
});
