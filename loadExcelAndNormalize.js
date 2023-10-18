const fs = require('fs');
const path = require('path');
const ExcelJS = require('exceljs');
const sqlite3 = require('sqlite3').verbose();
const format = require('date-fns/format');
const { arMA } = require('date-fns/locale');

const downloadFolder = path.join(__dirname, '/reportSourceFiles');
const dbFile = 'amm.db';
const db = new sqlite3.Database(dbFile);
const searchText = 'JEN-C';

// Function to create the database/table
const createDatabase = () => {
  db.run(`CREATE TABLE IF NOT EXISTS demanda_diaria (
    fecha_hora TEXT,
    nemo TEXT,
    planta_generadora TEXT,
    potencia_disponible TEXT,
    costo TEXT,
    fpne TEXT,
    banda TEXT
  )`);
}

// Function to read all files
const getExcelFileLocations = () => {
  let fileLocations = [];
  if (fs.existsSync(downloadFolder)) {
    const files = fs.readdirSync(downloadFolder);
    fileLocations = files.map((file) => {
      if (path.extname(file) === '.xlsx') {
        return path.join(downloadFolder, file);
      }
    });
  }
  return fileLocations.filter(Boolean);
}

const readReportTable = async (filePath) => {
  if (!filePath) {
    return;
  }
  const fileName = path.basename(filePath).split('.')[0];
  const dayOfFile = parseInt(fileName.slice(3, 5));
  const monthOfFile = parseInt(fileName.slice(5, 7));
  const yearOfFile = parseInt(fileName.slice(7));
  const rowsToExtract = [];
  const rowsToInsert = [];
  const demanda = {};
  const book = new ExcelJS.Workbook();
  await book.xlsx.readFile(filePath);
  const sheetName = 'LDM';
  const worksheet = book.getWorksheet(sheetName);
  const firstColumn = worksheet.getColumn(1).values;
  let match_counter = 0;

  firstColumn.forEach((content, rowNumber) => {
    if (content && content.toString().includes(searchText)) {
      match_counter++;
      if (match_counter > 1 && match_counter < 5) {
        rowsToExtract.push(rowNumber);
      }
    }
  });

  rowsToExtract.forEach((row, index) => {
    const tempRow = worksheet.getRow(row).values.slice(3, 6);

    if (index === 0) {
      demanda.minima = tempRow;
    } else if (index === 1) {
      demanda.media = tempRow;
    } else {
      demanda.alta = tempRow;
    }
  });

  for (let i = 0; i < 24; i++) {
    const dateTmp = new Date(yearOfFile, monthOfFile - 1, dayOfFile, i, 0);
    const dateTmpFormatted = format(dateTmp, 'yyyy-MM-dd HH:mm:ss');
    let rowToInsert = {
      fecha: dateTmpFormatted,
      nemo: 'JEN-C',
      planta_generadora: 'JAGUAR ENERGY',
    };

    if (i <= 5) {
      rowToInsert.potencia = demanda.minima[0];
      rowToInsert.costo = demanda.minima[1];
      rowToInsert.fpne = demanda.minima[2];
      rowToInsert.banda = 'DEMANDA MINIMA';
    } else if (i <= 17) {
      rowToInsert.potencia = demanda.media[0];
      rowToInsert.costo = demanda.media[1];
      rowToInsert.fpne = demanda.media[2];
      rowToInsert.banda = 'DEMANDA MEDIA';
    } else if (i <= 22) {
      rowToInsert.potencia = demanda.alta[0];
      rowToInsert.costo = demanda.alta[1];
      rowToInsert.fpne = demanda.alta[2];
      rowToInsert.banda = 'DEMANDA MAXIMA';
    } else {
      rowToInsert.potencia = demanda.minima[0];
      rowToInsert.costo = demanda.minima[1];
      rowToInsert.fpne = demanda.minima[2];
      rowToInsert.banda = 'DEMANDA MINIMA';
    }
    rowsToInsert.push(rowToInsert);
  }

  const insertStatement = db.prepare(`INSERT INTO demanda_diaria (fecha_hora, nemo, planta_generadora, potencia_disponible, costo, fpne, banda) VALUES (?,?,?,?,?,?,?)`);

  rowsToInsert.forEach((rowTmp) => {
    insertStatement.run(
      rowTmp.fecha,
      rowTmp.nemo,
      rowTmp.planta_generadora,
      rowTmp.potencia,
      rowTmp.costo,
      rowTmp.fpne,
      rowTmp.banda
    );
  });

  insertStatement.finalize();
};

async function processExcelFilesInBatches(fileLocations, batchSize) {
  for (let i = 0; i < fileLocations.length; i += batchSize) {
    const batch = fileLocations.slice(i, i + batchSize);
    for (const filePath of batch) {
      await readReportTable(filePath);
    }
  }
}

(async () => {
  createDatabase();
  const fileLocations = getExcelFileLocations();
  const batchSize = 5;
  await processExcelFilesInBatches(fileLocations, batchSize);
  console.log("Processing completed.");
})();
