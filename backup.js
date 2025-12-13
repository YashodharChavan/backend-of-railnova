import express from "express";
import cors from "cors";
import multer from "multer";
import * as XLSX from "xlsx";
import jwt from "jsonwebtoken";
import cookieParser from "cookie-parser";
import { createClient } from '@supabase/supabase-js';
import fetch from 'node-fetch';

const app = express();
const port = 3002;

// Multer configuration
const storage = multer.memoryStorage();
const upload = multer({
  storage,
  fileFilter: (req, file, cb) => {
    const validTypes = [
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
      "application/vnd.ms-excel",
    ];
    validTypes.includes(file.mimetype)
      ? cb(null, true)
      : cb(new Error("Only .xlsx and .xls files are allowed"));
  },
});

// Middleware
app.use(cookieParser());
app.use(
  cors({
    origin: true,
    methods: ["GET", "POST"],
    allowedHeaders: ["Content-Type"],
    credentials: true
  })
);
const SECRET = "supersecret";

app.use(express.json());

// Database configuration
const dbConfig = {
  host: "localhost",
  user: "root",
  password: "root@123",
  database: "Railways",
  dateStrings: true,
};

let supabase;
let supabaseUrl = "https://pojmggviqeoezopoiija.supabase.co";
let supabaseAnonKey = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InBvam1nZ3ZpcWVvZXpvcG9paWphIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTUyNDU1MTYsImV4cCI6MjA3MDgyMTUxNn0.9cysU2JShCs0Qn9usUOkGeX71hC8F6MCkpv1xZCpEwI"
async function connectDB() {
  try {
    supabase = await createClient(supabaseUrl, supabaseAnonKey, { global: { fetch } })

    // db = await postgres(connectionString)
  } catch (error) {
    process.exit(1);
  }
}




// Helper functions
function excelSerialToDate(serial) {
  if (typeof serial !== 'number' || isNaN(serial)) return null;
  const utc_days = Math.floor(serial - 25569);
  const utc_value = utc_days * 86400;
  const date_info = new Date(utc_value * 1000);
  const fractional_day = serial - Math.floor(serial) + 0.0001;
  let total_seconds = Math.floor(86400 * fractional_day);
  const seconds = total_seconds % 60;
  total_seconds -= seconds;
  const hours = Math.floor(total_seconds / 3600);
  const minutes = Math.floor(total_seconds / 60) % 60;
  return new Date(Date.UTC(date_info.getUTCFullYear(), date_info.getUTCMonth(), date_info.getUTCDate(), hours, minutes, seconds));
}

function parseDateValue(value) {
  if (value instanceof Date) return value;
  if (typeof value === 'number') {
    return excelSerialToDate(value);
  }
  if (typeof value === 'string' && value.trim() !== '') {
    // Normalize separators
    let val = value.trim().replace(/[-.]/g, "/");

    // Match dd/mm/yyyy or dd/mm/yy (optional time)
    const match = val.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2}|\d{4})(?:\s+(\d{1,2}):(\d{2}))?$/);
    if (match) {
      let [, d, m, y, h = 0, min = 0] = match;
      if (y.length === 2) {
        y = parseInt(y, 10) + 2000; // Assume 2000s for two-digit years
      }
      return new Date(Date.UTC(y, m - 1, d, h, min));
    }
  }
  return null;
}


function formatDateForDB(value) {
  const date = parseDateValue(value);
  if (!date) return null;
  return date.toISOString().slice(0, 10);
}

function formatTimeForDB(value) {
  const date = parseDateValue(value);
  if (!date) return null;
  const hours = date.getUTCHours().toString().padStart(2, '0');
  const mins = date.getUTCMinutes().toString().padStart(2, '0');
  return `${hours}:${mins}`;
}

function cleanYesNo(val) {
  if (!val) return null;
  const str = String(val).trim().toUpperCase();
  if (str === 'Y' || str === 'YES') return 'Y';
  if (str === 'N' || str === 'NO') return 'N';
  return null;
}

const allowedTables = [
  "sc_wadi", "wadi_sc", "gtl_wadi", "wadi_gtl", "ubl_hg", "hg_ubl",
  "ltrr_sc", "sc_ltrr", "pune_dd", "dd_pune", "mrj_pune", "pune_mrj",
  "sc_tjsp", "tjsp_sc"
];


app.get("/api/fetch-data", async (req, res) => {
  const tables = [
    "sc_wadi",
    "gtl_wadi",
    "ubl_hg",
    "ltrr_sc",
    "mrj_pune",
    "pune_dd",
    "sc_tjsp",
  ];

  try {
    const results = {};

    for (const table of tables) {
      const { data, error } = await supabase.from(table).select("*");
      if (error) {
        console.error(`Error fetching ${table}:`, error);
        results[table] = { error: error.message };
        continue; // skip this table but continue with others
      }
      results[table] = data;
    }

    res.json({ success: true, data: results });
  } catch (err) {
    res.status(500).json({ success: false, message: err.message });
  }
});


// Process Excel and update database
app.post("/api/upload", upload.single("file"), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ success: false, message: "No file uploaded" });
    }

    // Parse Excel file
    const workbook = XLSX.read(req.file.buffer, { type: "buffer", cellDates: false });
    const worksheet = workbook.Sheets[workbook.SheetNames[0]];
    const data = XLSX.utils.sheet_to_json(worksheet, { header: 1, defval: '' });

    // Process routes
    const processedRoutes = await processExcelData(data);
    res.json({
      success: true,
      message: "Database updated successfully",
      routes: processedRoutes,
    });
  } catch (error) {
    console.error("Upload error:", error);
    res.status(500).json({
      success: false,
      message: error.message || "File processing failed"
    });
  }
});

function normalizeRoute(rawRoute) {
  if (!rawRoute || typeof rawRoute !== "string") return "";

  rawRoute = rawRoute.trim();
  if (!rawRoute) return "";

  // normalize dash and remove extra spaces
  const parts = rawRoute.split(/\s*-\s*/); // split on "-" with optional spaces
  if (parts.length !== 2) {
    return "";
  }
  return parts[0].toLowerCase() + "_" + parts[1].toLowerCase();
}


// Core processing function
async function processExcelData(data) {
  const ROUTE_COL = 29; // Column AD (0-based index)
  const processedRoutes = [];
  let currentRoute = null;
  let startRow = 3; // Data starts at row 4 (0-based index 3)
  for (let i = 3; i < data.length; i++) {
    const route = (data[i][ROUTE_COL] || '').toString().trim();
    if (!route) {
      continue;
    }
    const normalizedRoute = normalizeRoute(route);
    if (!allowedTables.includes(normalizedRoute)) {
      console.warn(`Skipping unknown route: ${normalizedRoute} actual route is ${route}`);
      return;
    }

    // New route block detected
    if (normalizedRoute && normalizedRoute !== currentRoute) {
      if (currentRoute) {
        // Process previous block
        const result = await processRouteBlock(
          currentRoute,
          data,
          startRow,
          i - 1
        );
        processedRoutes.push(result);
      }
      currentRoute = normalizedRoute;
      startRow = i;
    }
  }

  // Process last route block
  if (currentRoute) {
    const result = await processRouteBlock(
      currentRoute,
      data,
      startRow,
      data.length - 1
    );
    processedRoutes.push(result);
  }

  return processedRoutes;
}

// Process a single route block
async function processRouteBlock(route, data, startRow, endRow) {
  const [src, dest] = route.split("_");
  const reverseRoute = `${dest}_${src}`;
  // Extract data for both directions
  const srcDestData = extractRakeData(data, startRow, endRow, "SRC-DEST");
  const destSrcData = extractRakeData(data, startRow, endRow, "DEST-SRC");


  // Update database
  await updateRouteTable(route, srcDestData);
  await updateRouteTable(reverseRoute, destSrcData);

  return {
    route,
    reverseRoute,
    srcDestCount: srcDestData.length,
    destSrcCount: destSrcData.length
  };
}
// Extract rake data with all columns
function extractRakeData(data, startRow, endRow, direction) {
  const config = direction === "SRC-DEST"
    ? {
      rakeId: 1,     // B
      from: 4,       // E
      to: 5,         // F
      type: 2,       // C
      isLoaded: 3,   // D
      loco: 10,      // K
      base: 12,      // M
      dueDate: 14,   // O
      wagon: 7,      // H
      bpcStn: 18,    // S
      bpcDate: 17,   // R
      bpcType: 15,   // P
      arrival: 25,   // Z
      stts: 23,      // X
      loc: 24,       // Y
      ic: 27,        // AB
      fc: 26         // AA
    }
    : {
      rakeId: 32,    // AG
      from: 35,      // AJ
      to: 36,        // AK
      type: 33,      // AH
      isLoaded: 34,  // AI
      loco: 41,      // AP
      base: 43,      // AR
      dueDate: 45,   // AT
      wagon: 38,     // AM
      bpcStn: 49,    // AX
      bpcDate: 48,   // AW
      bpcType: 46,   // AU
      arrival: 56,   // BE
      stts: 54,      // BC
      loc: 55,       // BD
      ic: 58,        // BG
      fc: 57         // BF
    };

  const rakes = [];
  let r = startRow;

  while (r <= endRow) {
    const row = data[r] || [];
    const nextRow = r + 1 <= endRow ? (data[r + 1] || []) : [];

    const rakeId = row[config.rakeId] !== undefined ? (row[config.rakeId] || '').toString().trim() : '';

    // Skip empty rake IDs
    if (!rakeId) {
      r++;
      continue;
    }

    // Check if next row has no rake ID (potential second loco)
    const nextRakeId = nextRow[config.rakeId] !== undefined ? (nextRow[config.rakeId] || '').toString().trim() : '';
    let hasSecondLoco = nextRakeId === '' && r + 1 <= endRow;

    // Extract loco numbers
    const loco1 = row[config.loco] !== undefined ? (row[config.loco] || '').toString().trim() : '';
    const loco2 = hasSecondLoco ? (nextRow[config.loco] !== undefined ? (nextRow[config.loco] || '').toString().trim() : '') : '';

    // Check for more than 2 loco numbers
    const allLocos = [loco1, loco2]
      .filter(Boolean)
      .join(',')
      .split(/[\s,/|]+/)
      .filter(Boolean);

    if (allLocos.length > 2) {
      r += hasSecondLoco ? 2 : 1;
      continue;
    }

    // Get first non-empty value for fields that might have duplicates
    const getFirstValue = (col) => {
      let val = row[col] !== undefined ? row[col] : '';
      if (val !== '' && val !== null) return val;

      if (hasSecondLoco) {
        val = nextRow[col] !== undefined ? nextRow[col] : '';
        if (val !== '' && val !== null) return val;
      }

      return null;
    };

    // Create rake object
    const rake = {
      rakeId: rakeId,
      from: row[config.from] !== undefined ? (row[config.from] || '').toString().trim() : null,
      to: row[config.to] !== undefined ? (row[config.to] || '').toString().trim() : null,
      type: row[config.type] !== undefined ? (row[config.type] || '').toString().trim() : null,
      isLoaded: row[config.isLoaded] !== undefined ? (row[config.isLoaded] || '').toString().trim() : null,
      loco1: loco1 || null,
      loco2: loco2 || null,
      base: getFirstValue(config.base) ? (getFirstValue(config.base) || '').toString().trim() : null,
      dueDate: formatDateForDB(getFirstValue(config.dueDate)),
      wagon: getFirstValue(config.wagon) ? (getFirstValue(config.wagon) || '').toString().trim() : null,
      bpcStn: getFirstValue(config.bpcStn) ? (getFirstValue(config.bpcStn) || '').toString().trim() : null,
      bpcDate: formatDateForDB(getFirstValue(config.bpcDate)),
      bpcType: getFirstValue(config.bpcType) ? (getFirstValue(config.bpcType) || '').toString().trim() : null,
      arrival: formatTimeForDB(getFirstValue(config.arrival)),
      stts: getFirstValue(config.stts) ? (getFirstValue(config.stts) || '').toString().trim() : null,
      loc: getFirstValue(config.loc) ? (getFirstValue(config.loc) || '').toString().trim() : null,
      ic: cleanYesNo(getFirstValue(config.ic)),
      fc: cleanYesNo(getFirstValue(config.fc))
    };

    rakes.push(rake);

    // Move to next row
    r += hasSecondLoco ? 2 : 1;
  }

  return rakes;
}


// Update database table
async function updateRouteTable(tableName, rakes) {
  if (!rakes.length) return;

  try {
    // 🚨 Clear the table before inserting new data
    const { error: truncateError } = await supabase
      .from(tableName)
      .delete()
      .neq('rake_id', 0); // Delete all records (assuming 'id' exists)

    if (truncateError) throw truncateError;

    // Prepare data for insertion
    const records = rakes.map(rake => ({
      "rake_id": rake.rakeId ? rake.rakeId.trim() : null,
      "from_station": rake.from,
      "to_station": rake.to,
      "type": rake.type,
      "isloaded": rake.isLoaded,
      "loco1": rake.loco1,
      "loco2": rake.loco2,
      "base": rake.base,
      "due_date": rake.dueDate,
      "wagon": rake.wagon ? parseInt(rake.wagon, 10) : null, // Ensure integer
      "bpc_stn": rake.bpcStn,
      "bpc_date": rake.bpcDate,
      "bpc_type": rake.bpcType,
      "arrival": rake.arrival,
      "stts": rake.stts,
      "loc": rake.loc,
      "ic": rake.ic,
      "fc": rake.fc
    }));

    function dedupeBatch(batch, key = "rake_id") {
      const seen = new Set();
      return batch.filter(item => {
        if (seen.has(item[key])) return false;
        seen.add(item[key]);
        return true;
      });
    }

    // Insert in batches (Supabase has a limit per request)
    const BATCH_SIZE = 100;
    let insertedCount = 0;

    for (let i = 0; i < records.length; i += BATCH_SIZE) {
      const batch = records.slice(i, i + BATCH_SIZE);
      const uniqueBatch = dedupeBatch(batch);

      const { data, error } = await supabase
        .from(tableName)
        .upsert(uniqueBatch, { onConflict: ['rake_id'] });

      if (error) {
        console.error(`Insert error for table ${tableName}:`, error.message, error.details || "");
      }

      insertedCount += batch.length;
    }


    return insertedCount;

  } catch (error) {
  }
}

app.get("/health", (req, res) => {
  res.json({ status: "Server is running" });
});



async function startServer() {
  await connectDB();
  app.listen(port, () => console.log(`Server running on port ${port}`));
}

startServer();