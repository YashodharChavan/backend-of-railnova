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
    let val = value.trim().replace(/[-.]/g, "/");
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
  if (['Y', 'YES', 'TRUE', '1'].includes(str)) return 'Y';
  if (['N', 'NO', 'FALSE', '0'].includes(str)) return 'N';
  console.warn(`Unexpected isLoaded value: "${val}" (converted to ${str})`); // Debug log
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
      const { data, error } = await supabase
        .from(table)
        .select("*")
        .order("seq", { ascending: true });
      if (error) {
        console.error(`Error fetching ${table}:`, error);
        results[table] = { error: error.message };
        continue;
      }
      results[table] = data;
    }

    res.json({ success: true, data: results });
  } catch (err) {
    res.status(500).json({ success: false, message: err.message });
  }
});

app.post("/api/upload", upload.single("file"), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ success: false, message: "No file uploaded" });
    }

    const workbook = XLSX.read(req.file.buffer, { type: "buffer", cellDates: false });
    const worksheet = workbook.Sheets[workbook.SheetNames[0]];
    const data = XLSX.utils.sheet_to_json(worksheet, { header: 1, defval: '' });

    const processedRoutes = await processExcelData(data);
    // await applyOverrides(); // Removed as applyOverrides is not defined
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
  const parts = rawRoute.split(/\s*-\s*/);
  if (parts.length !== 2) {
    return "";
  }
  return parts[0].toLowerCase() + "_" + parts[1].toLowerCase();
}

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
      continue; // Changed return to continue to process all routes
    }

    if (normalizedRoute && normalizedRoute !== currentRoute) {
      if (currentRoute) {
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

async function processRouteBlock(route, data, startRow, endRow) {
  const [src, dest] = route.split("_");
  const reverseRoute = `${dest}_${src}`;
  const srcDestData = extractRakeData(data, startRow, endRow, "SRC-DEST");
  // const destSrcData = extractRakeData(data, startRow, endRow, "DEST-SRC");

  await updateRouteTable(route, srcDestData);
  // await updateRouteTable(reverseRoute, destSrcData);

  return {
    route,
    reverseRoute,
    srcDestCount: srcDestData.length,
    // destSrcCount: destSrcData.length
  };
}

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
    if (!rakeId) {
      r++;
      continue;
    }

    const nextRakeId = nextRow[config.rakeId] !== undefined ? (nextRow[config.rakeId] || '').toString().trim() : '';
    let hasSecondLoco = nextRakeId === '' && r + 1 <= endRow;

    const loco1 = row[config.loco] !== undefined ? (row[config.loco] || '').toString().trim() : '';
    const loco2 = hasSecondLoco ? (nextRow[config.loco] !== undefined ? (nextRow[config.loco] || '').toString().trim() : '') : '';

    const allLocos = [loco1, loco2]
      .filter(Boolean)
      .join(',')
      .split(/[\s,/|]+/)
      .filter(Boolean);

    if (allLocos.length > 2) {
      r += hasSecondLoco ? 2 : 1;
      continue;
    }

    const getFirstValue = (col) => {
      let val = row[col] !== undefined ? row[col] : '';
      if (col === config.isLoaded) {
        console.log(`Raw isLoaded value for rake ${rakeId} (row ${r + 1}, direction ${direction}): "${val}"`);
      }
      if (val !== '' && val !== null) return val;

      if (hasSecondLoco) {
        val = nextRow[col] !== undefined ? nextRow[col] : '';
        if (col === config.isLoaded) {
          console.log(`Raw isLoaded value for rake ${rakeId} (next row ${r + 2}, direction ${direction}): "${val}"`);
        }
        if (val !== '' && val !== null) return val;
      }

      return null;
    };

    const rake = {
      rakeId: rakeId,
      from: row[config.from] !== undefined ? (row[config.from] || '').toString().trim() : null,
      to: row[config.to] !== undefined ? (row[config.to] || '').toString().trim() : null,
      type: row[config.type] !== undefined ? (row[config.type] || '').toString().trim() : null,
      isLoaded: getFirstValue(config.isLoaded), 
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
    r += hasSecondLoco ? 2 : 1;
  }

  return rakes;
}

function dedupeRakesById(rakes) {
  const seen = new Set();
  return rakes.filter(r => {
    const id = r.rakeId?.trim();
    if (!id || seen.has(id)) return false;
    seen.add(id);
    return true;
  });
}

async function updateRouteTable(tableName, rakes) {
  if (!rakes.length) return;

  try {
    rakes = dedupeRakesById(rakes);
    const { data: existingRows, error: fetchError } = await supabase
      .from(tableName)
      .select("rake_id");
    if (fetchError) throw fetchError;

    const existingRakeIds = existingRows.map(r => r.rake_id);
    const updates = [];
    const inserts = [];

    rakes.forEach(rake => {
      const rakeId = rake.rakeId?.trim();
      if (!rakeId) return;

      const parsedWagon = rake.wagon ? parseInt(rake.wagon, 10) : null;
      const record = {
        rake_id: rakeId,
        from_station: rake.from || null,
        to_station: rake.to || null,
        type: rake.type || null,
        isloaded: rake.isLoaded || null,
        loco1: rake.loco1 || null,
        loco2: rake.loco2 || null,
        base: rake.base || null,
        due_date: rake.dueDate || null,
        wagon: isNaN(parsedWagon) ? null : parsedWagon,
        bpc_stn: rake.bpcStn || null,
        bpc_date: rake.bpcDate || null,
        bpc_type: rake.bpcType || null,
        arrival: rake.arrival || null,
        stts: rake.stts || null,
        loc: rake.loc || null,
        ic: rake.ic || null,
        fc: rake.fc || null
      };

      if (existingRakeIds.includes(rakeId)) {
        updates.push(record);
      } else {
        inserts.push(record);
      }
    });

    if (updates.length) {
      const { error: upsertError } = await supabase
        .from(tableName)
        .upsert(updates, { onConflict: ["rake_id"] });
      if (upsertError) console.error(`Upsert error for ${tableName}:`, upsertError);
    }

    if (inserts.length) {
      const { error: insertError } = await supabase
        .from(tableName)
        .insert(inserts);
      if (insertError) console.error(`Insert error for ${tableName}:`, insertError);
    }

    return { updated: updates.length, inserted: inserts.length };

  } catch (error) {
    console.error("Error updating table", tableName, error);
    throw error;
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