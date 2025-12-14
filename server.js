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
    origin: "https://rail-nova.vercel.app",
    methods: ["GET", "POST"],
    allowedHeaders: ["Content-Type", "Authorization"],
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

app.post("/api/login", async (req, res) => {
  try {
    const { username, password } = req.body;

    if (!username || !password) {
      return res.status(400).json({ success: false, message: "Username and password are required" });
    }

    // 🔹 Fetch user from Supabase users table
    const { data: users, error } = await supabase
      .from("users")
      .select("*")
      .eq("username", username)
      .limit(1);

    if (error) {
      console.error("Supabase fetch error:", error);
      return res.status(500).json({ success: false, message: "Database error while fetching user" });
    }

    if (!users || users.length === 0) {
      return res.status(401).json({ success: false, message: "Invalid username" });
    }

    const user = users[0];

    // ⚠️ Compare plain text passwords (for now)
    // In future: store hashed passwords and use bcrypt.compare()
    if (password !== user.password) {
      return res.status(401).json({ success: false, message: "Invalid password" });
    }

    // ✅ Create a JWT token
    const token = jwt.sign(
      { id: user.id, username: user.username, role: user.role },
      SECRET,
      { expiresIn: "2h" }
    );

    // Store token in a cookie
    res.cookie("token", token, { httpOnly: true, sameSite: "lax" });

    // Send response
    res.json({
      success: true,
      message: "Login successful",
      user: {
        id: user.id,
        username: user.username,
        firstname: user.firstname,
        lastname: user.lastname,
        email: user.email,
        role: user.role,
        designation: user.designation
      },
      token
    });

  } catch (err) {
    console.error("Login error:", err);
    res.status(500).json({ success: false, message: "Internal server error" });
  }
});

app.get("/api/forecast-vs-actual", async (req, res) => {
  try {
    // Fetch data from forecast_data table
    const { data: rows, error } = await supabase
      .from("forecast_data")
      .select("arrival, fc, ic")
      .not("arrival", "is", null);

    if (error) throw error;

    // Initialize counters for each time period
    const results = {
      Morning: { forecasted: 0, actual: 0 },
      Afternoon: { forecasted: 0, actual: 0 },
      Evening: { forecasted: 0, actual: 0 },
      Night: { forecasted: 0, actual: 0 }
    };

    // Group data by time period
    rows.forEach(row => {
      if (!row.arrival || typeof row.arrival !== "string") return;

      const timeMatch = row.arrival.match(/^(\d{2}):(\d{2}):(\d{2})$/);
      if (!timeMatch) return;

      const hour = parseInt(timeMatch[1], 10);
      let period;

      if (hour >= 6 && hour <= 11) period = "Morning";
      else if (hour >= 12 && hour <= 17) period = "Afternoon";
      else if (hour >= 18 && hour <= 23) period = "Evening";
      else period = "Night";

      if (row.fc === "Y") results[period].forecasted++;
      if (row.ic === "Y") results[period].actual++;
    });

    // Convert to chart-friendly format
    const chartData = Object.entries(results).map(([period, counts]) => ({
      period,
      forecasted: counts.forecasted,
      actual: counts.actual
    }));

    // Sort order: Morning → Afternoon → Evening → Night
    chartData.sort(
      (a, b) =>
        ["Morning", "Afternoon", "Evening", "Night"].indexOf(a.period) -
        ["Morning", "Afternoon", "Evening", "Night"].indexOf(b.period)
    );

    res.json({
      success: true,
      data: chartData
    });
  } catch (err) {
    console.error("Error in /api/forecast-vs-actual:", err);
    res.status(500).json({
      success: false,
      message: "Server error fetching forecast vs actual data"
    });
  }
});


app.get("/api/wagon-totals", async (req, res) => {
  // Simple retry helper for transient errors
  const retry = async (fn, retries = 3, delay = 1000) => {
    for (let i = 0; i < retries; i++) {
      try {
        return await fn();
      } catch (err) {
        if (i === retries - 1) throw err;
        console.warn(`Retry ${i + 1}/${retries} failed: ${err.message}`);
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }
  };

  try {
    console.time("Supabase wagon-totals query");

    // Fetch from Supabase view/table `wagon_totals_data`
    const { data, error } = await retry(async () => {
      const result = await supabase
        .from("wagon_totals_data")
        .select("wagon, isloaded")
        .range(0, 999); // limit 1000 rows
      if (result.error) throw result.error;
      return result;
    });

    console.timeEnd("Supabase wagon-totals query");

    if (error) throw error;

    console.log("Supabase raw data length:", data?.length || 0);
    console.log("Supabase raw data sample:", data?.slice(0, 5));

    // Initialize totals
    let totalLoaded = 0;
    let totalEmpty = 0;

    // Process each row
    (data || []).forEach(row => {
      const wagons = Number(row.wagon) || 0;
      if (row.isloaded === "L") {
        totalLoaded += wagons;
      } else if (row.isloaded === "E") {
        totalEmpty += wagons;
      }
    });

    // Prepare final response
    const resultData = [
      { name: "Loaded Wagons", value: totalLoaded },
      { name: "Empty Wagons", value: totalEmpty }
    ];

    res.json({
      success: true,
      data: resultData
    });
  } catch (err) {
    console.error("Error in /api/wagon-totals:", {
      message: err.message,
      stack: err.stack || "No stack trace",
      details: err.details || "No additional details",
      code: err.code || "No code provided"
    });

    res.status(500).json({
      success: false,
      message: "Server error fetching wagon totals",
      error: err.message
    });
  }
});

function authenticateUser(req, res, next) {
  const token = req.cookies.token;
  if (!token) {
    return res.status(401).json({ message: "Not authenticated" });
  }
  try {
    const decoded = jwt.verify(token, SECRET);
    req.user = decoded; // { id, username, role }
    next();
  } catch (err) {
    res.status(403).json({ message: "Invalid token" });
  }
}
app.get("/api/ic-fc-stats", async (req, res) => {
  const tablePairs = [
    { src: "sc", dest: "wadi" },
    { src: "gtl", dest: "wadi" },
    { src: "ubl", dest: "hg" },
    { src: "ltrr", dest: "sc" },
    { src: "pune", dest: "dd" },
    { src: "mrj", dest: "pune" },
    { src: "sc", dest: "tjsp" }
  ];

  // Simple retry helper for transient Supabase connection issues
  const retry = async (fn, retries = 3, delay = 1000) => {
    for (let i = 0; i < retries; i++) {
      try {
        return await fn();
      } catch (err) {
        if (i === retries - 1) throw err;
        console.warn(`Retry ${i + 1}/${retries} failed: ${err.message}`);
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }
  };

  try {
    console.time("Supabase ic-fc-stats query");

    // Build route names for both directions
    const routes = tablePairs.flatMap(pair => [
      `${pair.src}_${pair.dest}`,
      `${pair.dest}_${pair.src}`
    ]);

    // Fetch IC and FC stats for all routes
    const { data, error } = await retry(async () => {
      const result = await supabase
        .from("ic_fc_stats_data")
        .select("route, ic, fc")
        .in("route", routes)
        .range(0, 999); // limit 1000 rows
      if (result.error) throw result.error;
      return result;
    });

    console.timeEnd("Supabase ic-fc-stats query");

    if (error) throw error;

    console.log("Supabase raw data length:", data?.length || 0);
    console.log("Supabase raw data sample:", data?.slice(0, 5));

    // Initialize counts
    const tableCounts = {};
    routes.forEach(route => {
      tableCounts[route] = { ic: 0, fc: 0 };
    });

    // Process rows
    (data || []).forEach(row => {
      if (tableCounts[row.route]) {
        if (row.ic === "Y") tableCounts[row.route].ic++;
        if (row.fc === "Y") tableCounts[row.route].fc++;
      }
    });

    // Format final result
    const results = tablePairs.map(pair => {
      const forwardTable = `${pair.src}_${pair.dest}`;
      const reverseTable = `${pair.dest}_${pair.src}`;
      return {
        pair: forwardTable,
        directions: [
          {
            direction: "forward",
            tableName: forwardTable,
            IC: tableCounts[forwardTable].ic,
            FC: tableCounts[forwardTable].fc
          },
          {
            direction: "reverse",
            tableName: reverseTable,
            IC: tableCounts[reverseTable].ic,
            FC: tableCounts[reverseTable].fc
          }
        ]
      };
    });

    // Send structured response
    res.json({
      success: true,
      data: results
    });

  } catch (err) {
    console.error("Error in /api/ic-fc-stats:", {
      message: err.message,
      stack: err.stack || "No stack trace",
      details: err.details || "No additional details",
      code: err.code || "No code provided"
    });

    res.status(500).json({
      success: false,
      message: "Server error fetching IC/FC stats",
      error: err.message
    });
  }
});

app.get("/api/ic-stats", async (req, res) => {
  // Simple retry utility
  const retry = async (fn, retries = 3, delay = 1000) => {
    for (let i = 0; i < retries; i++) {
      try {
        return await fn();
      } catch (err) {
        if (i === retries - 1) throw err;
        console.warn(`Retry ${i + 1}/${retries} failed: ${err.message}`);
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }
  };

  try {
    console.time("Supabase ic-stats query");

    // Fetch from Supabase table or view `ic_stats_data`
    const { data, error } = await retry(async () => {
      const result = await supabase
        .from("ic_stats_data")
        .select("ic")
        .range(0, 999); // Limit to 1000 rows (adjust as needed)
      if (result.error) throw result.error;
      return result;
    });

    console.timeEnd("Supabase ic-stats query");

    if (error) throw error;

    console.log("Supabase raw data length:", data?.length || 0);
    console.log("Supabase raw data sample:", data?.slice(0, 5));

    // Initialize counts
    let totalIC = 0;
    let totalTrains = 0;

    (data || []).forEach(row => {
      totalTrains++;
      if (row.ic === "Y") totalIC++;
    });

    // Prepare frontend chart data
    const dataResponse = [
      { name: "Interchanged Trains", value: totalIC },
      { name: "Non-Interchanged Trains", value: totalTrains - totalIC }
    ];

    res.json({
      success: true,
      data: dataResponse
    });
  } catch (err) {
    console.error("Error in /api/ic-stats:", {
      message: err.message,
      details: err.details || "No additional details",
      hint: err.hint || "No hint provided",
      code: err.code || "No code",
      stack: err.stack || "No stack trace"
    });

    res.status(500).json({
      success: false,
      message: "Server error fetching IC stats",
      error: err.message
    });
  }
});


app.get("/api/dashboard-stats", async (req, res) => {
  // Define all route sections (same as table names)
  const tables = [
    "sc_wadi", "wadi_sc", "gtl_wadi", "wadi_gtl", "ubl_hg", "hg_ubl",
    "ltrr_sc", "sc_ltrr", "pune_dd", "dd_pune", "mrj_pune", "pune_mrj",
    "sc_tjsp", "tjsp_sc"
  ];

  try {
    const { data, error } = await supabase
      .from("dashboard_stats_data")
      .select("route, ic, fc");

    if (error) {
      console.error("Supabase fetch error:", error);
      return res.status(500).json({
        success: false,
        message: "Error fetching dashboard data",
        error: error.message,
      });
    }

    if (!data || data.length === 0) {
      return res.json({
        success: true,
        stats: { totalTrains: 0, totalInterchange: 0, totalForecast: 0 },
        breakdown: tables.map((table) => ({ table, count: 0 })),
      });
    }

    let totalICCount = 0;
    let totalFCCount = 0;
    let totalTrainCount = 0;

    const perTableCounts = {};
    tables.forEach((table) => (perTableCounts[table] = { count: 0 }));

    for (const row of data) {
      totalTrainCount++;

      if (row.ic === "Y") {
        totalICCount++;
        if (perTableCounts[row.route]) perTableCounts[row.route].count++;
      }

      if (row.fc === "Y") {
        totalFCCount++;
      }
    }

    const breakdown = Object.entries(perTableCounts).map(([table, { count }]) => ({
      table,
      count,
    }));

    res.json({
      success: true,
      stats: {
        totalTrains: totalTrainCount,
        totalInterchange: totalICCount,
        totalForecast: totalFCCount,
      },
      breakdown,
    });

  } catch (err) {
    console.error("Error in /api/dashboard-stats:", err);
    res.status(500).json({
      success: false,
      message: "Server error fetching dashboard statistics",
      error: err.message,
    });
  }
});


app.get("/api/get-user-and-role", authenticateUser, async (req, res) => {
  try {
    // req.user should have been set by your authenticateUser middleware (from JWT)
    const userId = req.user.id;

    // Fetch fresh user data from Supabase (optional but more accurate)
    const { data: userData, error } = await supabase
      .from("users")
      .select("username, role, designation, email, firstname, lastname")
      .eq("id", userId)
      .single();

    if (error || !userData) {
      return res.status(404).json({ success: false, message: "User not found" });
    }

    // Respond with structured data
    res.json({
      success: true,
      username: userData.username,
      role: userData.role,
      designation: userData.designation,
      email: userData.email,
      firstName: userData.firstname,
      lastName: userData.lastname
    });
  } catch (err) {
    console.error("Error fetching user:", err);
    res.status(500).json({ success: false, message: "Server error" });
  }
});

app.get("/api/fetch-handing-over", async (req, res) => {
  const tables = [
    "wadi_sc", "wadi_gtl", "hg_ubl", "sc_ltrr", "pune_mrj", "dd_pune", "tjsp_sc"
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