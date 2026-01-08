import NodeCache from 'node-cache';
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

const allowedOrigins = [
  "https://rail-nova.vercel.app",
  "http://localhost:5173"
];

// Middleware
app.use(cookieParser());
app.use(
  cors({
    origin: function (origin, callback) {
      // allow non-browser tools like Postman
      if (!origin) return callback(null, true);

      if (allowedOrigins.includes(origin)) {
        callback(null, true);
      } else {
        callback(new Error("Not allowed by CORS"));
      }
    },
    credentials: true,
    methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization"],
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
    console.log("database connected successfully")
  } catch (error) {
    console.error("Database connection error:", error);
    process.exit(1);
  }
}

function isDSL(loco) {
  if (!loco) return false;
  const prefixes = ["11", "12", "13", "14", "49", "69", "70"];
  return prefixes.some(prefix => loco.startsWith(prefix));
}

app.get("/analysis/locos", async (req, res) => {
  const tables = [
    "sc_wadi", "wadi_sc", "gtl_wadi", "wadi_gtl", "ubl_hg", "hg_ubl",
    "ltrr_sc", "sc_ltrr", "pune_dd", "dd_pune", "mrj_pune", "pune_mrj",
    "sc_tjsp", "tjsp_sc"
  ];

  try {
    const cacheKey = 'locos_stats';
    const cachedData = cache.get(cacheKey);
    if (cachedData) return res.json(cachedData);

    const { data, error } = await supabase
      .from('analysis_locos')
      .select('route, loco1, loco2');

    if (error) throw error;

    const tableCounts = {};
    tables.forEach(table => {
      tableCounts[table] = { dslTotal: 0, acTotal: 0, dslMulti: 0, acMulti: 0 };
    });

    (data || []).forEach(row => {
      if (!tableCounts[row.route]) return;

      const l1 = row.loco1?.trim();
      const l2 = row.loco2?.trim();

      // count loco1
      if (l1) {
        if (isDSL(l1)) {
          tableCounts[row.route].dslTotal++;
        } else {
          tableCounts[row.route].acTotal++;
        }
      }

      // count loco2
      if (l2) {
        if (isDSL(l2)) {
          tableCounts[row.route].dslTotal++;   // add to total
          tableCounts[row.route].dslMulti++;   // add to multi
        } else {
          tableCounts[row.route].acTotal++;
          tableCounts[row.route].acMulti++;
        }
      }
    });

    const results = tables.map(table => ({
      route: table,
      DSL: {
        total: tableCounts[table].dslTotal,
        multi: tableCounts[table].dslMulti
      },
      AC: {
        total: tableCounts[table].acTotal,
        multi: tableCounts[table].acMulti
      }
    }));

    cache.set(cacheKey, results);
    res.json(results);
  } catch (err) {
    console.error("Error in /analysis/locos:", err);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.get("/api/wagon-totals", async (req, res) => {
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
    // Query the view directly
    console.time('Supabase wagon-totals query');
    const { data, error } = await retry(async () => {
      return await supabase
        .from('wagon_totals_data')
        .select('wagon, isloaded')
        .range(0, 999); // Limit to 1000 rows
    });
    console.timeEnd('Supabase wagon-totals query');

    if (error) {
      console.error('Error fetching view:', {
        message: error.message,
        details: error.details || 'No details provided',
        hint: error.hint || 'No hint provided',
        code: error.code || 'No code provided'
      });
      throw error;
    }

    // Log raw data for debugging
    console.log('Supabase raw data length:', data?.length || 0);
    console.log('Supabase raw data sample:', data?.slice(0, 5));

    // Initialize totals
    let totalLoaded = 0;
    let totalEmpty = 0;

    // Process rows
    (data || []).forEach(row => {
      if (row.isloaded === 'L') {
        totalLoaded += Number(row.wagon) || 0; // Ensure wagon is treated as integer
      } else if (row.isloaded === 'E') {
        totalEmpty += Number(row.wagon) || 0;
      }
    });

    const resultData = [
      { name: "Loaded Wagons", value: totalLoaded },
      { name: "Empty Wagons", value: totalEmpty }
    ];

    const response = { success: true, data: resultData };

    res.json(response);
  } catch (err) {
    console.error("Error in /api/wagon-totals:", {
      message: err.message,
      stack: err.stack || 'No stack trace',
      details: err.details || 'No additional details',
      code: err.code || 'No code provided'
    });
    res.status(500).json({
      success: false,
      message: "Server error fetching wagon totals",
      error: err.message
    });
  }
});

app.get("/", (req, res)=> {

  res.json({success: true});
})

app.get("/api/ic-stats", async (req, res) => {
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
    // Query the view directly
    console.time('Supabase ic-stats query');
    const { data, error } = await retry(async () => {
      return await supabase
        .from('ic_stats_data')
        .select('ic')
        .range(0, 999); // Limit to 1000 rows
    });
    console.timeEnd('Supabase ic-stats query');

    if (error) {
      console.error('Error fetching view:', {
        message: error.message,
        details: error.details || 'No details provided',
        hint: error.hint || 'No hint provided',
        code: error.code || 'No code provided'
      });
      throw error;
    }

    // Log raw data for debugging
    console.log('Supabase raw data length:', data?.length || 0);
    console.log('Supabase raw data sample:', data?.slice(0, 5));

    // Count IC and total trains
    let totalIC = 0;
    let totalTrains = 0;

    (data || []).forEach(row => {
      if (row.ic === 'Y') {
        totalIC++;
      }
      totalTrains++;
    });

    const dataResponse = [
      { name: "Interchanged Trains", value: totalIC },
      { name: "Non-Interchanged Trains", value: totalTrains - totalIC }
    ];

    const response = { success: true, data: dataResponse };

    res.json(response);
  } catch (err) {
    console.error("Error in /api/ic-stats:", {
      message: err.message,
      stack: err.stack || 'No stack trace',
      details: err.details || 'No additional details',
      code: err.code || 'No code provided'
    });
    res.status(500).json({
      success: false,
      message: "Server error fetching IC stats",
      error: err.message
    });
  }
});

app.post("/api/login", async (req, res) => {
  const { username, password } = req.body;
  try {
    // Supabase equivalent of the MySQL query
    const { data: users, error } = await supabase
      .from('users')
      .select('*')
      .eq('username', username.trim())
      .eq('password', password);

    if (error) throw error;

    if (users && users.length > 0) {
      const user = users[0];

      // Create JWT token (same as original)
      const token = jwt.sign(
        {
          id: user.id,
          username: user.username,
          role: user.role,
          designation: user.designation,
          firstName: user.firstname,
          lastName: user.lastname,
          email: user.email
        },
        SECRET,
        { expiresIn: "1d" }
      );

      // Send as httpOnly cookie (same as original)
      res.cookie("token", token, {
        httpOnly: true,
        secure: true,
        sameSite: "none",
        maxAge: 24 * 60 * 60 * 1000
      });

      return res.json({
        success: true,
        token, // 🔑 REQUIRED for Bearer auth
      });

    } else {
      f
      return res.json({ success: false, message: "Invalid username or password" });
    }
  } catch (error) {
    res.status(500).json({ success: false, message: "Error connecting to server" });
  }
});

app.post("/api/logout", (req, res) => {
  res.clearCookie("token", {
    httpOnly: true,
    secure: false,
    sameSite: "lax"
  });
  res.json({ success: true });
});

app.get("/auth/check", (req, res) => {
  const token = req.cookies.token;
  if (!token) return res.json({ loggedIn: false });

  try {
    const decoded = jwt.verify(token, SECRET);
    res.json({ loggedIn: true, user: decoded });
  } catch {
    res.json({ loggedIn: false });
  }
});




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


// Add this route to server.js

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


app.get("/api/summary/:type", async (req, res) => {
  const type = (req.params.type || "").toLowerCase();
  const allowedTypes = ["master", "forecasted", "interchanged", "remaining"];
  if (!allowedTypes.includes(type)) {
    return res.status(400).json({ success: false, message: "Invalid summary type" });
  }

  try {
    // Check cache first
    const cacheKey = `summary_${type}`;
    const cachedData = cache.get(cacheKey);
    if (cachedData) {
      console.log(`Cache hit for summary_${type}`);
      return res.json(cachedData);
    }

    console.time("dbQuery");

    // Select the appropriate view based on type
    const viewName = `summary_${type === "interchanged" ? "interchanged" : type}`;
    const { data, error } = await supabase
      .from(viewName)
      .select('route, *');

    if (error) {
      console.error(`Error fetching view ${viewName}:`, {
        message: error.message,
        details: error.details,
        hint: error.hint,
        code: error.code
      });
      throw error;
    }


    // Group data by table
    const results = [];
    const tableData = {};
    (data || []).forEach(row => {
      if (!tableData[row.route]) {
        tableData[row.route] = [];
      }
      // Remove route from row to match original data structure
      const { route, ...rowData } = row;
      tableData[row.route].push(rowData);
    });

    // Build results in the same order as routePairs
    const routePairs = [
      ["sc_wadi", "wadi_sc"],
      ["gtl_wadi", "wadi_gtl"],
      ["ubl_hg", "hg_ubl"],
      ["ltrr_sc", "sc_ltrr"],
      ["pune_dd", "dd_pune"],
      ["mrj_pune", "pune_mrj"],
      ["sc_tjsp", "tjsp_sc"]
    ];

    routePairs.flat().forEach(table => {
      results.push({
        table,
        data: tableData[table] || []
      });
    });

    console.timeEnd("processing");

    const response = { success: true, data: results };

    // Cache the response
    cache.set(cacheKey, response);
    console.log(`Cache set for summary_${type}`);

    res.json(response);
  } catch (err) {
    console.error(`Error in /api/summary/${type}:`, {
      message: err.message,
      stack: err.stack,
      details: err.details || "No additional details"
    });
    res.status(500).json({ success: false, message: "Server error fetching summary" });
  }
});

// app.get("/api/get-user-and-role", (req, res) => {
//   res.json({
//     username: req.user.username,
//     role: req.user.role,
//     designation: req.user.designation,
//     email: req.user.email,
//     firstName: req.user.firstName,
//     lastName: req.user.lastName
//   });
// });

app.get("/api/get-user-and-role", (req, res) => {
  try {
    const authHeader = req.headers.authorization;

    // ❌ No Authorization header
    if (!authHeader || !authHeader.startsWith("Bearer ")) {
      return res.status(401).json({ message: "No token provided" });
    }

    // ✅ Extract token
    const token = authHeader.split(" ")[1];

    // ✅ Verify token
    const decoded = jwt.verify(token, SECRET);

    // ✅ Send user data
    res.json({
      username: decoded.username,
      role: decoded.role,
      designation: decoded.designation,
      email: decoded.email,
      firstName: decoded.firstName,
      lastName: decoded.lastName,
    });
  } catch (err) {
    console.error("get-user-and-role error:", err);
    res.status(401).json({ message: "Invalid or expired token" });
  }
});



app.post("/api/add-user", async (req, res) => {
  try {
    const { username, password, email, role, designation, firstName, lastName } = req.body;

    if (!username || !password || !email || !role) {
      return res.status(400).json({ success: false, message: "Missing required fields" });
    }

    // Insert directly into Supabase (password stored as plain text)
    const { data, error } = await supabase
      .from("users")
      .insert([
        {
          username,
          password,       // ⚠️ stored as plain text
          email,
          role,
          designation,
          firstname: firstName, // use your actual DB column names
          lastname: lastName,
        }
      ])
      .select();

    if (error) {
      console.error("Supabase insert error:", error);
      if (error && error.code === "23505") {
        return res.status(400).json({ success: false, message: "Username already exists" });
      }
      return res.status(500).json({ success: false, message: "Database Insertion Failed", error });
    }

    return res.status(201).json({ success: true, user: data[0] });
  } catch (err) {
    console.error("Server error:", err);
    return res.status(500).json({ success: false, message: "Internal server error" });
  }
});

app.get("/api/users/:id", async (req, res) => {
  try {
    const { id } = req.params;

    const { error } = await supabase
      .from("users")
      .delete()
      .eq("id", id);

    if (error) {
      console.error("Supabase delete error:", error);
      return res.status(500).json({ success: false, message: "Failed to delete user" });
    }

    return res.json({ success: true, message: "User deleted successfully" });
  } catch (err) {
    console.error("Server error:", err);
    return res.status(500).json({ success: false, message: "Internal server error" });
  }
});
app.get("/api/users", async (req, res) => {
  try {
    const { data, error } = await supabase
      .from("users")
      .select("*");

    if (error) {
      console.error("Supabase fetch error:", error);
      return res.status(500).json({ success: false, message: "Failed to fetch users" });
    }

    return res.json({ success: true, users: data });
  } catch (err) {
    console.error("Server error:", err);
    return res.status(500).json({ success: false, message: "Internal server error" });
  }
});

// Add this endpoint to your server.js
app.get("/api/forecast-vs-actual", async (req, res) => {
  const tables = [
    "sc_wadi", "wadi_sc", "gtl_wadi", "wadi_gtl", "ubl_hg", "hg_ubl",
    "ltrr_sc", "sc_ltrr", "pune_dd", "dd_pune", "mrj_pune", "pune_mrj",
    "sc_tjsp", "tjsp_sc"
  ];


  try {
    let results = {
      Morning: { forecasted: 0, actual: 0 },
      Afternoon: { forecasted: 0, actual: 0 },
      Evening: { forecasted: 0, actual: 0 },
      Night: { forecasted: 0, actual: 0 }
    };

    // First get all relevant data from the table
    const { data: rows, error } = await supabase
      .from('forecast_data')
      .select('arrival, fc, ic')
      .not('arrival', 'is', null);
    if (error) throw error;
    const allRows = rows || [];
    // Process allRows as above
    if (error) throw error;

    // Process rows in JavaScript instead of SQL
    const timePeriods = rows.reduce((acc, row) => {
      if (!row.arrival || typeof row.arrival !== 'string') {
        console.warn(`Skipping row with invalid arrival: ${row.arrival}`);
        return acc;
      }
      // Ensure arrival is a valid HH:MM:SS format
      const timeMatch = row.arrival.match(/^(\d{2}):(\d{2}):(\d{2})$/);
      if (!timeMatch) {
        console.warn(`Invalid time format for arrival: ${row.arrival}`);
        return acc;
      }
      const hour = parseInt(timeMatch[1], 10);
      let period;

      if (hour >= 6 && hour <= 11) period = 'Morning';
      else if (hour >= 12 && hour <= 17) period = 'Afternoon';
      else if (hour >= 18 && hour <= 23) period = 'Evening';
      else period = 'Night';

      if (!acc[period]) {
        acc[period] = { forecasted: 0, actual: 0 };
      }

      if (row.fc === 'Y') acc[period].forecasted++;
      if (row.ic === 'Y') acc[period].actual++;

      return acc;
    }, {});

    // Aggregate results
    Object.entries(timePeriods).forEach(([period, counts]) => {
      results[period].forecasted += counts.forecasted;
      results[period].actual += counts.actual;
    });

    // Convert to array format for frontend (same as original)
    const chartData = Object.entries(results).map(([period, counts]) => ({
      period,
      forecasted: counts.forecasted,
      actual: counts.actual
    }));

    return res.json({
      success: true,
      data: chartData.sort((a, b) =>
        ['Morning', 'Afternoon', 'Evening', 'Night'].indexOf(a.period) -
        ['Morning', 'Afternoon', 'Evening', 'Night'].indexOf(b.period)
      )
    });

  } catch (err) {
    console.error("Error in /api/forecast-vs-actual:", err);
    return res.status(500).json({
      success: false,
      message: "Server error fetching forecast comparison data"
    });
  }
});

app.get("/api/wagon-summary", async (req, res) => {
  const tables = [
    "sc_wadi", "wadi_sc", "gtl_wadi", "wadi_gtl", "ubl_hg", "hg_ubl",
    "ltrr_sc", "sc_ltrr", "pune_dd", "dd_pune", "mrj_pune", "pune_mrj",
    "sc_tjsp", "tjsp_sc"
  ];

  try {
    // Check cache first
    const cacheKey = 'wagon_summary';
    const cachedData = cache.get(cacheKey);
    if (cachedData) {
      return res.json(cachedData);
    }


    // Query the view
    const { data, error } = await supabase
      .from('wagon_summary_data')
      .select('route, isloaded');

    if (error) {
      console.error('Error fetching view:', {
        message: error.message,
        details: error.details,
        hint: error.hint,
        code: error.code
      });
      throw error;
    }


    // Initialize counts for each table
    const tableCounts = {};
    tables.forEach(table => {
      tableCounts[table] = { loaded_wagons: 0, empty_wagons: 0 };
    });

    // Process rows
    (data || []).forEach(row => {
      if (tableCounts[row.route]) {
        if (row.isloaded === 'L') {
          tableCounts[row.route].loaded_wagons++;
        } else if (row.isloaded === 'E') {
          tableCounts[row.route].empty_wagons++;
        }
      }
    });

    // Build summary
    const summary = tables.map(table => ({
      route: table.toUpperCase(),
      loaded_wagons: tableCounts[table].loaded_wagons,
      empty_wagons: tableCounts[table].empty_wagons
    })).sort((a, b) => a.route.localeCompare(b.route));


    const response = { success: true, data: summary };

    // Cache the response
    cache.set(cacheKey, response);

    res.json(response);
  } catch (err) {
    console.error("Error in /api/wagon-summary:", {
      message: err.message,
      stack: err.stack,
      details: err.details || "No additional details"
    });
    res.status(500).json({
      success: false,
      message: "Server error fetching wagon summary"
    });
  }
});

async function applyOverrides() {
  try {

    // Fetch all overrides
    const { data: overrides, error: fetchError } = await supabase
      .from('useroverrides')
      .select('*');

    if (fetchError) throw fetchError;

    // Apply updates for each override
    for (const row of overrides) {
      const { tablename, rake_id, column_name, new: newValue } = row;
      console.log(tablename, rake_id, column_name, newValue);
      if (!tablename || !rake_id || !column_name) continue;

      const { error: updateError } = await supabase
        .from(tablename)
        .update({ [column_name.toLowerCase()]: newValue })
        .eq("rake_id", rake_id);

      if (updateError) throw updateError;
    }

    console.log("UserOverrides applied to all route tables.");
  } catch (err) {
    console.error("Error applying user overrides:", err);
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
    // Query the view directly
    console.time('Supabase ic-fc query');
    const { data, error } = await retry(async () => {
      return await supabase
        .from('ic_fc_stats_data')
        .select('route, ic, fc')
        .in('route', tablePairs.flatMap(pair => [`${pair.src}_${pair.dest}`, `${pair.dest}_${pair.src}`]))
        .range(0, 999); // Limit to 1000 rows
    });
    console.timeEnd('Supabase ic-fc query');

    if (error) {
      console.error('Error fetching view:', {
        message: error.message,
        details: error.details || 'No details provided',
        hint: error.hint || 'No hint provided',
        code: error.code || 'No code provided'
      });
      throw error;
    }

    // Log raw data for debugging
    console.log('Supabase raw data length:', data?.length || 0);
    console.log('Supabase raw data sample:', data?.slice(0, 5));

    // Initialize counts for each table
    const tableCounts = {};
    tablePairs.forEach(pair => {
      const forwardTable = `${pair.src}_${pair.dest}`;
      const reverseTable = `${pair.dest}_${pair.src}`;
      tableCounts[forwardTable] = { ic: 0, fc: 0 };
      tableCounts[reverseTable] = { ic: 0, fc: 0 };
    });

    // Process rows to count IC and FC
    (data || []).forEach(row => {
      if (tableCounts[row.route]) {
        if (row.ic === 'Y') tableCounts[row.route].ic++;
        if (row.fc === 'Y') tableCounts[row.route].fc++;
      }
    });

    // Build results
    const results = tablePairs.map(pair => {
      const forwardTable = `${pair.src}_${pair.dest}`;
      const reverseTable = `${pair.dest}_${pair.src}`;
      return {
        pair: forwardTable,
        directions: [
          {
            direction: 'forward',
            tableName: forwardTable,
            IC: tableCounts[forwardTable].ic,
            FC: tableCounts[forwardTable].fc
          },
          {
            direction: 'reverse',
            tableName: reverseTable,
            IC: tableCounts[reverseTable].ic,
            FC: tableCounts[reverseTable].fc
          }
        ]
      };
    });

    const response = { success: true, data: results };

    res.json(response);
  } catch (err) {
    console.error("Error in /api/ic-fc-stats:", {
      message: err.message,
      stack: err.stack || 'No stack trace',
      details: err.details || 'No additional details',
      code: err.code || 'No code provided'
    });
    res.status(500).json({
      success: false,
      message: "Server error fetching IC/FC stats",
      error: err.message
    });
  }
});
const cache = new NodeCache({ stdTTL: 300 }); // Cache for 5 minutes

app.get("/api/dashboard-stats", async (req, res) => {
  const tables = [
    "sc_wadi", "wadi_sc", "gtl_wadi", "wadi_gtl", "ubl_hg", "hg_ubl",
    "ltrr_sc", "sc_ltrr", "pune_dd", "dd_pune", "mrj_pune", "pune_mrj",
    "sc_tjsp", "tjsp_sc"
  ];

  try {
    // Query the view
    const { data, error } = await supabase
      .from('dashboard_stats_data')
      .select('route, ic, fc');

    if (error) {
      console.error('Error fetching view:', {
        message: error.message,
        details: error.details || 'No details provided',
        hint: error.hint || 'No hint provided',
        code: error.code || 'No code provided'
      });
      throw error;
    }

    // Log raw data for debugging
    console.log('Supabase raw data:', data);

    // Initialize aggregates
    let totalICCount = 0;
    let totalFCCount = 0;
    let totalTrainCount = 0;
    const perTableCounts = {};

    // Initialize per-table counts
    tables.forEach(table => {
      perTableCounts[table] = { count: 0 };
    });

    // Process rows
    (data || []).forEach(row => {
      if (row.ic === 'Y') {
        totalICCount++;
        perTableCounts[row.route].count++;
      }
      if (row.fc === 'Y') {
        totalFCCount++;
      }
      totalTrainCount++;
    });

    // Convert perTableCounts to array
    const breakdown = Object.entries(perTableCounts).map(([table, { count }]) => ({
      table,
      count
    }));

    const response = {
      success: true,
      stats: {
        totalTrains: totalTrainCount,
        totalInterchange: totalICCount,
        totalForecast: totalFCCount,
      },
      breakdown
    };

    res.json(response);
  } catch (err) {
    console.error("Error in /api/dashboard-stats:", {
      message: err.message,
      stack: err.stack || 'No stack trace',
      details: err.details || 'No additional details',
      code: err.code || 'No code provided'
    });
    res.status(500).json({
      success: false,
      message: "Server error fetching dashboard statistics",
      error: err.message
    });
  }
});

app.get("/api/route/:tableName", async (req, res) => {
  try {
    const tableName = req.params.tableName.toLowerCase();

    // Make sure to define allowedTables somewhere in your code
    // Example: const allowedTables = ["SC-WADI", "WADI-SC", ...];
    if (!allowedTables.includes(tableName)) {
      return res.status(400).json({ success: false, message: "Invalid route" });
    }

    // Supabase equivalent query
    const { data, error } = await supabase
      .from(tableName)
      .select('*');

    if (error) throw error;

    res.json({ success: true, data });

  } catch (error) {
    console.error("Error fetching route data:", error);
    res.status(500).json({ success: false, message: "Server error" });
  }
});


app.get("/api/save-table", async (req, res) => {
  const { tableName, rake_id, column, value, original } = req.query;
  console.log("Request params:", { tableName, rake_id, column, value, original });

  if (!rake_id || !column) {
    return res.status(400).json({ error: "Missing parameters" });
  }

  try {
    // Update the main table
    const { error: updateError, data: updateData } = await supabase
      .from(tableName)
      .update({ [column.toLowerCase()]: value })
      .eq("rake_id", rake_id);

    console.log("Main table update result:", { updateError, updateData });
    if (updateError) throw updateError;

    // Check if the rake_id and column_name combination exists
    const { count } = await supabase
      .from('useroverrides')
      .select('*', { count: 'exact', head: true })
      .eq('rake_id', rake_id)
      .eq('column_name', column.toLowerCase());

    console.log("Exists check count:", count);
    const exists = count > 0;

    if (exists) {
      // Update only the 'new' column if the combination exists
      const { error: updateOverrideError, data: updateOverrideData } = await supabase
        .from('useroverrides')
        .update({ new: value, time: new Date().toISOString() })
        .eq('rake_id', rake_id)
        .eq('column_name', column.toLowerCase());

      console.log("Override update result:", { updateOverrideError, updateOverrideData });
      if (updateOverrideError) throw updateOverrideError;
    } else {
      // Insert a new row if the combination doesn't exist
      const { error: insertOverrideError, data: insertOverrideData } = await supabase
        .from('useroverrides')
        .insert({
          rake_id,
          new: value,
          firstvalue: original,
          tablename: tableName,
          column_name: column,
          time: new Date().toISOString(),
        });

      console.log("Override insert result:", { insertOverrideError, insertOverrideData });
      if (insertOverrideError) throw insertOverrideError;
    }

    res.json({ success: true, rake_id, column, value });
  } catch (err) {
    console.error("Error saving table:", err);
    res.status(500).json({ error: "Failed to save" });
  }
});


app.get("/api/clear-all-data", async (req, res) => {
  try {
    const { error } = await supabase.rpc("truncate_all_tables");
    if (error) throw error;

    res.json({ success: true, message: "All tables truncated successfully" });
  } catch (err) {
    console.error("Error truncating tables:", err);
    res.status(500).json({ success: false, error: err.message });
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
    await applyOverrides();
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
    // console.error(`Error updating ${tableName}:`, error);
    // throw new Error(`Database update failed for ${tableName}`);
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