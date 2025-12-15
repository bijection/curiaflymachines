require('dotenv').config();
const express = require('express');
const { Pool } = require('pg');
const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
const fs = require('fs-extra');
const path = require('path');
const { exec } = require('child_process');
const util = require('util');
const { v4: uuidv4 } = require('uuid');
const stream = require('stream');
const { promisify } = require('util');

const execPromise = util.promisify(exec);
const pipeline = promisify(stream.pipeline);

// --- Configuration ---
const PORT = process.env.PORT || 3000;
const S3_BUCKET_NAME = process.env.S3_BUCKET_NAME;

// --- Database Setup ---
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

// Initialize DB Table
const initDb = async () => {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS deployments (
      id SERIAL PRIMARY KEY,
      subdomain TEXT UNIQUE NOT NULL,
      session_id INT NOT NULL,
      block_id INT NOT NULL,
      generation_id INT NOT NULL,
      parent_generation_ids INT[],
      fileset JSONB NOT NULL,
      machine_id TEXT,
      last_access TIMESTAMPTZ DEFAULT NOW(),
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_subdomain ON deployments(subdomain);
    CREATE INDEX IF NOT EXISTS idx_session_machine ON deployments(session_id, last_access);
  `);
};

// --- S3 Client ---
const s3 = new S3Client({
  endpoint: process.env.AWS_ENDPOINT_URL_S3,
  region: process.env.AWS_REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
});

// --- Helpers ---

// Helper: Download files from S3 to a temp directory
async function downloadFileset(fileset, destinationDir) {
  await fs.ensureDir(destinationDir);

  for (const [filePath, s3Key] of Object.entries(fileset)) {
    const fullPath = path.join(destinationDir, filePath);
    await fs.ensureDir(path.dirname(fullPath));

    const command = new GetObjectCommand({
      Bucket: S3_BUCKET_NAME,
      Key: s3Key,
    });

    try {
      const response = await s3.send(command);
      const fileStream = fs.createWriteStream(fullPath);
      await pipeline(response.Body, fileStream);
    } catch (err) {
      console.error(`Error downloading ${filePath} (Key: ${s3Key}):`, err);
      throw err;
    }
  }
}

// Helper: Prune old machines (LRU) if > 30 per session
async function pruneMachines(sessionId) {
  const client = await pool.connect();
  try {
    // Check count of running machines for this session
    const res = await client.query(
      `SELECT count(*) as count FROM deployments WHERE session_id = $1 AND machine_id IS NOT NULL`,
      [sessionId]
    );
    
    const count = parseInt(res.rows[0].count, 10);
    
    if (count > 30) {
      // Find least recently accessed machine
      const lruRes = await client.query(
        `SELECT id, machine_id, subdomain FROM deployments 
         WHERE session_id = $1 AND machine_id IS NOT NULL 
         ORDER BY last_access ASC LIMIT 1`,
        [sessionId]
      );

      if (lruRes.rows.length > 0) {
        const victim = lruRes.rows[0];
        console.log(`Pruning machine ${victim.machine_id} for subdomain ${victim.subdomain} (Session: ${sessionId})`);

        // Destroy via flyctl
        try {
            // --force ensures it's destroyed even if running
            await execPromise(`flyctl machine destroy ${victim.machine_id} --force`);
        } catch (e) {
            console.error(`Failed to destroy machine ${victim.machine_id}:`, e.message);
            // We continue to remove it from DB so we don't get stuck in a loop
        }

        // Update DB to reflect machine is gone
        await client.query(
          `UPDATE deployments SET machine_id = NULL WHERE id = $1`,
          [victim.id]
        );
      }
    }
  } finally {
    client.release();
  }
}

// Core Function: Deploy Machine Logic
async function deployMachineLogic(subdomain) {
  console.log(`Starting deployment for subdomain: ${subdomain}`);
  
  const client = await pool.connect();
  let deploymentRecord;

  try {
    const res = await client.query(`SELECT * FROM deployments WHERE subdomain = $1`, [subdomain]);
    if (res.rows.length === 0) throw new Error('Subdomain not found in records');
    deploymentRecord = res.rows[0];
  } finally {
    client.release();
  }

  const tempDir = path.join(require('os').tmpdir(), 'curia-deploy', subdomain);
  
  try {
    // 1. Download Files
    console.log(`Downloading files for ${subdomain}...`);
    await downloadFileset(deploymentRecord.fileset, tempDir);

    // 2. Deploy using Flyctl
    // We assume the temp folder has a Dockerfile as per prompt instructions.
    // We use `flyctl machine run` with remote build.
    // App name format: curia-session-[sessionId]
    const appName = `curia-session-${deploymentRecord.session_id}`;
    
    console.log(`Deploying to app ${appName}...`);

    // Note: 'flyctl machine run . --build-remote' attempts to build the local context remotely 
    // and run it as a machine. The output is requested in JSON to parse the ID.
    // If the app doesn't exist, this might fail depending on flyctl version, 
    // assuming app exists or is created beforehand.
    const cmd = `flyctl machine run . --app ${appName} --detach --json`;
    
    const { stdout } = await execPromise(cmd, { cwd: tempDir });
    
    let machineId;
    try {
        // flyctl returns json, usually an array or object with 'id'
        const output = JSON.parse(stdout);
        machineId = output.id || (Array.isArray(output) && output[0]?.id);
        if (!machineId) throw new Error("No machine ID in output");
    } catch (parseErr) {
        console.error("Failed to parse flyctl output:", stdout);
        throw new Error("Deployment failed: Could not parse machine ID");
    }

    console.log(`Deployed machine: ${machineId}`);

    // 3. Update DB
    await pool.query(
      `UPDATE deployments SET machine_id = $1, last_access = NOW() WHERE subdomain = $2`,
      [machineId, subdomain]
    );

    // 4. Prune if necessary
    await pruneMachines(deploymentRecord.session_id);

    return machineId;

  } catch (err) {
    console.error(`Deployment failed for ${subdomain}:`, err);
    throw err;
  } finally {
    // Cleanup temp files
    await fs.remove(tempDir);
  }
}

// Internal Function: Save Fileset Record
async function saveFilesetRecord(data) {
    const { fileset, sessionId, blockId, generationId, parentGenerationIds } = data;
    
    // Generate unique subdomain
    const subdomain = `gen-${sessionId}-${generationId}-${uuidv4().slice(0, 8)}`;

    const query = `
        INSERT INTO deployments 
        (subdomain, session_id, block_id, generation_id, parent_generation_ids, fileset)
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING subdomain
    `;
    
    const values = [
        subdomain, 
        sessionId, 
        blockId, 
        generationId, 
        parentGenerationIds, 
        JSON.stringify(fileset)
    ];

    const res = await pool.query(query, values);
    return res.rows[0].subdomain;
}

// --- Express App ---
const app = express();
app.use(express.json());

// --- Admin Interface & API ---

// Serve static assets for admin UI
app.use(express.static(path.join(__dirname, 'public')));

// Admin Page
app.get('/admin', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'admin.html'));
});

// API: Stats
app.get('/api/admin/stats', async (req, res) => {
    try {
        const total = await pool.query('SELECT COUNT(*) FROM deployments');
        const active = await pool.query('SELECT COUNT(*) FROM deployments WHERE machine_id IS NOT NULL');
        res.json({
            totalDeployments: parseInt(total.rows[0].count),
            activeMachines: parseInt(active.rows[0].count)
        });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// API: Recent Deployments
app.get('/api/admin/deployments', async (req, res) => {
    try {
        const result = await pool.query(`
            SELECT id, subdomain, session_id, machine_id, last_access, created_at 
            FROM deployments 
            ORDER BY created_at DESC 
            LIMIT 50
        `);
        res.json(result.rows);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// 1. API Endpoint: /deploy
app.post('/deploy', async (req, res) => {
    try {
        const { fileset, sessionId, blockId, generationId, parentGenerationIds } = req.body;
        
        if (!fileset || !sessionId) {
            return res.status(400).json({ error: 'Missing required fields' });
        }

        // Save metadata
        const subdomain = await saveFilesetRecord({
            fileset, sessionId, blockId, generationId, parentGenerationIds
        });

        // Trigger internal deploy function
        // Note: We await this to ensure initial success, though for high throughput 
        // you might want to make this background async.
        const machineId = await deployMachineLogic(subdomain);

        res.json({ 
            success: true, 
            subdomain: subdomain, 
            url: `http://${subdomain}.curia.site`,
            machineId: machineId 
        });

    } catch (err) {
        console.error(err);
        res.status(500).json({ error: err.message });
    }
});

// 2. Wildcard Handler for [subdomain].curia.site
// Since Express doesn't support wildcard subdomains directly in routing syntax easily,
// we catch all other requests and parse the host.
app.use(async (req, res, next) => {
    const host = req.hostname; // e.g., "abc.curia.site"
    
    // Check if it matches our domain pattern
    if (!host.endsWith('.curia.site')) {
        return res.status(404).send('Not Found');
    }

    const subdomain = host.replace('.curia.site', '');
    
    if (!subdomain) {
        return res.status(404).send('Subdomain missing');
    }

    try {
        // Check DB
        const result = await pool.query(
            `SELECT machine_id FROM deployments WHERE subdomain = $1`, 
            [subdomain]
        );

        if (result.rows.length === 0) {
            return res.status(404).send('Deployment not found');
        }

        let machineId = result.rows[0].machine_id;

        // If no machine is running, deploy one
        if (!machineId) {
            console.log(`Cold start for ${subdomain}...`);
            // This might take time, client will hang until deployed
            machineId = await deployMachineLogic(subdomain);
        } else {
            // Update last access time (async, don't await)
            pool.query(
                `UPDATE deployments SET last_access = NOW() WHERE subdomain = $1`, 
                [subdomain]
            ).catch(err => console.error('Error updating access time', err));
        }

        // Fly Replay Logic
        // We tell the Fly proxy to retry this request against the specific machine ID
        console.log(`Replaying request for ${subdomain} to machine ${machineId}`);
        res.set('fly-replay', `instance=${machineId}`);
        return res.status(200).send(`Replaying to ${machineId}`);

    } catch (err) {
        console.error('Proxy Error:', err);
        res.status(500).send('Internal Server Error');
    }
});

// Start Server
initDb().then(() => {
    app.listen(PORT, () => {
        console.log(`Manager running on port ${PORT}`);
    });
});