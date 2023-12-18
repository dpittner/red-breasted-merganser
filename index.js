const duckdb = require('duckdb');
const express = require("express");
const app = express()

PORT = 8080
app.use(express.json())
const router = express.Router()
app.use("/", router)

const server = app.listen(PORT, () => {
	console.log(`Server is up and running on port ${PORT}!!`)
})

const db = new duckdb.Database(':memory:', { allow_unsigned_extensions: 'true' });

const connection = db.connect();

let initialized = false;

const query = (query) => {
    return new Promise((resolve, reject) => {
      connection.all(query, (err, res) => {
        if (err) reject(err);
        resolve(res);
      })
    })
  }
router.post("/", async (req,res) => {
    try {
        if (!initialized) {
            await query(`SET home_directory='/tmp';`);
            await query(`SET custom_extension_repository = 'http://extensions.quacking.cloud';`);
            await query(`INSTALL httpfs;`);
            await query(`LOAD httpfs;`);
            await query(`SET enable_http_metadata_cache=true;`);
            await query(`SET enable_object_cache=true;`);

            await query(`SET s3_endpoint='s3.direct.eu-de.cloud-object-storage.appdomain.cloud';`);
            await query(`SET s3_url_style='path';`);

            initialized = true;
        }

        const queryResult = await query(req.body.query);

        res.status(200);
        res.send(JSON.stringify(queryResult));
      
    } catch (e) {
        res.status(400);
        res.send(JSON.stringify(e));
    }
  });
  

  process.on('SIGTERM', () => {
	console.info('SIGTERM signal received.');
  });
