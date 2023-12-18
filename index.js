const duckdb = require('duckdb');

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
async function main(args) {
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

        const queryResult = await query(args.query);

        return {
            statusCode: 200,
            headers: { "Content-Type": "application/json;charset=utf-8" },
            body: JSON.stringify(queryResult)
        };
    } catch (e) {
        return {
            statusCode: 400,
            body: JSON.stringify(e)
        }
    }
  }
  

  process.on('SIGTERM', () => {
	console.info('SIGTERM signal received.');
  });

  module.exports.main = main;