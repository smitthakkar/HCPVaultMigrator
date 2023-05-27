const axios = require('axios');
const sqlite3 = require('sqlite3');
const sqlite = require('sqlite');

const SOURCE_URL = '<SOURCE_URL>/';
const SOURCE_TOKEN = '<SOURCE_TOKEN>';
const DESTINATION_URL = '<DESTINATION_URL>/';
const DESTINATION_TOKEN = '<DESTINATION_TOKEN>';

const MAX_RETRY_COUNT = 2;

let ROOT_KEYS =  ['kv/issuers/', 'kv/holders/', 'transit/keys/'];

let db;
const databaseName = `./database-${Date.now()}.db`;

const timer = ms => new Promise(res => setTimeout(res, ms));

async function init() {

    db = await sqlite.open({
        filename: __dirname + `${databaseName}`, driver: sqlite3.Database
    });
    console.log('Database created successfully. Database Name :', databaseName);
}

async function create() {
    console.log('Creating error log table');

    await db.exec(`
        CREATE TABLE IF NOT EXISTS error_log (
        id                    INTEGER PRIMARY KEY AUTOINCREMENT,
        key_type              TEXT,
        key_path              TEXT,
        message               TEXT,
        message_info          TEXT,
        created_on            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_on            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );`);

    console.log('Table created successfully.');
}

async function getKeysAtPath(url, apiKey, path) {
    const apiURL = url + 'v1/' + path;
    const requestObj = {'API_URL': apiURL};
    const headers = {
        'X-Vault-Token': apiKey,
        'X-Vault-Namespace': 'admin'
    }
    const results = await axios({
        method: 'LIST',
        url: apiURL, headers: headers
    }).then(data=>data).catch(err=>err);

    if (results.status != 200) {
        return [];
    }
    return results.data.data.keys;
}

async function getDataAtPath(url, apiKey, path) {
    const apiURL = url + 'v1/' + path;
    const requestObj = {'API_URL': apiURL};
    const headers = {
        'X-Vault-Token': apiKey,
        'X-Vault-Namespace': 'admin'
    }
    const results = await axios({
        method: 'GET',
        url: apiURL, headers: headers,
        timeout: 36000000
    }).then(data => data)
        .catch(err => {
        throw {...err, ...requestObj}
    });
    return results.data.data;
}

async function writeKeyAtPath(url, apiKey, path, data) {
    const apiURL = url + 'v1/' + path;
    const requestObj = {'API_URL': apiURL};
    const headers = {
        'X-Vault-Token': apiKey,
        'X-Vault-Namespace': 'admin'
    }

    const results = await axios({
        method: 'POST',
        url: apiURL,
        data: data,
        headers: headers,
    }).then(data => data)
        .catch(err => {
            throw {...err, ...requestObj}
        });
    return results.status;
}

async function getTransitBackup(url, apiKey, path) {
    const apiURL = url + 'v1/transit/backup/' + path;
    const requestObj = {'API_URL': apiURL};
    const headers = {
        'X-Vault-Token': apiKey,
        'X-Vault-Namespace': 'admin'
    }
    const results = await axios({
        method: 'GET',
        url: apiURL, headers: headers,
    }).then(data => data)
        .catch(err => {
            throw {...err, ...requestObj}
        });
    return results.data.data;
}

async function restoreTransitBackup(url, apiKey, path, data) {
    const apiURL = url + 'v1/transit/restore/' + path;
    const requestObj = {'API_URL': apiURL};
    const headers = {
        'X-Vault-Token': apiKey,
        'X-Vault-Namespace': 'admin'
    }

    const results = await axios({
        method: 'POST',
        url: apiURL,
        data: data,
        headers: headers
    }).then(data => data)
        .catch(err => {
            throw {...err, ...requestObj}
        });
    return results.status;
}

async function updateConfigAtPath(url, apiKey, path) {
    let data = await getDataAtPath(url, apiKey, path);
    data["exportable"] = true;
    data["allow_plaintext_backup"] = true;
    path += '/config'
    let res = await writeKeyAtPath(url, apiKey, path, data);
    return res;
}

function handleAxiosError(error) {
    const errorObj = {};
    if (error.response) {
        errorObj['error_status'] = error.response.status;
        errorObj['error_response'] = error.response.data;
        errorObj['error_response_headers'] = error.response.headers;
        errorObj['request_url'] = error["API_URL"];
        // Request made and server responded
    } else if (error.request) {
        // The request was made but no response was received
        errorObj['error_request'] = error.request;
        errorObj['request_url'] = error["API_URL"];
    } else {
        // Something happened in setting up the request that triggered an Error
        errorObj['message'] = error.message;
    }
    return errorObj;
}

function compareTransitBackups(sourceBackup, destinationBackup) {
    let sourceData = JSON.parse(Buffer.from(sourceBackup.backup, 'base64').toString());
    let destinationData = JSON.parse(Buffer.from(destinationBackup.backup, 'base64').toString());

    delete sourceData["policy"]["AllowImportedKeyRotation"];
    delete sourceData["policy"]["auto_rotate_period"];
    delete sourceData["policy"]["Imported"];
    delete sourceData["policy"]["restore_info"];
    delete sourceData["policy"]["backup_info"];

    delete destinationData["policy"]["AllowImportedKeyRotation"];
    delete destinationData["policy"]["auto_rotate_period"];
    delete destinationData["policy"]["Imported"];
    delete destinationData["policy"]["restore_info"];
    delete destinationData["policy"]["backup_info"];


    let sourceString = JSON.stringify(sourceData);
    let destinationString = JSON.stringify(destinationData);

    const dataCheck = sourceString === destinationString ? true : false;
    return {sourceString, destinationString, dataCheck};
}

async function processTransit(targetPath, value, isWritingEnabled, retryCount = MAX_RETRY_COUNT) {
    if (retryCount > MAX_RETRY_COUNT) {
        return;
    }
    try {
        await updateConfigAtPath(SOURCE_URL, SOURCE_TOKEN, targetPath);
        let sourceBackup = await getTransitBackup(SOURCE_URL, SOURCE_TOKEN, value);
        sourceBackup["name"] = value;
        if (isWritingEnabled) {
            await restoreTransitBackup(DESTINATION_URL, DESTINATION_TOKEN, value, sourceBackup);
        }
        let destinationBackup = await getTransitBackup(DESTINATION_URL, DESTINATION_TOKEN, value);

        let {sourceString, destinationString, dataCheck} = compareTransitBackups(sourceBackup, destinationBackup);
        if (!dataCheck) {
            throw `Transit verification failed for path: ${targetPath}`;
        }
    } catch (error) {
        const errorObj = handleAxiosError(error);
        const command = `INSERT INTO error_log(key_type, key_path,message,message_info) 
            VALUES('TRANSIT', '${targetPath}', 'FAILED_PROCESSING_${retryCount}_TIME', '${JSON.stringify(errorObj)}')`;
        await db.exec(command);
        //put sleep interval
        await timer(1000);
        await processTransit(targetPath, value, isWritingEnabled, retryCount + 1);
    }

}

async function processkv(targetPath, isWritingEnabled, retryCount = MAX_RETRY_COUNT) {
    if (retryCount > MAX_RETRY_COUNT) {
        return;
    }
    try {
        let data = await getDataAtPath(SOURCE_URL, SOURCE_TOKEN, targetPath);
        if (isWritingEnabled) {
            await writeKeyAtPath(DESTINATION_URL, DESTINATION_TOKEN, targetPath, data);
        }
        let destinationData = await getDataAtPath(DESTINATION_URL, DESTINATION_TOKEN, targetPath);
        let dataString = JSON.stringify(data);
        let destinationString = JSON.stringify(destinationData);
        const dataCheck = dataString === destinationString ? true : false;
        if (!dataCheck) {
            throw `KV verification failed for path: ${targetPath}`
        }
        return dataCheck;
    } catch (error) {
        const errorObj = handleAxiosError(error);
        const key_type = targetPath.includes('holders') ? 'HOLDER':'ISSUER';
        const command = `INSERT INTO error_log(key_type, key_path,message,message_info) 
            VALUES('${key_type}', '${targetPath}', 'FAILED_PROCESSING_${retryCount}_TIME', '${JSON.stringify(errorObj)}')`;
        await db.exec(command);
        // sleep interval
        await timer(1000);
        await processkv(targetPath, isWritingEnabled, retryCount + 1);
    }
}

async function execute() {
    await init();
    await create();
    let arguments = process.argv;
    let isWritingEnabled = false;
    if (arguments[2] === 'write') {
        isWritingEnabled = true;
        console.log('Starting processing, Writing is enabled. This operation will write keys in destination');
    } else {
        console.log('Starting processing, Writing is disabled. This operation will verify keys in destination');
    }
    let root_index = 0;
    let promises = [];
    while (root_index < ROOT_KEYS.length) {
        let root = ROOT_KEYS[root_index];
        let keyList = await getKeysAtPath(SOURCE_URL, SOURCE_TOKEN, root);
        console.log(`Starting migration for root : ${root}, numberOfKeys : ${keyList.length}`);
        let destinationList = await getKeysAtPath(DESTINATION_URL, DESTINATION_TOKEN, root);
        let destinationCache = {};
        destinationList.forEach(a => {
            destinationCache[a] = 1;
        });
        let index = 0;
        let existsCount = 0;
        while (index < keyList.length) {
            if (index % 2000 == 0) {
                console.log(`Completed ${index}/${keyList.length}`);
            }

            let value = keyList[index];
            if (destinationCache[value] === 1 && isWritingEnabled) {
                index++;
                existsCount++;
                continue;
            }
            if (value.toString().indexOf("/") !== -1) {
                let targetPath = root + value;
                ROOT_KEYS.push(targetPath);
            } else {
                let targetPath = root + value;
                if (targetPath.includes('transit')) {
                    promises.push(processTransit(targetPath, value, isWritingEnabled, 1));
                } else {
                    promises.push(processkv(targetPath, isWritingEnabled, 1));
                }
            }
            if (promises.length > 80) {
                let results = await Promise.allSettled(promises);
                promises = [];
                let failed = results.filter(s => {
                    return s.status == 'rejected';
                })
                if (failed.length > 0) {
                    console.log(`Failed : ${failed.length}/${results.length}`);
                }
            }
            index++;
        }
        console.log(`Completed Processing for root: ${root}, total : ${keyList.length}, existing : ${existsCount}, added : ${index - existsCount}`);
        root_index++;
    }
    if (promises.length > 0) {
        let results = await Promise.allSettled(promises);
        promises = [];
        let failed = results.filter(s => {
            return s.status == 'rejected';
        })
        if (failed.length > 0) {
            console.log(`Failed : ${failed.length}/${results.length}`);
        }
    }

}

execute()
    .then(data => console.log('completed'))
    .catch(err => console.error('Failed', err));
