import WebSocket from "ws";
import { ulid } from "ulid";
import { strict as assert } from 'assert';

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function promise() {
    let stolenResolve = null;
    let stolenReject = null;
    const promise = new Promise((resolve, reject) => {
        stolenResolve = resolve;
        stolenReject = reject;
    });
    assert(stolenResolve);
    assert(stolenReject);
    return {
        promise,
        resolve: stolenResolve,
        reject: stolenReject,
    };
}

const RETRY_COUNT = 5;

class RhinoShard {
    constructor() {
    }

    getName() {
        return this._name;
    }

    openTable(name) {
        const table = new RhinoTable();
        table._name = name;
        table._rh = this;
        return table;
    }
}

class RhinoTable {
    constructor() {
    }

    getName() {
        return this._name;
    }
}

class Rhino {
    constructor() {
    }

    static async login(token) {
        const rh = new Rhino();
        rh._callbacks = {};
        rh._callbacksDisabled = false;
        this._gracefulShutdown = false;

        // Setup WebSocket connection.
        // Note that we don't attempt to reconnect to the WebSocket once it fails. This is because
        // right now we mainly target short-lived edge workloads where it's easier to restart the
        // whole worker. This might change in the future.
        rh._ws = new WebSocket("ws://localhost:3001");

        const wsOpened = promise();
        rh._ws.on("open", wsOpened.resolve);
        rh._ws.on("close", () => {
            rh._wsFailedCleanup();
            wsOpened.reject("connection to server closed");
        });
        rh._ws.on("error", () => {
            rh._wsFailedCleanup();
            wsOpened.reject("connection to the server failed");
        });
        rh._ws.on("message", rh._handleMessage.bind(rh));

        // Wait for the WebSocket connection to open.
        await wsOpened.promise;

        // Try to login.
        const payload = await rh._send({ "Login": { "token": token } });
        if (payload === "InvalidToken") {
            throw new Error("token is invalid");
        } else if (typeof payload === "object" && "LoggedIn" in payload) {
            rh._region = payload["LoggedIn"]["region"];
            return rh;
        } else {
            throw new Error("unexpected response");
        }
    }

    _wsFailedCleanup() {
        if (!this._gracefulShutdown) {
            console.error("Rhino: connection to the server failed");
        }
        this._callbacksDisabled = true;
        this._ws.close();
        for (const correlationId in this._callbacks) {
            this._callbacks[correlationId].error("connection to the server failed");
        }
    }

    _handleMessage(response) {
        if (this._callbacksDisabled) {
            // This means that all callbacks were already errored due to WebSocket connection failure.
            return;
        }

        const parsed = JSON.parse(response);
        console.log("received a response", parsed);

        const correlationId = parsed["correlation_id"];
        this._callbacks[correlationId].success(parsed["payload"]);
    }

    async _send(payload) {
        const responseP = promise();
        const correlationId = ulid();

        // Register both error and success callbacks.
        const that = this;
        this._callbacks[correlationId] = {
            success: (response) => {
                responseP.resolve(response);
                delete that._callbacks[correlationId];
            },
            error: (message) => {
                responseP.reject(message);
                delete that._callbacks[correlationId];
            },
        };

        // Try to send the message.
        const sendP = promise();
        const request = {
            "payload": payload,
            "correlation_id": correlationId,
        };
        // TODO: timeouts.
        this._ws.send(JSON.stringify(request), {}, (error) => {
            if (error) {
                sendP.reject(error);
            } else {
                sendP.resolve();
            }
        });

        // Wait for the send to happen first to throw if it failed.
        await sendP.promise;

        return responseP.promise;
    }

    async _sendAndRetry(payload, errorMessage) {
        let i;
        let reason;
        let response;
        for (i = 0; i < RETRY_COUNT; i++) {
            response = await rh._send(payload);
            if (typeof response === "object" && "PleaseRetry" in response) {
                reason = response["PleaseRetry"]["reason"];
                await sleep(100);
                continue;
            } else {
                break;
            }
        }

        if (i === RETRY_COUNT) {
            throw new Error(`${errorMessage} because ${reason}`);
        }

        return response;
    }

    async createShard({ name, region }) {
        const payload = await this._sendAndRetry({ "CreateShard": { name, region } }, "cannot create shard");
        if (payload === "ShardCreated") {
            const shard = new RhinoShard();
            shard._name = name;
            shard._rh = this;
            return shard;
        } else if (payload === "ShardAlreadyExists") {
            throw new Error("shard with this name already exists");
        } else {
            throw new Error("unexpected response");
        }
    }

    async openShard(name) {
        const shard = new RhinoShard();
        shard._name = name;
        shard._rh = this;
        return shard;
    }

    async createTable({ name, pk }) {
        const payload = await this._sendAndRetry({ "CreateTable": { name, pk } }, "cannot create table");
        if (payload === "TableCreated") {
            return;
        } else if (payload === "TableAlreadyExists") {
            throw new Error("table with this name already exists");
        } else {
            throw new Error("unexpected response");
        }
    }

    getRegion() {
        return this._region;
    }

    close() {
        this._gracefulShutdown = true;
        this._ws.close();
    }
}

const rh = await Rhino.login("f9b37d32-b67c-42cb-9d03-13b247ba34aa");
console.log("Connected to region: ", rh.getRegion());

try {
    await rh.createTable({
        name: "test_table",
        pk: ["country", "user_id"],
    });
    console.log("Table created");
} catch (err) {
    console.log(`Couldn't create shard because of ${err}`);
}

let shard;
try {
    shard = await rh.createShard({ name: "test_shard_name", region: "us-east-2" });
    console.log("Shard created");
} catch (err) {
    console.log(`Couldn't create shard because of ${err}`);
    shard = await rh.openShard("test_shard_name");
    console.log("Shard opened");
}

const table = shard.openTable("test_table");

// await table.set({
//     'field1': 'value',
//     'field2': 123,
//     'some_other_field': 456,
// });

// await table.get({
//     'field1': 'value',
//     'field2': 123,
// });

// await table.remove({
//     'field1': 'value',
//     'field2': 123,
// });

rh.close();
