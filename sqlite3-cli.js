#!/usr/bin/env node

const util = require("util")
const readline = require("readline")
const sqlite3 = require("sqlite3")

/**
 * @param {import("stream").Readable | import("stream").Writable} s
 */
function isTTY(s) {
    return /** @type {import("tty").ReadStream | import("tty").WriteStream} */ (s).isTTY
}

/**
 * @param {import("stream").Writable} out
 * @param {ReadonlyArray<Buffer | string>} data
 * @param {(err?: Error | null) => void} [callback]
 */
async function write(out, data, callback) {
    for (const chunk of data) {
        const ok = out.write(chunk, callback)
        if (!ok) {
            await new Promise(resolve => {
                out.on("drain", resolve)
            })
        }
    }
}

/**
 * @typedef {object} Interrupter
 * @property {Promise<never>} receiver
 * @property {(reason?: any) => void} interrupt
 */

/** @returns {Interrupter} */
function createInterrupter() {
    /** @type {Partial<Interrupter>} */
    const interrupter = {}
    const newChannel = () => {
        interrupter.receiver = new Promise((resolve, reject) => {
            /** @param {any} [reason] */
            interrupter.interrupt = (reason) => {
                newChannel()
                reject(reason)
            }
        })
    }
    newChannel()
    return /** @type {Interrupter} */ (interrupter)
}

class SQLite3CLI {
    /**
     * @param {string} dbfile
     * @param {import("stream").Writable} output
     * @param {Partial<readline.ReadLineOptions>} options
     */
    constructor(dbfile, output, options = {}) {
        /** @type {Partial<readline.ReadLineOptions>} */
        const {
            input: conin = process.stdin,
            output: conout = process.stderr,
            terminal: terminal,
            completer: completer,
            historySize: historySize,
            prompt: prompt = "sqlite> ",
            crlfDelay: crlfDelay = Infinity,
            removeHistoryDuplicates: removeHistoryDuplicates,
        } = options
        this.dbfile = dbfile
        this.conin = /** @type {import("stream").Readable} */ (conin)
        this.conout = /** @type {import("stream").Writable} */ (conout)
        this.output = /** @type {import("stream").Writable} */ (output)
        this.terminal = terminal != null ? terminal : (isTTY(this.conin) && isTTY(this.conout))
        this.colors = isTTY(this.conout)
        this.rl = readline.createInterface({
            input: conin,
            output: conout,
            terminal: this.terminal,
            completer,
            historySize,
            prompt,
            crlfDelay,
            removeHistoryDuplicates,
        })
        this.interrupter = createInterrupter()
        const handleSigInt = () => {
            const reason = new Error("interrupted")
            this.interrupter.interrupt(reason)
        }
        this.rl.addListener("SIGINT", handleSigInt)
    }
    /**
     * @param {string[]} argv
     */
    static argparse(argv) {
        // TODO: implement command line parser
        const [node, script, ...args] = argv
        const dbfile = args[0] || ''
        const cli = new this(dbfile, process.stdout)
        return cli
    }
    nextLine() {
        const nextLine = new Promise((resolve) => {
            if (this.terminal) {
                this.rl.prompt()
            }
            /** @type {(line?: string) => void} */
            const handler = (line) => {
                this.rl.removeListener("line", handler)
                this.rl.removeListener("close", handler)
                if (line != null) {
                    resolve(line)
                } else {
                    resolve(null)
                }
            }
            this.rl.addListener("line", handler)
            this.rl.addListener("close", handler)
        })
        return nextLine
    }
    async start() {
        /** @type {sqlite3.Database} */
        const db = await new Promise((resolve, reject) => {
            const db = new sqlite3.Database(this.dbfile, (err) => {
                if (err) {
                    reject(err)
                    return
                }
                resolve(db)
            })
        })
        try {
            while (true) {
                try {
                    const line = await Promise.race([this.nextLine(), this.interrupter.receiver])
                    if (line == null) {
                        break
                    }
                    if (line.trim() == "") {
                        continue
                    }
                    try {
                        await this.runStatement(db, line)
                    } catch (err) {
                        await write(
                            this.conout,
                            [
                                util.inspect(err, {
                                    colors: this.colors
                                }),
                                "\n",
                            ], (err) => {
                                if (err) {
                                    this.interrupter.interrupt(err)
                                }
                            })
                    }
                } catch (err) {
                    await write(
                        this.conout,
                        [
                            util.inspect(err, {
                                colors: this.colors
                            }),
                            "\n",
                        ], (err) => {
                            if (err) {
                                this.interrupter.interrupt(err)
                            }
                        })
                }
            }
        } finally {
            new Promise((resolve, reject) => {
                db.close((err) => {
                    if (err) {
                        reject(err)
                    } else {
                        resolve()
                    }
                })
            })
        }
    }
    /**
     * @param {sqlite3.Database} db
     * @param {string} sql
     */
    async runStatement(db, sql) {
        /** @type {sqlite3.Statement} */
        const stmt = await new Promise((resolve, reject) => {
            const stmt = db.prepare(sql, (err) => {
                if (err) {
                    reject(err)
                    return
                }
                resolve(stmt)
            })
        })
        try {
            /** @type {() => Promise<Record<string, any> | null>} */
            const getRow = () => {
                return new Promise((resolve, reject) => {
                    stmt.get((err, row) => {
                        if (err) {
                            reject(err)
                            return
                        }
                        resolve(row)
                    })
                })
            }
            let nextRow = getRow()
            /** @type {Record<string, any> | null} */
            let row
            while (row = await Promise.race([nextRow, this.interrupter.receiver])) {
                nextRow = getRow()
                await write(
                    this.output,
                    [
                        JSON.stringify(row),
                        "\n"
                    ], (err) => {
                        if (err) {
                            this.interrupter.interrupt(err)
                        }
                    })
            }
        } finally {
            await new Promise((resolve, reject) => {
                stmt.finalize((err) => {
                    if (err) {
                        reject(err)
                        return
                    }
                    resolve()
                })
            })
        }
    }
}

// run if top level module
if (typeof require !== "undefined" && require.main === module) {
    const cli = SQLite3CLI.argparse(process.argv)
    cli.start()
}

module.exports = SQLite3CLI