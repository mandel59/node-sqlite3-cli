#!/usr/bin/env node
const util = require("util")
const readline = require("readline")
const sqlite3 = require("sqlite3")

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
        /** @type {Promise<sqlite3.Database>} */
        this.db = new Promise((resolve, reject) => {
            const db = new sqlite3.Database(dbfile, (err) => {
                if (err) {
                    reject(err)
                    return
                }
                resolve(db)
            })
        })
        this.conin = conin
        this.conout = conout
        this.output = output
        this.rl = readline.createInterface({
            input: conin,
            output: conout,
            terminal,
            completer,
            historySize,
            prompt,
            crlfDelay,
            removeHistoryDuplicates,
        })
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
    async nextLine() {
        if ( /** @type {import("tty").WriteStream} */ (this.conout).isTTY) {
            this.rl.prompt()
        }
        return new Promise((resolve) => {
            /** @type {(line?: string) => void} */
            const handler = (line) => {
                this.rl.removeListener("line", handler)
                this.rl.removeListener("close", handler)
                resolve(line)
            }
            this.rl.addListener("line", handler)
            this.rl.addListener("close", handler)
        })
    }
    async start() {
        /** @type {((reason?: any) => void) | undefined} */
        let interrupt = undefined
        const handleSigInt = () => {
            if (interrupt) {
                const reason = new Error("interrupted")
                interrupt(reason)
            }
        }
        this.rl.addListener("SIGINT", handleSigInt)
        const db = await this.db
        while (true) {
            const line = await this.nextLine()
            if (line == null) {
                break
            }
            if (line.trim() == "") {
                continue
            }
            try {
                /** @type {sqlite3.Statement} */
                const stmt = await new Promise((resolve, reject) => {
                    const stmt = db.prepare(line, (err) => {
                        if (err) {
                            reject(err)
                            return
                        }
                        resolve(stmt)
                    })
                })
                /** @type {() => Promise<Record<string, any> | null>} */
                const getRow = () => {
                    return new Promise((resolve, reject) => {
                        interrupt = reject
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
                while (row = await nextRow) {
                    nextRow = getRow()
                    if (!this.output.write(JSON.stringify(row) + "\n")) {
                        await new Promise(resolve => {
                            this.output.on("drain", resolve)
                        })
                    }
                }
            } catch (err) {
                const ok = this.conout.write(
                    util.inspect(err, {
                        colors: this.rl.terminal
                    }) + "\n")
                if (!ok) {
                    await new Promise(resolve => {
                        this.conout.on("drain", resolve)
                    })
                }
            } finally {
                interrupt = undefined
            }
        }
    }
}

// run if top level module
if (typeof require !== "undefined" && require.main === module) {
    const cli = SQLite3CLI.argparse(process.argv)
    cli.start()
}

module.exports = SQLite3CLI