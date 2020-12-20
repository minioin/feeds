// KV implemented using appendonly log

import { readLines } from "https://deno.land/std@0.79.0/io/bufio.ts";
import {
  ensureFile,
  exists,
  move,
} from "https://deno.land/std@0.79.0/fs/mod.ts";
import { CSVWriter, readCSV } from "https://deno.land/x/csv/mod.ts";

const csvOptions = {
  columnSeparator: ",",
  lineSeparator: "\n",
};

export class KV {
  _file: string;
  _kv: Record<string, string>;

  constructor(file: string) {
    this._file = file;
    this._kv = {};
  }
  async ensureUnlocked() {
    const lockFile = this._file + ".lock";
    let lockExists = await exists(lockFile);
    if (lockExists) {
      throw new Error("File locked by another process");
    }
  }

  async load(compact = false) {
    const lockFile = this._file + ".lock";
    try {
      await this.ensureUnlocked();
      if (compact) {
        await ensureFile(lockFile);
      }
      await ensureFile(this._file);
      const file = await Deno.open(this._file);
      for await (
        const row of readCSV(file, csvOptions)
      ) {
        const cells = [];
        for await (const cell of row) {
          cells.push(cell);
        }
        const [key, ...value] = cells;
        if (!key || !value) continue;
        this._kv[key] = value.join("").trim();
      }

      if (compact) {
        this._compactFile();
      }
    } catch (e) {
      // Ignore
    }
    if (compact) {
      await Deno.remove(lockFile);
    }
  }

  async _compactFile() {
    const compactFile = this._file + "_compact";
    const file = await Deno.open(
      compactFile,
      { write: true, create: true, truncate: true },
    );
    const writer = new CSVWriter(file, {
      columnSeparator: ",",
      lineSeparator: "\n",
    });
    for (const key in this._kv) {
      if (!key || !this._kv[key]) continue;
      await writer.writeCell(key);
      await writer.writeCell(this._kv[key] as string);
      await writer.nextLine();
    }
    file.close();
    await move(compactFile, this._file, { overwrite: true });
  }

  get(key: string): string {
    return this._kv[key];
  }

  async put(key: string, value: string) {
    await this.ensureUnlocked();
    const file = await Deno.open(
      this._file,
      { write: true, create: true, truncate: false, append: true },
    );
    const writer = new CSVWriter(file, {
      columnSeparator: ",",
      lineSeparator: "\n",
    });
    if (!key || !value) {
      throw new Error("Null or empty value provided to kv");
    }
    await writer.writeCell(key);
    await writer.writeCell(value);
    await writer.nextLine();
    file.close();
    this._kv[key] = value;
  }
}
