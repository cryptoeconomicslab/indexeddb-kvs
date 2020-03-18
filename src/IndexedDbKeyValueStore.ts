import { KeyValueStore, Iterator, BatchOperation } from '@cryptoeconomicslab/db'
import { Bytes } from '@cryptoeconomicslab/primitives'
const STORE_KEY_PATH = 'key'

class IndexedDbIterator implements Iterator {
  private req: IDBRequest | null = null
  private cursor: IDBCursorWithValue | null = null

  constructor(
    readonly lowerBound: Bytes,
    readonly dbPromise: Promise<IDBDatabase>,
    readonly storeKey: string,
    readonly lowerBoundExclusive?: boolean
  ) {}

  private async init() {
    const db = await this.dbPromise
    const tx = db.transaction(this.storeKey, 'readonly')
    const store = tx.objectStore(this.storeKey)
    const lowerBound = this.lowerBoundExclusive
      ? this.lowerBound.increment()
      : this.lowerBound
    this.req = store.openCursor(
      IDBKeyRange.lowerBound(lowerBound.toHexString())
    )
  }

  public async next(): Promise<{ key: Bytes; value: Bytes } | null> {
    if (!this.cursor) {
      await this.init()
      this.cursor = await new Promise(resolve => {
        if (this.req) {
          this.req.onsuccess = e => {
            resolve((e.target as any).result as IDBCursorWithValue)
          }
        }
      })
      return this.cursor ? createBytesKeyValue({ ...this.cursor.value }) : null
    } else {
      const result: IDBCursorWithValue = await new Promise(resolve => {
        if (this.cursor && this.req) {
          this.cursor.continue()
          this.req.onerror = e => {
            console.error(e)
          }
          this.req.onsuccess = e => {
            resolve((e.target as any).result)
          }
        }
      })
      if (!result) return null
      return result.value ? createBytesKeyValue({ ...result.value }) : null
    }
  }
}

export class IndexedDbKeyValueStore implements KeyValueStore {
  private dbName: Bytes
  private db: IDBDatabase | null = null
  private openPromise: Promise<IDBDatabase>
  private storeKey: string

  constructor(
    name: Bytes = Bytes.fromString('db'),
    key = '_',
    version?: number
  ) {
    this.dbName = name
    this.storeKey = key
    this.openPromise = this.openDb(version)
  }

  private async openDb(version?: number): Promise<IDBDatabase> {
    return await new Promise(resolve => {
      const req = indexedDB.open(this.dbName.intoString(), version)

      req.onsuccess = () => {
        const db = req.result
        db.onversionchange = e => {
          this.db = null
          if (e.newVersion) {
            this.openPromise = this.openDb(e.newVersion)
          }
          db.close()
        }
        resolve(db)
      }

      req.onupgradeneeded = () => {
        const db = req.result
        if (!db.objectStoreNames.contains(this.storeKey)) {
          req.result.createObjectStore(this.storeKey, {
            keyPath: STORE_KEY_PATH
          })
        }
      }
    })
  }

  private async getDb(): Promise<IDBDatabase> {
    return this.db || this.openPromise
  }

  private async getVersion(): Promise<number> {
    const db = await this.getDb()
    return db.version
  }

  private async getObjectStoreNames(): Promise<DOMStringList> {
    const db = await this.getDb()
    return db.objectStoreNames
  }

  public async get(key: Bytes): Promise<Bytes | null> {
    const db = await this.getDb()
    const tx = db.transaction(this.storeKey, 'readonly')
    const store = tx.objectStore(this.storeKey)
    const req = store.get(key.toHexString())

    return await new Promise(resolve => {
      req.onerror = () => {
        resolve(null)
      }
      req.onsuccess = () => {
        const result = req.result ? new Bytes(req.result.value.data) : null
        resolve(result)
      }
    })
  }

  public async put(key: Bytes, value: Bytes): Promise<void> {
    const db = await this.getDb()
    const tx = db.transaction(this.storeKey, 'readwrite')
    const store = tx.objectStore(this.storeKey)
    store.put(createKeyValue(key.toHexString(), value))

    return new Promise((resolve, reject) => {
      tx.onerror = e => {
        console.error(e)
        reject(new Error('cannot complete put operation'))
      }
      tx.oncomplete = () => {
        resolve()
      }
    })
  }

  public async del(key: Bytes): Promise<void> {
    const db = await this.getDb()
    const tx = db.transaction(this.storeKey, 'readwrite')
    const store = tx.objectStore(this.storeKey)
    store.delete(key.toHexString())
    return new Promise((resolve, reject) => {
      tx.onerror = () => {
        reject(new Error('cannot complete put operation'))
      }
      tx.oncomplete = () => {
        resolve()
      }
    })
  }

  public async batch(operations: BatchOperation[]): Promise<void> {
    const db = await this.getDb()
    const tx = db.transaction(this.storeKey, 'readwrite')
    const store = tx.objectStore(this.storeKey)

    operations.forEach(operation => {
      if (operation.type === 'Put') {
        store.put(createKeyValue(operation.key.toHexString(), operation.value))
      } else if (operation.type === 'Del') {
        store.delete(operation.key.toHexString())
      }
    })

    return new Promise((resolve, reject) => {
      tx.onerror = e => {
        console.error(e)
        reject(new Error('cannot complete transaction'))
      }
      tx.oncomplete = () => {
        resolve()
      }
    })
  }

  public iter(lowerBound: Bytes, lowerBoundExclusive?: boolean): Iterator {
    return new IndexedDbIterator(
      lowerBound,
      this.getDb(),
      this.storeKey,
      lowerBoundExclusive
    )
  }

  // use objectStore to manage bucket
  // each bucket has a bucket key as a private field for a key of global objectStore.
  // bucket(key) method returns a new IndexedDbKeyValueStore instance concatenating a given key
  // with parent key.
  // in order to add a new objectStore, bucket must pass new version number to constructor.
  public async bucket(key: Bytes): Promise<KeyValueStore> {
    let version
    const objectStoreNames = await this.getObjectStoreNames()
    const newObjectStoreKey = this.storeKey + key.toHexString()
    if (!objectStoreNames.contains(newObjectStoreKey)) {
      version = (await this.getVersion()) + 1
    }

    const bucket = new IndexedDbKeyValueStore(
      this.dbName,
      newObjectStoreKey,
      version
    )

    // in order for new version to be detected
    await bucket.openDb()
    return Promise.resolve(bucket)
  }

  public async open(): Promise<void> {
    // call getDb to open connection if it's been closed.
    // if it's already open, do nothing.
    await this.getDb()
  }

  public async close(): Promise<void> {
    return await new Promise(resolve => {
      if (this.db) {
        this.db.close()
        this.db.onclose = () => {
          resolve()
        }
      } else {
        resolve()
      }
    })
  }
}

// create key value pair to store in indexeddb
function createKeyValue<T>(key: string, value: T) {
  return {
    key,
    value
  }
}

// create key value pair.
// because type info is lost in IndexedDB, value has to be reinstantiated as Bytes
function createBytesKeyValue({ key, value }: { key: string; value: Bytes }) {
  return {
    key: Bytes.fromHexString(key),
    value: new Bytes(value.data)
  }
}
