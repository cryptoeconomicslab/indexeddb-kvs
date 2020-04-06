import { Bytes } from '@cryptoeconomicslab/primitives'
import { IndexedDbKeyValueStore } from '../src'
import 'fake-indexeddb/auto'

const testDbName = Bytes.fromString('root')

describe('IndexedDbKeyValueStore', () => {
  describe('db upgrade', () => {
    const testDbKey0 = Bytes.fromString('0')
    let kvs: IndexedDbKeyValueStore

    beforeEach(async () => {
      kvs = new IndexedDbKeyValueStore(testDbName)
      await kvs.open()
    })

    afterEach(async () => {
      await kvs.close()
    })

    it('return next value', async () => {
      const a = await kvs.bucket(Bytes.fromString('aaa'))
      const b = await a.bucket(Bytes.fromString('bbb'))
      const bucket: IndexedDbKeyValueStore = (await b.bucket(
        Bytes.fromString('ccc')
      )) as IndexedDbKeyValueStore
      const iter = bucket.iter(testDbKey0)
      const result = await iter.next()
      expect(result).toBeNull()
    })
  })
})
