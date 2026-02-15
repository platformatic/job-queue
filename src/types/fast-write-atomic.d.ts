declare module 'fast-write-atomic' {
  export interface WriteOptions {
    mode?: number
  }

  interface FastWriteAtomic {
    promise (
      path: string,
      data: string | Buffer,
      options?: WriteOptions
    ): Promise<void>
  }

  const fastWriteAtomic: FastWriteAtomic
  export default fastWriteAtomic
}
