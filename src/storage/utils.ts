const cachedModules = new Map<string, unknown>()

export async function loadOptionalDependency<T> (moduleName: string, caller: string): Promise<T> {
  const existing = cachedModules.get(moduleName)
  if (existing) {
    return existing as T
  }

  try {
    let imported = await import(moduleName)
    if (imported.default) {
      imported = imported.default
    }

    cachedModules.set(moduleName, imported)
    return imported as T
  } catch (error) {
    if (
      (error as NodeJS.ErrnoException).code === 'ERR_MODULE_NOT_FOUND' &&
      (error as Error).message.includes(`'${moduleName}'`)
    ) {
      throw new Error(
        `${caller} requires the optional dependency '${moduleName}'. Install it with: npm install ${moduleName}`
      )
    }

    throw error
  }
}
