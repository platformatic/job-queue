import eslintPluginPrettier from 'eslint-plugin-prettier'
import { globalIgnores } from 'eslint/config'
import neostandard from 'neostandard'

const eslint = [
  ...neostandard({ ts: true }),
  globalIgnores(['dist/',]),
  {
    files: ['**/*.ts'],
    rules: {
      '@typescript-eslint/consistent-type-imports': ['error', { fixStyle: 'inline-type-imports' }],
      'prettier/prettier': 'error'
    },
    plugins: {
      prettier: eslintPluginPrettier
    }
  }
]

export default eslint
