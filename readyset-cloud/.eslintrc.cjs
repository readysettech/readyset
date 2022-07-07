module.exports = {
  root: true,
  parser: "@typescript-eslint/parser",
  plugins: ["@typescript-eslint", "jest"],
  env: {
    es6: true,
    node: true,
  },
  parserOptions: {
    tsconfigRootDir: __dirname,
    project: [
      "./tsconfig.eslint.json",
      "./packages/*/tsconfig.json",
      "./apps/*/tsconfig.json",
    ],
  },
  extends: [
    "eslint:recommended",
    "plugin:@typescript-eslint/eslint-recommended",
    "plugin:@typescript-eslint/recommended",
    "plugin:@typescript-eslint/recommended-requiring-type-checking",
    "plugin:@typescript-eslint/strict",
    "plugin:prettier/recommended",
    "plugin:jest/recommended",
  ],
  /* eslint-disable @typescript-eslint/naming-convention */
  rules: {
    "@typescript-eslint/naming-convention": ["warn"],
  },
  /* eslint-enable @typescript-eslint/naming-convention */
};
