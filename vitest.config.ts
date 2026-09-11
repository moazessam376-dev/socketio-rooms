import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    exclude: ["node_modules/**", "dist/**"],
    fileParallelism: false,
    hookTimeout: 10000,
    testTimeout: 10000,
  },
});
