import { defineConfig } from "@hey-api/openapi-ts";

export default defineConfig({
  input: "./open-api.json",
  output: {
    path: "src/api/generated",
    fileName: {
      case: "preserve",
    },
    tsConfigPath: "./tsconfig.json",
    postProcess: ["oxlint", "oxfmt"],
  },
  plugins: [
    {
      name: "@hey-api/client-ky",
    },
    {
      name: "@pinia/colada",
      queryOptions: true,
      mutationOptions: true,
      queryKeys: true,
    },
    {
      name: "valibot",
      requests: true,
      responses: true,
      definitions: true,
      metadata: true,
    },
  ],
});
