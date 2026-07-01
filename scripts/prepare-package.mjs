// Assembles a publish-ready package under dist/ that preserves the historical
// tarball layout: the compiled sources live under `dist/src/**` and the
// package's `main`/`types` point at `./src/index.js`. Consumers deep-import
// paths like `@clipboard-health/mongo-jobs/src/lib/testing`, so this layout
// MUST be kept stable across releases.
import { copyFileSync, existsSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const dist = join(root, "dist");

if (!existsSync(join(dist, "src", "index.js"))) {
  throw new Error("dist/src/index.js not found — run `node --run build` first.");
}

const pkg = JSON.parse(readFileSync(join(root, "package.json"), "utf8"));

const publishPkg = {
  name: pkg.name,
  version: pkg.version,
  description: pkg.description,
  keywords: pkg.keywords,
  bugs: pkg.bugs,
  license: pkg.license,
  repository: pkg.repository,
  type: pkg.type,
  main: "./src/index.js",
  typings: "./src/index.d.ts",
  types: "./src/index.d.ts",
  publishConfig: pkg.publishConfig,
  dependencies: pkg.dependencies,
  peerDependencies: pkg.peerDependencies,
};

writeFileSync(join(dist, "package.json"), `${JSON.stringify(publishPkg, null, 2)}\n`);

for (const file of ["README.md", "LICENSE"]) {
  if (existsSync(join(root, file))) {
    copyFileSync(join(root, file), join(dist, file));
  }
}

console.log(`Prepared dist/ for publishing (${publishPkg.name}@${publishPkg.version}).`);
