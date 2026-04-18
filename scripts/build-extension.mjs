import { build, context } from "esbuild";
import { cp, mkdir } from "node:fs/promises";
import { resolve } from "node:path";

const watchMode = process.argv.includes("--watch");
const root = process.cwd();
const outdir = resolve(root, "extension/dist");

const commonConfig = {
  bundle: true,
  platform: "browser",
  target: "chrome114",
  outbase: "extension/src",
  outdir,
  entryNames: "[dir]/index",
  sourcemap: true,
  logLevel: "info",
};

async function copyStaticAssets() {
  await mkdir(resolve(outdir, "ui"), { recursive: true });
  await cp(resolve(root, "extension/manifest.json"), resolve(root, "extension/dist/manifest.json"));
  await cp(resolve(root, "extension/src/ui/panel.css"), resolve(root, "extension/dist/ui/panel.css"));
}

async function buildAll() {
  await build({
    ...commonConfig,
    entryPoints: [
      "extension/src/background/index.ts",
      "extension/src/content/index.ts",
    ],
  });
  await copyStaticAssets();
}

if (watchMode) {
  const ctx = await context({
    ...commonConfig,
    entryPoints: [
      "extension/src/background/index.ts",
      "extension/src/content/index.ts",
    ],
    plugins: [
      {
        name: "copy-static-assets",
        setup(buildApi) {
          buildApi.onEnd(async () => {
            await copyStaticAssets();
          });
        },
      },
    ],
  });

  await ctx.watch();
  console.log("Watching extension sources...");
} else {
  await buildAll();
}

