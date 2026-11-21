import { copyFile } from "node:fs/promises";

await copyFile(
  new URL("../../api/proto/kvstore/v1/kvstore.proto", import.meta.url),
  new URL("./dist/kvstore.proto", import.meta.url),
);

await copyFile(
  new URL("../../LICENSE", import.meta.url),
  new URL("./dist/LICENSE", import.meta.url),
);
