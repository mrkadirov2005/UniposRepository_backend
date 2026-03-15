import fs from "fs";
import path from "path";
import readline from "readline/promises";
import { stdin as input, stdout as output } from "process";
import { fileURLToPath } from "url";
import { google } from "googleapis";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const KEY_DIR = path.join(__dirname, "../key");
const TOKEN_PATH =
  process.env.GOOGLE_DRIVE_OAUTH_TOKEN_PATH ||
  path.join(KEY_DIR, "google_oauth_token.json");

const findClientSecretPath = () => {
  if (process.env.GOOGLE_DRIVE_OAUTH_CLIENT_SECRET_PATH) {
    return process.env.GOOGLE_DRIVE_OAUTH_CLIENT_SECRET_PATH;
  }

  if (!fs.existsSync(KEY_DIR)) {
    return null;
  }

  const fileName = fs
    .readdirSync(KEY_DIR)
    .find((name) => name.startsWith("client_secret_") && name.endsWith(".json"));

  return fileName ? path.join(KEY_DIR, fileName) : null;
};

const main = async () => {
  const secretPath = findClientSecretPath();
  if (!secretPath || !fs.existsSync(secretPath)) {
    throw new Error(
      "OAuth client secret file not found. Put client_secret_*.json in key/ or set GOOGLE_DRIVE_OAUTH_CLIENT_SECRET_PATH."
    );
  }

  const payload = JSON.parse(fs.readFileSync(secretPath, "utf8"));
  const source = payload.installed || payload.web;

  if (!source?.client_id || !source?.client_secret) {
    throw new Error("Invalid client secret JSON. Missing client_id/client_secret.");
  }

  const redirectUri =
    process.env.GOOGLE_DRIVE_REDIRECT_URI ||
    (source.redirect_uris && source.redirect_uris[0]) ||
    "http://localhost";

  const client = new google.auth.OAuth2(
    source.client_id,
    source.client_secret,
    redirectUri
  );

  const authUrl = client.generateAuthUrl({
    access_type: "offline",
    prompt: "consent",
    scope: ["https://www.googleapis.com/auth/drive"],
  });

  console.log("\nOpen this URL in your browser and approve access:");
  console.log(authUrl);

  const rl = readline.createInterface({ input, output });
  const code = await rl.question("\nPaste the authorization code here: ");
  rl.close();

  const { tokens } = await client.getToken(code.trim());
  if (!tokens.refresh_token) {
    throw new Error(
      "Google did not return a refresh_token. Re-run and ensure consent is prompted."
    );
  }

  fs.mkdirSync(path.dirname(TOKEN_PATH), { recursive: true });
  fs.writeFileSync(
    TOKEN_PATH,
    JSON.stringify(
      {
        refresh_token: tokens.refresh_token,
        scope: tokens.scope,
        token_type: tokens.token_type,
        created_at: new Date().toISOString(),
      },
      null,
      2
    )
  );

  console.log(`\nSaved refresh token to: ${TOKEN_PATH}`);
  console.log("Backups will now use OAuth user mode automatically.");
};

main().catch((err) => {
  console.error(`\nFailed to generate token: ${err.message}`);
  process.exit(1);
});
