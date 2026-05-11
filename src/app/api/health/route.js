import { NextResponse } from "next/server";
import { getAdapter } from "@/lib/db/driver.js";

export async function GET() {
  const db = await getAdapter();
  return NextResponse.json({
    ok: true,
    db: {
      driver: db.driver,
      storage: db.storage || "local",
    },
    env: {
      vercel: process.env.VERCEL === "1" || process.env.VERCEL === "true",
      vercelEnv: process.env.VERCEL_ENV || "",
      nextPhase: process.env.NEXT_PHASE || "",
      supabaseConfigured: Boolean(process.env.SUPABASE_URL && (process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SECRET_KEY)),
      upstashConfigured: Boolean((process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL) && (process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN)),
    },
  }, {
    headers: { "Cache-Control": "no-store" },
  });
}
