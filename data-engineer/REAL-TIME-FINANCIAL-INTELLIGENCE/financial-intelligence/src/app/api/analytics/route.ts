import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { NextResponse } from "next/server";


export async function GET() {
  try {
    const filePath = join(process.cwd(), "data", "dashboard.json");
    const fileContents = await readFile(filePath, "utf8");
    const dashboardData = JSON.parse(fileContents);

    return NextResponse.json(dashboardData);
  } catch (error) {
    console.error("Failed to load dashboard analytics:", error);

    return NextResponse.json(
      {
        error:
          "Dashboard data is unavailable. Run the Spark dashboard export script first.",
      },
      { status: 503 }
    );
  }
}
