import { roomNameSchema } from "./events.js";

export function parseName(auth: unknown): string | null {
  if (typeof auth !== "object" || auth === null || !("name" in auth)) {
    return null;
  }

  const result = roomNameSchema.safeParse(auth.name);
  return result.success ? result.data : null;
}
