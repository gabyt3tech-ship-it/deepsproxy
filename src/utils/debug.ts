export type LogLevel = 'debug' | 'info' | 'warn' | 'error';

const LOG_LEVELS: Record<LogLevel, number> = {
  debug: 0,
  info: 1,
  warn: 2,
  error: 3,
};

function getEffectiveLevel(): number {
  const env = process.env.LOG_LEVEL?.toLowerCase() as LogLevel | undefined;
  if (env && env in LOG_LEVELS) {
    return LOG_LEVELS[env];
  }
  if (process.env.DEBUG === 'true' || process.env.DEBUG === '1') {
    return LOG_LEVELS.debug;
  }
  return LOG_LEVELS.info;
}

const currentLevel = getEffectiveLevel();

export function debug(...args: unknown[]) {
  if (currentLevel <= LOG_LEVELS.debug) {
    console.log('[DEBUG]', ...args);
  }
}

export function info(...args: unknown[]) {
  if (currentLevel <= LOG_LEVELS.info) {
    console.log('[INFO]', ...args);
  }
}

export function warn(...args: unknown[]) {
  if (currentLevel <= LOG_LEVELS.warn) {
    console.warn('[WARN]', ...args);
  }
}

export function debugError(...args: unknown[]) {
  if (currentLevel <= LOG_LEVELS.error) {
    console.error('[ERROR]', ...args);
  }
}
