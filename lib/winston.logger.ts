import minimist from "minimist";
import { createLogger, format, transports } from "winston";
const args = minimist(process.argv.slice(2), {
  boolean: ["console-logger"],
});
// custom log display format
const customFormat = format.printf(({ timestamp, level, stack, message }) => {
  return `[${level.toUpperCase()}] - ${stack || message}`;
});
let log_level = "info";
if (process.env.LOG_LEVEL) {
  log_level = process.env.LOG_LEVEL;
}
const options = {
  console: {
    level: log_level,
  },
};
const instanceLogger = {
  format: format.combine(
    format.timestamp(),
    format.errors({ stack: true }),
    format.json(),
  ),
  transports: [new transports.Console(options.console)],
};
const consoleLogger = {
  format: format.combine(format.errors({ stack: true }), customFormat),
  transports: [new transports.Console(options.console)],
};
// Don't do json logging when running from console.
const useConsole: boolean =
  args["console-logger"] != undefined ? Boolean(args["console-logger"]) : false;
export const instance = useConsole
  ? createLogger(consoleLogger)
  : createLogger(instanceLogger);
