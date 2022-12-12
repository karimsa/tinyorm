import createDebug from "debug";
import pino from "pino";

const transportOptions =
	process.env.NODE_ENV === "test"
		? { transport: { target: "pino-pretty", options: { colorize: true } } }
		: {};

export const logger = pino(
	{
		...transportOptions,
		level: "debug",
	},
	pino.multistream(
		[
			{ stream: process.stdout },
			{ stream: process.stderr, level: "debug" },
			{ stream: process.stderr, level: "warn" },
			{ stream: process.stderr, level: "error" },
		],
		{ dedupe: true },
	),
);

export function debug(namespace: string, message: string, meta: object) {
	if (createDebug.enabled(`tinyorm:${namespace}`)) {
		logger.debug({ ...meta, namespace }, message);
	}
}
