import pino from "pino";
import createDebug from "debug";

export const logger = pino(
	{
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
