const { build } = require("esbuild");

const buildFile = (input, output) =>
	build({
		entryPoints: [input],
		outfile: output,
		bundle: true,
		platform: "node",
		target: "node14",
		define: {
			"process.env.NODE_ENV": '"production"',
		},
		plugins: [
			{
				name: "make-all-packages-external",
				setup(build) {
					let filter = /^[^./]|^\.[^./]|^\.\.[^/]/;
					build.onResolve({ filter }, (args) => ({
						path: args.path,
						external: true,
					}));
				},
			},
		],
		watch: process.argv.includes("-w"),
	});

buildFile(`./src/index.ts`, `./tinyorm.dist.js`).catch((error) => {
	if (!error.errors) {
		console.error(error);
	}
	process.exit(1);
});
