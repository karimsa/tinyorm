module.exports = {
	clearMocks: true,
	collectCoverage: false,
	coverageDirectory: "coverage",
	transform: {
		"^.+\\.tsx?$": "esbuild-jest",
	},
	testMatch: ["**/__tests__/**/*.spec.ts"],
};
