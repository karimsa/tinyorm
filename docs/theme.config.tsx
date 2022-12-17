import React from "react";
import { DocsThemeConfig, useConfig } from "nextra-theme-docs";

const config: DocsThemeConfig = {
	logo: <span>TinyORM</span>,
	head: () => {
		const { title } = useConfig();

		return (
			<>
				<title>{title} - TinyORM</title>
			</>
		);
	},
	project: {
		link: "https://github.com/karimsa/tinyorm",
	},
};

export default config;
