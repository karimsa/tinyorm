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
	useNextSeoProps() {
		return {
			titleTemplate: "%s - TinyORM",
		};
	},
	footer: {
		text: (
			<span>
				MIT {new Date().getFullYear()} &copy;{" "}
				<a href="https://alibhai.co" target="_blank" rel="noreferrer">
					Karim Alibhai
				</a>
				.
			</span>
		),
	},
};

export default config;
