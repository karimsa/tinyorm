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
	docsRepositoryBase: "https://github.com/karimsa/tinyorm",
	editLink: {
		component: ({
			children,
			className,
			filePath,
		}: {
			children: React.ReactNode;
			className?: string;
			filePath?: string;
		}) =>
			filePath?.startsWith("pages/reference/") ? (
				<span className={className} style={{ pointerEvents: "none" }}>
					This page is auto-generated
				</span>
			) : (
				<a
					href={`https://github.com/karimsa/tinyorm/tree/master/docs/${filePath}`}
					target="_blank"
					rel='noreferrer'
					className={className}
				>
					{children}
				</a>
			),
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
