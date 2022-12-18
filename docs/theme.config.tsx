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
	navbar: {
		extraContent: (
			<>
				<a href={`https://npmjs.org/@karimsa/tinyorm`}>
					<img
						src={
							"https://img.shields.io/npm/v/@karimsa/tinyorm?label=latest&color=green"
						}
						alt='latest npm version'
					/>
				</a>
				<a href={`https://npmjs.org/@karimsa/tinyorm/next`}>
					<img
						src={
							"https://img.shields.io/npm/v/@karimsa/tinyorm/next?label=next"
						}
						alt='beta npm version'
					/>
				</a>
			</>
		),
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
