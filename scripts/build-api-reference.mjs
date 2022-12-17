#!/usr/bin/env zx

import * as fs from "fs";
import * as path from "path";

const docNodes = require("../docs/tmp/api-reference.json").children;
const mainExport = fs.readFileSync(
	path.resolve(__dirname, "..", "src", "index.ts"),
	"utf8",
);

function getPageFileName(pageName) {
	return pageName.toLowerCase().replace(/[^a-z]+/g, "-");
}

function buildTextBlock(content) {
	if (!content) {
		return "";
	}
	if (!Array.isArray(content.summary)) {
		console.dir({ content }, { depth: null });
		throw new Error(`Expected array of content nodes`);
	}
	return content.summary
		.map((node) => {
			if (node.kind === "text" || node.kind === "code") {
				return node.text;
			}
			throw new Error(`Unrecognized content node: ${node.kind}`);
		})
		.join("");
}

function buildComment(node) {
	const descriptionTag = node.summary?.blockTags?.find(
		(tag) => tag.tag === "@description",
	);
	if (descriptionTag) {
		return buildTextBlock(descriptionTag.content);
	}
	return buildTextBlock(node.comment);
}

function buildChildNode(node) {
	const params = node.parameters?.filter((param) => param.comment);

	return [
		`### ${node.name} <TypeBadge>${node.kindString}</TypeBadge>`,
		``,
		getNodeSources(node),
		``,
		buildComment(node),
		``,
		node.parameters?.length === 0 ? `**Parameters:** None.` : ``,
		params?.length > 0 ? `**Parameters:**` : ``,
		...(params?.map((param) => {
			return [
				` - ${param.name}`,
				...(param.comment ? [`: ${buildTextBlock(param.comment)}`] : []),
			].join("");
		}) ?? []),
		``,
	].join("\n");
}

function getNodeSources(node) {
	return node.sources.map((source) => {
		return `**Source:** [${
			source.fileName
		}](${`https://github.com/karimsa/tinyorm/tree/master/${source.fileName}#L${source.line}`})`;
	});
}

function buildPage(node) {
	const children =
		node.children?.flatMap((childNode) => {
			if (childNode.kindString === "Constructor") {
				return [];
			}

			const flags = childNode?.flags ?? {};
			if (flags.isExternal || flags.isPrivate || flags.isProtected) {
				return [];
			}

			if (childNode.signatures) {
				return childNode.signatures.map((signature) => ({
					...signature,
					...childNode,
				}));
			}
			return [childNode];
		}) ?? [];
	const properties = children.filter(
		(childNode) => childNode.kindString === "Property",
	);
	const methods = children.filter(
		(childNode) => childNode.kindString === "Method",
	);

	return [
		`import { TypeBadge } from '../../components/TypeBadge';`,
		``,
		`# ${node.name} <TypeBadge>${node.kindString}</TypeBadge>`,
		``,
		getNodeSources(node),
		``,
		buildComment(node),
		``,
		...(properties.length > 0
			? [
					`## Properties`,
					``,
					...properties.map(
						(childNode) =>
							` - [${childNode.name}](#${getPageFileName(
								childNode.name,
							)}-${childNode.kindString.toLowerCase()})`,
					),
					``,
			  ]
			: []),
		...(methods.length > 0
			? [
					`## Methods`,
					``,
					...methods.map(
						(childNode) =>
							` - [${childNode.name}](#${getPageFileName(
								childNode.name,
							)}-${childNode.kindString.toLowerCase()})`,
					),
					``,
			  ]
			: []),
		...properties.map((childNode) => buildChildNode(childNode)),
		...methods.map((childNode) => buildChildNode(childNode)),
		``,
	].join("\n");
}

const meta = {};

for (const node of docNodes) {
	if (!mainExport.includes(node.name)) {
		continue;
	}

	const fileName = getPageFileName(node.name);
	console.dir(Object.keys(node));

	meta[fileName] = node.name;
	fs.writeFileSync(
		path.resolve(
			__dirname,
			"..",
			"docs",
			"pages",
			"reference",
			`${fileName}.mdx`,
		),
		buildPage(node),
	);
}

fs.writeFileSync(
	path.resolve(__dirname, "..", "docs", "pages", "reference", "_meta.json"),
	JSON.stringify(meta, null, "\t"),
);
