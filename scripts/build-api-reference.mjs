#!/usr/bin/env zx

import * as fs from "fs";
import * as path from "path";

function flattenNode(node) {
	if (node.kindString === "Reference") {
		return [];
	}
	if (node.kind === 1 || node.kindString === "Module") {
		return node.children.flatMap((childNode) => flattenNode(childNode));
	}
	if (node.signatures) {
		return node.signatures.map((signature) => ({
			...signature,
			...node,
		}));
	}
	return [node];
}

const docNodes = flattenNode(require("../docs/tmp/api-reference.json")).sort(
	(left, right) => {
		return left.name.localeCompare(right.name);
	},
);
const mainExport = fs.readFileSync(
	path.resolve(__dirname, "..", "src", "index.ts"),
	"utf8",
);

function getPageFileName(pageName) {
	return pageName.toLowerCase().replace(/[^a-z]+/g, "-");
}

function buildTextBlock(headingLevel, content) {
	if (!content) {
		return [];
	}
	if (!Array.isArray(content.summary)) {
		console.dir({ content }, { depth: null });
		throw new Error(`Expected array of content nodes`);
	}
	return content.summary.flatMap((node) => {
		if (node.kind === "text" || node.kind === "code") {
			return [node.text];
		}
		if (node.kind === "inline-tag" && node.tag === "@link") {
			return [`[${node.text}](/reference/${getPageFileName(node.text)})`];
		}
		if (node.kind === "inline-tag" && node.tag === "@embedDocs") {
			const refNode = docNodes.find(
				(refNode) =>
					refNode.name === node.text && refNode.kindString !== "Reference",
			);
			if (!refNode) {
				throw new Error(`Could not find node for @embedDocs: ${node.text}`);
			}

			return buildNodeContent(headingLevel, refNode);
		}
		throw new Error(`Unrecognized content node: ${node.kind}`);
	});
}

function buildComment(headingLevel, node) {
	const descriptionTag = node.summary?.blockTags?.find(
		(tag) => tag.tag === "@description",
	);
	if (descriptionTag) {
		return buildTextBlock(headingLevel, descriptionTag.content);
	}
	return buildTextBlock(headingLevel, node.comment);
}

function buildFunctionNode(headingLevel, node) {
	const params = node.parameters?.filter((param) => param.comment);

	return [
		``,
		node.parameters?.length === 0 ? `**Parameters:** None.` : ``,
		params?.length > 0 ? `**Parameters:**` : ``,
		...(params?.flatMap((param) => {
			return [
				` - ${param.name}`,
				...(param.comment
					? [`: `, ...buildTextBlock(headingLevel + 1, param.comment)]
					: []),
			];
		}) ?? []),
		``,
	];
}

function getNodeSources(node) {
	return node.sources.map((source) => {
		return `**Source:** [${
			source.fileName
		}](${`https://github.com/karimsa/tinyorm/tree/master/${source.fileName}#L${source.line}`})`;
	});
}

function buildClassNode(headingLevel, node) {
	const children =
		node.children?.flatMap((childNode) => {
			if (childNode.kindString === "Constructor") {
				return [];
			}

			const flags = childNode?.flags ?? {};
			if (flags.isExternal || flags.isPrivate || flags.isProtected) {
				return [];
			}

			return flattenNode(childNode);
		}) ?? [];
	const properties = children.filter(
		(childNode) => childNode.kindString === "Property",
	);
	const methods = children.filter(
		(childNode) => childNode.kindString === "Method",
	);

	return [
		``,
		...(properties.length > 0
			? [
					`${"#".repeat(headingLevel)} Properties`,
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
					`${"#".repeat(headingLevel)} Methods`,
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
		...properties.flatMap((childNode) =>
			buildNode(headingLevel + 1, childNode),
		),
		...methods.flatMap((childNode) => buildNode(headingLevel + 1, childNode)),
		``,
	];
}

function buildNodeContent(headingLevel, node) {
	switch (node.kindString) {
		case "Class":
		case "Interface":
		case "Type alias":
			return buildClassNode(headingLevel, node);
		case "Function":
		case "Method":
			return buildFunctionNode(headingLevel, node);
		case "Property":
			return [];

		default:
			throw new Error(`Unrecognized node type: ${node.kindString}`);
	}
}

function buildNode(headingLevel, node) {
	return [
		`${"#".repeat(headingLevel)} ${node.name} <TypeBadge>${
			node.kindString
		}</TypeBadge>`,
		``,
		...getNodeSources(node),
		``,
		...buildComment(headingLevel + 1, node),
		...buildNodeContent(headingLevel + 1, node),
	];
}

function buildPage(node) {
	const suffixContent =
		node.comment?.blockTags?.flatMap((tag) =>
			tag.tag === "@docs" ? tag.content : [],
		) ?? [];

	return [
		`import { TypeBadge } from '../../components/TypeBadge';`,
		``,
		...buildNode(1, node),
		...buildTextBlock(2, { summary: suffixContent }),
	];
}

const meta = {};

for (const node of docNodes) {
	// Hide things that are not explicitly exported
	if (!mainExport.includes(node.name)) {
		continue;
	}

	const fileName = getPageFileName(node.name);
	const page = buildPage(node);

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
		page.join("\n"),
	);
}

fs.writeFileSync(
	path.resolve(__dirname, "..", "docs", "pages", "reference", "_meta.json"),
	JSON.stringify(meta, null, "\t"),
);
