import type { AppProps } from "next/app";

import "./global.css";

export default function Nextra({ Component, pageProps }: AppProps) {
	return <Component {...pageProps} />;
}
