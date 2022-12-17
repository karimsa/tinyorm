import React from "react";
import classes from "./TypeBadge.module.css";

export const TypeBadge: React.FC<{ children: string }> = ({ children }) => (
	<span className={classes.typeBadge}>{children}</span>
);
