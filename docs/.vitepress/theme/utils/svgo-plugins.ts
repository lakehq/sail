import type { CustomPlugin } from "svgo";

// An SVGO plugin to set the class on the SVG element.
const setClass = (className: string): CustomPlugin => {
  return {
    name: "setClass",
    fn: () => {
      return {
        root: {
          exit: (node) => {
            for (const child of node.children) {
              if (child.type === "element" && child.name === "svg") {
                child.attributes.class = className;
              }
            }
          },
        },
      };
    },
  };
};

// An SVGO plugin to scale SVG size by a given factor.
const scaleSize = (scale: number): CustomPlugin => {
  if (scale <= 0) {
    throw new Error(`invalid scale factor: ${scale}`);
  }

  // Scales a value with an optional unit.
  function scaleValue(value?: string): string {
    if (value === undefined || value === null || value === "") {
      throw new Error("value must be a non-empty string");
    }
    return value.replace(/^([\d.]+)(.*)$/, (_, num, unit) => {
      return `${num * scale}${unit}`;
    });
  }

  return {
    name: "scaleSize",
    fn: () => {
      return {
        root: {
          exit: (node) => {
            for (const child of node.children) {
              if (child.type === "element" && child.name === "svg") {
                child.attributes.width = scaleValue(child.attributes.width);
                child.attributes.height = scaleValue(child.attributes.height);
              }
            }
          },
        },
      };
    },
  };
};

export { setClass, scaleSize };
