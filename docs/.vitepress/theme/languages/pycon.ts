import type { MarkdownOptions } from "vitepress";

// The Shiki custom language for interactive Python Console.
// The name "pycon" comes from Pygments, a Python syntax highlighter.
class PyCon {
  static language(): NonNullable<MarkdownOptions["languages"]>[number] {
    return {
      name: "pycon",
      displayName: "Python Console",
      scopeName: "text.python.console",
      repository: {},
      aliases: ["python-console"],
      patterns: [
        {
          match: "^((>>>|\\.\\.\\.)( |$))(.*)$",
          captures: {
            "1": {
              name: "punctuation.definition.prompt.pycon",
            },
            "4": {
              patterns: [
                {
                  include: "source.python",
                },
              ],
            },
          },
        },
      ],
    };
  }

  static transformers(): NonNullable<MarkdownOptions["codeTransformers"]> {
    return [
      {
        tokens(tokens) {
          if (
            this.options.lang !== "pycon" &&
            this.options.lang !== "python-console"
          ) {
            return tokens;
          }
          return tokens.map((line) => {
            // Shiki may merge tokens of the same type or whitespace tokens,
            // so we need to split the prompt token from the Python code in the same line.
            return line.flatMap((token, idx) => {
              if (
                idx === 0 &&
                (token.content.startsWith(">>>") ||
                  token.content.startsWith("..."))
              ) {
                const leftover: typeof line = [];
                if (token.content.length > 4) {
                  leftover.push({
                    ...token,
                    offset: token.offset + 4,
                    content: token.content.slice(4),
                  });
                  token.content = token.content.slice(0, 4);
                } else if (token.content.length < 4) {
                  if (line.length !== 1) {
                    throw new Error(
                      "prompt must be separated from code by whitespace",
                    );
                  }
                  token.content += " ";
                }
                token.htmlAttrs = { class: "pycon-prompt" };
                return [token, ...leftover];
              } else {
                return [token];
              }
            });
          });
        },
      },
    ];
  }
}

export default PyCon;
