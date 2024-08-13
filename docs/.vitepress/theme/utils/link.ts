import { PathLike } from "./tree";

class PageLink implements PathLike {
  readonly url: string;
  readonly title: string;

  constructor(url: string, title: string) {
    this.url = url;
    this.title = title;
  }

  path(): string[] {
    return this.url.split("/").filter(Boolean);
  }
}

export { PageLink };
