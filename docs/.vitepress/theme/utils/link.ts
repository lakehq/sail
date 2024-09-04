import { PathLike } from "./tree";

class PageLink implements PathLike {
  readonly url: string;
  readonly title: string;
  private readonly rank_: number | undefined;

  constructor(url: string, title: string, rank?: number) {
    this.url = url;
    this.title = title;
    this.rank_ = rank;
  }

  path(): string[] {
    return this.url.split("/").filter(Boolean);
  }

  rank(): number | undefined {
    return this.rank_;
  }
}

export { PageLink };
