interface PathLike {
  path(): string[];
  /**
   * An optional rank of the page among its siblings.
   * A page with a lower rank is listed before a page with a higher rank.
   */
  rank(): number | undefined;
}

interface NodeLike {
  name(): string;
  parent(): string | undefined;
  prev(): string | undefined;
  next(): string | undefined;
}

class TreeNode<T> {
  readonly name: string;
  data: T;
  children: TreeNode<T>[];

  constructor(name: string, data: T, children?: TreeNode<T>[]) {
    this.name = name;
    this.data = data;
    this.children = children ?? [];
  }

  /**
   * Construct trees from a list of nodes, where each node has pointers to its
   * parent, previous node, and next node. The previous and next pointers are
   * determined by pre-order traversal of the tree.
   * @param items The list of nodes.
   * @returns The list of root tree nodes.
   */
  static fromNodes<T extends NodeLike>(items: T[]): TreeNode<T>[] {
    function error(message: string): never {
      const input = items.map((item) => ({
        name: item.name(),
        parent: item.parent(),
        prev: item.prev(),
        next: item.next(),
      }));
      throw new Error(`${message}: ${JSON.stringify(input)}`);
    }

    const nodes = Object.fromEntries(
      items.map((item) => [item.name(), new TreeNode(item.name(), item)]),
    );
    if (items.length !== Object.keys(nodes).length) {
      error("duplicate node names");
    }
    const roots: TreeNode<T>[] = [];

    function lookup(name: string): TreeNode<T> {
      const node = nodes[name];
      if (node === undefined) {
        error(`node '${name}' does not exist`);
      }
      return node;
    }

    // Construct the tree structure of the nodes.
    for (const item of items) {
      const parent = item.parent();
      if (parent === undefined) {
        roots.push(nodes[item.name()]);
      } else {
        lookup(parent).children.push(nodes[item.name()]);
      }
    }

    // Construct the traversal order of the nodes.
    const traversal: string[] = [];
    let current = items.find((item) => item.prev() === undefined);
    while (current !== undefined) {
      traversal.push(current.name());
      if (traversal.length > items.length) {
        error("previous or next links form a cycle");
      }
      const next = current.next();
      if (next === undefined) {
        current = undefined;
      } else {
        const item = lookup(next).data;
        if (item.prev() !== current.name()) {
          error(
            `next node '${item.name()}' does not point to current node '${current.name()}'`,
          );
        }
        current = item;
      }
    }
    if (traversal.length !== items.length) {
      error("invalid previous or next links");
    }

    const rank = Object.fromEntries(
      traversal.map((name, index) => [name, index]),
    );

    function sort(trees: TreeNode<T>[]): void {
      trees.sort((a, b) => rank[a.name] - rank[b.name]);
      trees.forEach((tree) => sort(tree.children));
    }

    // Sort the children of each node by the order of traversal.
    sort(roots);

    return roots;
  }

  /**
   * Construct trees from a list of items, where each item has a path.
   * The path is split into components that defines the hierarchy of the tree.
   * When the prefix is specified, only items that match the prefix are included,
   * and the prefix is removed from the path of each matching item.
   * @param items The list of items where each item has a path.
   * @param prefix The list of path components that defines the prefix.
   * @returns The list of root tree nodes.
   */
  static fromPaths<T extends PathLike>(
    items: T[],
    prefix?: string[],
  ): TreeNode<T | null>[] {
    if (prefix === undefined) {
      prefix = [];
    }
    const roots: TreeNode<T | null>[] = [];

    for (const item of items) {
      const parts = item.path();

      // Skip the item if it does not match the prefix.
      if (prefix.length > parts.length) {
        continue;
      }
      let matches = true;
      for (let index = 0; index < prefix.length; index++) {
        if (prefix[index] !== parts[index]) {
          matches = false;
          break;
        }
      }
      if (!matches) {
        continue;
      }

      let siblings = roots;
      for (let index = prefix.length; index < parts.length; index++) {
        const part = parts[index];
        let node = siblings.find((child) => child.name === part);
        if (node === undefined) {
          node = new TreeNode(part, null);
          siblings.push(node);
        }
        if (index === parts.length - 1) {
          node.data = item;
        }
        siblings = node.children;
      }
    }

    function sort(trees: TreeNode<T | null>[]): void {
      trees.sort((a, b) => {
        const rankA = a.data?.rank();
        const rankB = b.data?.rank();
        if (rankA !== undefined && rankB !== undefined) {
          const diff = rankA - rankB;
          return diff !== 0 ? diff : a.name.localeCompare(b.name);
        }
        if (rankA !== undefined && rankB === undefined) {
          return -1;
        }
        if (rankA === undefined && rankB !== undefined) {
          return 1;
        }
        return a.name.localeCompare(b.name);
      });
      trees.forEach((tree) => sort(tree.children));
    }

    // Sort the children of each node by rank and name.
    sort(roots);

    return roots;
  }

  transform<U>(f: (name: string, data: T) => U): TreeNode<U> {
    return new TreeNode<U>(
      this.name,
      f(this.name, this.data),
      this.children.map((child) => child.transform(f)),
    );
  }

  collect(): T[] {
    const result: T[] = [this.data];
    for (const child of this.children) {
      result.push(...child.collect());
    }
    return result;
  }
}

export { TreeNode, PathLike, NodeLike };
