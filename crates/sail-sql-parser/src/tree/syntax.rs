use std::any::TypeId;
use std::collections::HashMap;

use itertools::Itertools;
use serde::{Deserialize, Serialize};

/// A node in the syntax graph.
/// `K` is the type of the key to uniquely identify a non-terminal node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SyntaxNode<K> {
    /// A choice among multiple nodes.
    Choice(Vec<SyntaxNode<K>>),
    /// A sequence of multiple nodes.
    Sequence(Vec<SyntaxNode<K>>),
    /// An optional node.
    Optional(Box<SyntaxNode<K>>),
    /// A node that can appear zero or more times.
    ZeroOrMore(Box<SyntaxNode<K>>),
    /// A node that can appear one or more times.
    OneOrMore(Box<SyntaxNode<K>>),
    /// A non-terminal node identified by a key.
    NonTerminal(K),
    /// A terminal node identified by its kind.
    Terminal(TerminalKind),
}

impl<K> SyntaxNode<K> {
    /// Maps the keys of non-terminal nodes using the provided function.
    /// The function must ensure that the mapped keys are still unique
    /// in the syntax graph.
    pub fn map<F, L>(self, f: F) -> SyntaxNode<L>
    where
        F: Fn(K) -> L + Copy,
    {
        match self {
            SyntaxNode::Choice(nodes) => {
                SyntaxNode::Choice(nodes.into_iter().map(|n| n.map(f)).collect())
            }
            SyntaxNode::Sequence(nodes) => {
                SyntaxNode::Sequence(nodes.into_iter().map(|n| n.map(f)).collect())
            }
            SyntaxNode::Optional(node) => SyntaxNode::Optional(Box::new(node.map(f))),
            SyntaxNode::ZeroOrMore(node) => SyntaxNode::ZeroOrMore(Box::new(node.map(f))),
            SyntaxNode::OneOrMore(node) => SyntaxNode::OneOrMore(Box::new(node.map(f))),
            SyntaxNode::NonTerminal(k) => SyntaxNode::NonTerminal(f(k)),
            SyntaxNode::Terminal(kind) => SyntaxNode::Terminal(kind),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TerminalKind {
    Keyword(String),
    Operator(String),
    Identifier,
    Variable,
    NumberLiteral,
    IntegerLiteral,
    StringLiteral,
}

/// A syntax descriptor for an AST node type.
pub struct SyntaxDescriptor {
    /// The name of the AST node type.
    pub name: String,
    /// The syntax node representing the structure of the AST node type.
    pub node: SyntaxNode<TypeId>,
    /// A list of AST node types that this AST node type refers to.
    pub children: Vec<(TypeId, Box<dyn Fn() -> SyntaxDescriptor>)>,
}

/// A trait for defining the syntax of an AST node type.
pub trait TreeSyntax {
    /// Returns the syntax descriptor for the AST node type.
    fn syntax() -> SyntaxDescriptor;
}

/// A syntax rule in the syntax graph.
/// This is similar to a context-free grammar production rule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyntaxRule {
    pub name: String,
    pub node: SyntaxNode<String>,
}

/// A syntax graph consisting of multiple syntax rules.
/// This is similar to a context-free grammar.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyntaxGraph {
    pub rules: Vec<SyntaxRule>,
}

impl SyntaxGraph {
    pub fn build<T: TreeSyntax + 'static>() -> Self {
        let mut builder = SyntaxGraphBuilder::new();
        builder.visit(TypeId::of::<T>(), Box::new(T::syntax));
        builder.build()
    }
}

struct SyntaxGraphBuilder {
    visited: HashMap<TypeId, (String, SyntaxNode<TypeId>)>,
}

impl SyntaxGraphBuilder {
    fn new() -> Self {
        Self {
            visited: HashMap::new(),
        }
    }

    fn visit(&mut self, ty: TypeId, f: Box<dyn Fn() -> SyntaxDescriptor>) {
        if self.visited.contains_key(&ty) {
            return;
        }
        let SyntaxDescriptor {
            name,
            node,
            children,
        } = f();
        self.visited.insert(ty, (name, node));
        for (ty, f) in children {
            self.visit(ty, f);
        }
    }

    // It's Ok to panic since the syntax graph is used to generate
    // syntax data at test time, and it relies on the correctness of
    // the AST type definitions.
    // So any error in the syntax graph indicates a bug in the AST
    // and should be surfaced during development.
    #[expect(clippy::panic)]
    fn build(self) -> SyntaxGraph {
        let names = self.visited.values().map(|(name, _)| name.clone()).counts();
        if self.visited.len() != names.len() {
            let duplicated = names
                .iter()
                .filter_map(|(name, count)| if *count > 1 { Some(name) } else { None })
                .collect::<Vec<_>>();
            panic!("duplicated syntax names found: {duplicated:?}");
        }
        let mut rules = self
            .visited
            .iter()
            .map(|(_, (name, node))| SyntaxRule {
                name: name.clone(),
                node: node.clone().map(|ty| {
                    self.visited
                        .get(&ty)
                        .map(|(name, _)| name.clone())
                        .unwrap_or_else(|| panic!("type should be visited: {ty:?}"))
                }),
            })
            .collect::<Vec<_>>();
        rules.sort_by(|a, b| a.name.cmp(&b.name));
        SyntaxGraph { rules }
    }
}
