# Documentation

Read in this order; stop when you have what you need.

| If you want to… | Read |
|---|---|
| understand the idea in 10 minutes, with diagrams | [How it works](how-it-works.md) ([en français](fr/comment-ca-marche.md)) |
| integrate it: adapters, Redis, HTTP contract, options, your own store | [Using Rebound](usage.md) |
| know exactly what is guaranteed, and the wire format | [Spec](02-spec.md) |
| see why the problem exists and what else is out there | [The problem](01-problem.md) |
| change the code | [Architecture](03-architecture.md), then [CONTRIBUTING](../.github/CONTRIBUTING.md) |
| run the chaos tester, or port the server to another language | [Conformance tester](06-conformance.md) |
| understand the example app and the TypeScript client | [Example and client](08-example-and-client.md) |
| know how far it goes under load | [Stress campaign v0.1.0](benchmarks/v0.1.0/README.md) |

## Folders

- [`adr/`](adr): Architecture Decision Records, one per change to the API, the protocol or the
  dependencies.
- [`benchmarks/`](benchmarks): stress campaign reports. Each version keeps its report, the scripts
  that built its tables and charts, and every raw output (`raw/`: logs, CSV samples, JSON reports).
- [`project/`](project): for maintainers: [code standards](project/code-standards.md), the
  [roadmap](project/roadmap.md) of the milestones and the work [journal](project/progress.md).
- [`fr/`](fr), [`img/`](img): the French version of *How it works* and the diagrams.
