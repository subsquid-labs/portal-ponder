/** Turn sources.generated.json into resolved per-chain filters + child-extraction rules. */
import { readFileSync } from "node:fs";
import { toEventSelector } from "viem";

export type ChildRule = { kind: "topic"; index: number } | { kind: "data"; word: number };

export type FactorySource = {
  name: string;
  factory: string;
  discoveryTopic0: string;
  childRule: ChildRule;
  childTopic0s: string[]; // topic0 of every child event
  childEventNames: string[];
};
export type SingletonSource = { name: string; address: string; topic0s: string[]; eventNames: string[] };

export type EulerChain = {
  chainId: number;
  dataset: string | null;
  realTime: boolean | null;
  factories: FactorySource[];
  singletons: SingletonSource[];
};

// which indexed arg / data word holds the deployed child address
const CHILD_RULE: Record<string, ChildRule> = {
  EVault: { kind: "topic", index: 1 }, // ProxyCreated(address indexed proxy, ...)
  EulerEarn: { kind: "topic", index: 1 }, // CreateEulerEarn(address indexed eulerEarn, ...)
  EulerSwapPool: { kind: "data", word: 0 }, // PoolDeployed(..., address pool, ...) — pool not indexed
};

const topic0 = (sig: string): string => toEventSelector(sig as `event ${string}`);

export const loadEulerChain = (chainId: number, file = "/Users/dz/Projects/portal-ponder/harness/euler/sources.generated.json"): EulerChain => {
  const all = JSON.parse(readFileSync(file, "utf8"));
  const c = all.chains.find((x: any) => x.chainId === chainId);
  if (!c) throw new Error(`chain ${chainId} not in sources.generated.json`);

  const factories: FactorySource[] = [];
  const singletons: SingletonSource[] = [];
  for (const s of c.sources) {
    if (s.kind === "factory") {
      const childEvents = (s.childEvents ?? []) as { name: string; signature: string }[];
      factories.push({
        name: s.name,
        factory: s.address.toLowerCase(),
        discoveryTopic0: topic0(s.discoveryEvent.signature),
        childRule: CHILD_RULE[s.name] ?? { kind: "topic", index: 1 },
        childTopic0s: childEvents.map((e) => topic0(e.signature)),
        childEventNames: childEvents.map((e) => e.name),
      });
    } else {
      const events = (s.events ?? []) as { name: string; signature: string }[];
      singletons.push({
        name: s.name,
        address: s.address.toLowerCase(),
        topic0s: events.map((e) => topic0(e.signature)),
        eventNames: events.map((e) => e.name),
      });
    }
  }
  return { chainId: c.chainId, dataset: c.portalDataset, realTime: c.realTime, factories, singletons };
};

/** extract a deployed child address from a discovery log per the source's rule */
export const extractChild = (rule: ChildRule, log: { topics?: string[]; data?: string }): string | undefined => {
  if (rule.kind === "topic") {
    const t = log.topics?.[rule.index];
    return t ? "0x" + t.slice(26) : undefined;
  }
  const data = log.data ?? "0x";
  const start = 2 + rule.word * 64;
  const word = data.slice(start, start + 64);
  return word.length === 64 ? "0x" + word.slice(24) : undefined;
};
