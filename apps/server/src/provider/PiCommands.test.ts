import { expect, it } from "@effect/vitest";

import {
  expandPiSkillReference,
  parsePiCompactCommand,
  hasPiNavigateTreeCommand,
  parsePiDiscoveredCommands,
  PI_COMPACT_SLASH_COMMAND,
  withPiBuiltinSlashCommands,
} from "./PiCommands.ts";

it("maps current Pi skill metadata to T3's user and project skill scopes", () => {
  expect(
    parsePiDiscoveredCommands({
      commands: [
        {
          name: "skill:global-review",
          description: "Review changes.",
          source: "skill",
          sourceInfo: {
            path: "/home/test/.agents/skills/global-review/SKILL.md",
            scope: "user",
          },
        },
        {
          name: "skill:project-deploy",
          description: "Deploy this project.",
          source: "skill",
          sourceInfo: {
            path: "/workspace/.agents/skills/project-deploy/SKILL.md",
            scope: "project",
          },
        },
        { name: "hello", description: "Say hello.", source: "extension" },
      ],
    }),
  ).toEqual({
    skills: [
      {
        name: "global-review",
        description: "Review changes.",
        path: "/home/test/.agents/skills/global-review/SKILL.md",
        scope: "user",
        enabled: true,
      },
      {
        name: "project-deploy",
        description: "Deploy this project.",
        path: "/workspace/.agents/skills/project-deploy/SKILL.md",
        scope: "project",
        enabled: true,
      },
    ],
    slashCommands: [{ name: "hello", description: "Say hello." }],
  });
});

it("maps the get_commands payload Prime Agent 0.9.8 sends", () => {
  const sourceInfo = (path: string, scope: string) => ({
    path,
    source: "auto",
    scope,
    origin: "top-level",
    baseDir: "/Users/me/.prime/agent",
  });
  expect(
    parsePiDiscoveredCommands({
      commands: [
        {
          name: "ant-account",
          description: "Show or switch the Anthropic subscription account",
          source: "extension",
          sourceInfo: sourceInfo("/Users/me/.prime/agent/extensions/ant/index.ts", "user"),
        },
        {
          name: "skill:architect",
          description: "Sketch types before code.",
          source: "skill",
          sourceInfo: sourceInfo("/Users/me/.agents/skills/architect/SKILL.md", "user"),
        },
        {
          name: "skill:contribution-triage",
          description: "Enforce the PR policy.",
          source: "skill",
          sourceInfo: sourceInfo("/repo/.agents/skills/contribution-triage/SKILL.md", "project"),
        },
      ],
    }),
  ).toEqual({
    slashCommands: [
      { name: "ant-account", description: "Show or switch the Anthropic subscription account" },
    ],
    skills: [
      {
        name: "architect",
        description: "Sketch types before code.",
        path: "/Users/me/.agents/skills/architect/SKILL.md",
        scope: "user",
        enabled: true,
      },
      {
        name: "contribution-triage",
        description: "Enforce the PR policy.",
        path: "/repo/.agents/skills/contribution-triage/SKILL.md",
        scope: "project",
        enabled: true,
      },
    ],
  });
});

it("maps Pi global location and interface labels onto T3 skill fields", () => {
  expect(
    parsePiDiscoveredCommands({
      commands: [
        {
          name: "skill:global-review",
          description: "Review changes.",
          source: "skill",
          location: "global",
          interface: {
            displayName: "Global Review",
            shortDescription: "Review diffs.",
          },
        },
      ],
    }),
  ).toEqual({
    skills: [
      {
        name: "global-review",
        description: "Review changes.",
        path: "pi:skill:global-review",
        scope: "user",
        enabled: true,
        displayName: "Global Review",
        shortDescription: "Review diffs.",
      },
    ],
    slashCommands: [],
  });
});

it("parses a standalone /compact command and optional instructions", () => {
  expect(parsePiCompactCommand("/compact")).toEqual({});
  expect(parsePiCompactCommand("  /compact  ")).toEqual({});
  expect(parsePiCompactCommand("/compact keep the auth rewrite")).toEqual({
    customInstructions: "keep the auth rewrite",
  });
  expect(parsePiCompactCommand("/compacted")).toBeNull();
  expect(parsePiCompactCommand("/compact-now")).toBeNull();
  expect(parsePiCompactCommand("please /compact")).toBeNull();
});

it("prepends the builtin compact command without duplicating a discovered one", () => {
  expect(withPiBuiltinSlashCommands([{ name: "hello", description: "Say hello." }])).toEqual([
    PI_COMPACT_SLASH_COMMAND,
    { name: "hello", description: "Say hello." },
  ]);
  expect(
    withPiBuiltinSlashCommands([
      { name: "compact", description: "Extension compact." },
      { name: "hello" },
    ]),
  ).toEqual([PI_COMPACT_SLASH_COMMAND, { name: "hello" }]);
});

it("leaves unrelated dollar-prefixed text unchanged", () => {
  expect(expandPiSkillReference("Explain $HOME", new Set(["global-review"]))).toBe("Explain $HOME");
});

it("hoists every known $ skill and keeps the rest of the prompt", () => {
  expect(expandPiSkillReference("use $alpha then $beta please", new Set(["alpha", "beta"]))).toBe(
    "/skill:alpha /skill:beta use  then  please",
  );
});

it("preserves code indentation and line breaks when expanding a skill", () => {
  expect(expandPiSkillReference("$review\n```ts\n  const x = 1;\n```", new Set(["review"]))).toBe(
    "/skill:review ```ts\n  const x = 1;\n```",
  );
});

it("keeps T3's internal rollback command out of the slash menu", () => {
  const data = {
    commands: [
      { name: "t3-navigate-tree", source: "extension" },
      { name: "review", source: "extension" },
    ],
  };
  expect(parsePiDiscoveredCommands(data).slashCommands.map((command) => command.name)).toEqual([
    "review",
  ]);
  expect(hasPiNavigateTreeCommand(data)).toBe(true);
  expect(hasPiNavigateTreeCommand({ commands: [{ name: "review" }] })).toBe(false);
});
