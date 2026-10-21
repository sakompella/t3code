import { expect, it } from "@effect/vitest";

import { hasPiNavigateTreeCommand, parsePiDiscoveredCommands } from "./commands.ts";

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
