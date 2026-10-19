/**
 * Records from the Prime Agent 0.10 fork's `--mode rpc` stdout
 * (`sakompella/prime-agent` fork/main 0f208bf35, slice-2 live smoke on
 * agentbox, 2026-10-11). A scripted parent spawned one haiku child (`adder`),
 * which answered `42` with `agent_message.send`; that message woke the
 * parent. Correlated responses are left out, since the fake answers requests
 * itself, and long strings are clipped.
 */

/** Stdout records in arrival order, from the parent's first `agent_start` on. */
export const PRIME_AGENT_FORK_CHILD_WAKE_RECORDS: ReadonlyArray<Record<string, unknown>> = [
  { type: "agent_start" },
  { type: "turn_start" },
  {
    type: "message_start",
    message: {
      role: "user",
      content: [
        {
          type: "text",
          text: "Call the ipython tool exactly once with exactly this code, unchanged. Then reply with exactly 'CHILD ANSWER: <the child'…",
        },
      ],
      timestamp: 1791710911618,
    },
  },
  {
    type: "message_end",
    message: {
      role: "user",
      content: [
        {
          type: "text",
          text: "Call the ipython tool exactly once with exactly this code, unchanged. Then reply with exactly 'CHILD ANSWER: <the child'…",
        },
      ],
      timestamp: 1791710911618,
    },
  },
  {
    type: "message_start",
    message: {
      role: "assistant",
      content: [],
      api: "faux",
      provider: "faux",
      model: "faux-1",
      diagnostics: [],
      usage: {
        input: 14119,
        output: 63,
        cacheRead: 0,
        cacheWrite: 0,
        totalTokens: 14182,
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
      },
      stopReason: "toolUse",
      timestamp: 1791710911624,
    },
    assistantMessageEvent: { type: "start" },
  },
  {
    type: "message_update",
    message: {
      role: "assistant",
      content: [
        { type: "toolCall", id: "tool:1791710911202:1992bb45", name: "ipython", arguments: {} },
      ],
      api: "faux",
      provider: "faux",
      model: "faux-1",
      diagnostics: [],
      usage: {
        input: 14119,
        output: 63,
        cacheRead: 0,
        cacheWrite: 0,
        totalTokens: 14182,
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
      },
      stopReason: "toolUse",
      timestamp: 1791710911624,
    },
    assistantMessageEvent: {
      type: "toolcall_delta",
      delta:
        "{\"code\":\"h = await rlm.spawn('What is 17 + 25? Reply with only the number.', name='adder', model='cpa-claude/claude-haiku-5-5', thinking='low')\\nres = await rlm.collect(h, timeout_ms=180000)\\nprint(res[0].status, repr(res[0].answer_preview))\"}",
    },
  },
  {
    type: "message_update",
    message: {
      role: "assistant",
      content: [
        {
          type: "toolCall",
          id: "tool:1791710911202:1992bb45",
          name: "ipython",
          arguments: {
            code: "h = await rlm.spawn('What is 17 + 25? Reply with only the number.', name='adder', model='cpa-claude/claude-haiku-5-5', thinking='low')\nres = await rlm.collect(h, timeout_ms=180000)\nprint(res[0].status, repr(res[0].answer_preview))",
          },
        },
      ],
      api: "faux",
      provider: "faux",
      model: "faux-1",
      diagnostics: [],
      usage: {
        input: 14119,
        output: 63,
        cacheRead: 0,
        cacheWrite: 0,
        totalTokens: 14182,
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
      },
      stopReason: "toolUse",
      timestamp: 1791710911624,
    },
    assistantMessageEvent: { type: "toolcall_end" },
  },
  {
    type: "message_end",
    message: {
      role: "assistant",
      content: [
        {
          type: "toolCall",
          id: "tool:1791710911202:1992bb45",
          name: "ipython",
          arguments: {
            code: "h = await rlm.spawn('What is 17 + 25? Reply with only the number.', name='adder', model='cpa-claude/claude-haiku-5-5', thinking='low')\nres = await rlm.collect(h, timeout_ms=180000)\nprint(res[0].status, repr(res[0].answer_preview))",
          },
        },
      ],
      api: "faux",
      provider: "faux",
      model: "faux-1",
      diagnostics: [],
      usage: {
        input: 14119,
        output: 63,
        cacheRead: 0,
        cacheWrite: 0,
        totalTokens: 14182,
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
      },
      stopReason: "toolUse",
      timestamp: 1791710911624,
    },
  },
  {
    type: "tool_execution_start",
    toolCallId: "tool:1791710911202:1992bb45",
    toolName: "ipython",
    args: {
      code: "h = await rlm.spawn('What is 17 + 25? Reply with only the number.', name='adder', model='cpa-claude/claude-haiku-5-5', thinking='low')\nres = await rlm.collect(h, timeout_ms=180000)\nprint(res[0].status, repr(res[0].answer_preview))",
    },
  },
  {
    type: "tool_execution_update",
    toolCallId: "tool:1791710911202:1992bb45",
    partialResult: {
      content: [{ type: "text", text: "Starting Python kernel..." }],
      details: { status: "starting" },
    },
  },
  {
    type: "tool_execution_update",
    toolCallId: "tool:1791710911202:1992bb45",
    partialResult: {
      content: [{ type: "text", text: "Restoring Python state..." }],
      details: { status: "starting" },
    },
  },
  {
    type: "tool_execution_update",
    toolCallId: "tool:1791710911202:1992bb45",
    partialResult: {
      content: [{ type: "text", text: "Preparing Python runtime..." }],
      details: { status: "starting" },
    },
  },
  {
    type: "rlm_child_update",
    child: {
      id: "sub-1fae0326",
      activeSessionId: "f37cc57f87da",
      sessionName: "adder",
      label: "What is 17 + 25? Reply with only the number.",
      status: "running",
      durationMs: 0,
      sessionDir:
        "/home/aditya/scratch/prime-fork-slice2/smoke-live2/home/.prime/agent/session-artifacts/01a12a4a-dae3-763e-a443-b8b159822c79/sub-1fae0326",
      model: "cpa-claude/claude-haiku-5-5",
    },
  },
  {
    type: "tool_execution_end",
    toolCallId: "tool:1791710911202:1992bb45",
    result: {
      content: [{ type: "text", text: "running None\n" }],
      details: { status: "ok", kernelRestarted: false, durationMs: 190, stdout: "running None\n" },
    },
    isError: false,
  },
  {
    type: "message_start",
    message: {
      role: "toolResult",
      toolCallId: "tool:1791710911202:1992bb45",
      toolName: "ipython",
      content: [{ text: "running None\n", type: "text" }],
      details: { status: "ok", kernelRestarted: false, durationMs: 190, stdout: "running None\n" },
      isError: false,
      timestamp: 1791710949003,
    },
  },
  {
    type: "message_end",
    message: {
      role: "toolResult",
      toolCallId: "tool:1791710911202:1992bb45",
      toolName: "ipython",
      content: [{ text: "running None\n", type: "text" }],
      details: { status: "ok", kernelRestarted: false, durationMs: 190, stdout: "running None\n" },
      isError: false,
      timestamp: 1791710949003,
    },
  },
  {
    type: "turn_end",
    message: {
      role: "assistant",
      content: [
        {
          type: "toolCall",
          id: "tool:1791710911202:1992bb45",
          name: "ipython",
          arguments: {
            code: "h = await rlm.spawn('What is 17 + 25? Reply with only the number.', name='adder', model='cpa-claude/claude-haiku-5-5', thinking='low')\nres = await rlm.collect(h, timeout_ms=180000)\nprint(res[0].status, repr(res[0].answer_preview))",
          },
        },
      ],
      api: "faux",
      provider: "faux",
      model: "faux-1",
      diagnostics: [],
      usage: {
        input: 14119,
        output: 63,
        cacheRead: 0,
        cacheWrite: 0,
        totalTokens: 14182,
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
      },
      stopReason: "toolUse",
      timestamp: 1791710911624,
    },
    toolResults: [
      {
        role: "toolResult",
        toolCallId: "tool:1791710911202:1992bb45",
        toolName: "ipython",
        content: [{ text: "running None\n", type: "text" }],
        details: {
          status: "ok",
          kernelRestarted: false,
          durationMs: 190,
          stdout: "running None\n",
        },
        isError: false,
        timestamp: 1791710949003,
      },
    ],
  },
  { type: "turn_start" },
  {
    type: "message_start",
    message: {
      role: "assistant",
      content: [],
      api: "faux",
      provider: "faux",
      model: "faux-1",
      diagnostics: [],
      usage: {
        input: 14193,
        output: 9,
        cacheRead: 0,
        cacheWrite: 0,
        totalTokens: 14202,
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
      },
      stopReason: "stop",
      timestamp: 1791710949007,
    },
    assistantMessageEvent: { type: "start" },
  },
  {
    type: "message_update",
    message: {
      role: "assistant",
      content: [{ type: "text", text: "CHILD ANSWER: see the tool result" }],
      api: "faux",
      provider: "faux",
      model: "faux-1",
      diagnostics: [],
      usage: {
        input: 14193,
        output: 9,
        cacheRead: 0,
        cacheWrite: 0,
        totalTokens: 14202,
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
      },
      stopReason: "stop",
      timestamp: 1791710949007,
    },
    assistantMessageEvent: { type: "text_delta", delta: "CHILD ANSWER: see the tool result" },
  },
  {
    type: "message_update",
    message: {
      role: "assistant",
      content: [{ type: "text", text: "CHILD ANSWER: see the tool result" }],
      api: "faux",
      provider: "faux",
      model: "faux-1",
      diagnostics: [],
      usage: {
        input: 14193,
        output: 9,
        cacheRead: 0,
        cacheWrite: 0,
        totalTokens: 14202,
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
      },
      stopReason: "stop",
      timestamp: 1791710949007,
    },
    assistantMessageEvent: { type: "text_end" },
  },
  {
    type: "message_end",
    message: {
      role: "assistant",
      content: [{ type: "text", text: "CHILD ANSWER: see the tool result" }],
      api: "faux",
      provider: "faux",
      model: "faux-1",
      diagnostics: [],
      usage: {
        input: 14193,
        output: 9,
        cacheRead: 0,
        cacheWrite: 0,
        totalTokens: 14202,
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
      },
      stopReason: "stop",
      timestamp: 1791710949007,
    },
  },
  {
    type: "turn_end",
    message: {
      role: "assistant",
      content: [{ type: "text", text: "CHILD ANSWER: see the tool result" }],
      api: "faux",
      provider: "faux",
      model: "faux-1",
      diagnostics: [],
      usage: {
        input: 14193,
        output: 9,
        cacheRead: 0,
        cacheWrite: 0,
        totalTokens: 14202,
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
      },
      stopReason: "stop",
      timestamp: 1791710949007,
    },
    toolResults: [],
  },
  {
    type: "agent_end",
    messages: [
      {
        customType: "harness_digest",
        content:
          "[harness-digest]\n\nThe persistent memories produced across this session so far:\n\n<harness_state>\n# Continual Harness Stat…",
        display: false,
        details: {
          digest:
            "# Continual Harness State\n\nLocal continual harness entries belong to this Prime Agent session. Global continual harness …",
          stateFingerprint: "b77e4293d649db19d47120215434b760c8bd519e8f6267d96c280d2d547884bc",
        },
        timestamp: 1791710911620,
        role: "custom",
      },
      {
        role: "user",
        content: [
          {
            text: "Call the ipython tool exactly once with exactly this code, unchanged. Then reply with exactly 'CHILD ANSWER: <the child'…",
            type: "text",
          },
        ],
        timestamp: 1791710911620,
      },
      {
        role: "assistant",
        content: [
          {
            type: "toolCall",
            id: "tool:1791710911202:1992bb45",
            name: "ipython",
            arguments: {
              code: "h = await rlm.spawn('What is 17 + 25? Reply with only the number.', name='adder', model='cpa-claude/claude-haiku-5-5', thinking='low')\nres = await rlm.collect(h, timeout_ms=180000)\nprint(res[0].status, repr(res[0].answer_preview))",
            },
          },
        ],
        api: "faux",
        provider: "faux",
        model: "faux-1",
        diagnostics: [],
        usage: {
          input: 14119,
          output: 63,
          cacheRead: 0,
          cacheWrite: 0,
          totalTokens: 14182,
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
        },
        stopReason: "toolUse",
        timestamp: 1791710911624,
      },
      {
        role: "toolResult",
        toolCallId: "tool:1791710911202:1992bb45",
        toolName: "ipython",
        content: [{ text: "running None\n", type: "text" }],
        details: {
          status: "ok",
          kernelRestarted: false,
          durationMs: 190,
          stdout: "running None\n",
        },
        isError: false,
        timestamp: 1791710949003,
      },
      {
        role: "assistant",
        content: [{ type: "text", text: "CHILD ANSWER: see the tool result" }],
        api: "faux",
        provider: "faux",
        model: "faux-1",
        diagnostics: [],
        usage: {
          input: 14193,
          output: 9,
          cacheRead: 0,
          cacheWrite: 0,
          totalTokens: 14202,
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
        },
        stopReason: "stop",
        timestamp: 1791710949007,
      },
    ],
  },
  {
    type: "observed_session_event",
    activeSessionId: "f37cc57f87da",
    event: { type: "agent_start" },
  },
  {
    type: "observed_session_event",
    activeSessionId: "f37cc57f87da",
    event: { type: "turn_start" },
  },
  {
    type: "observed_session_event",
    activeSessionId: "f37cc57f87da",
    event: {
      type: "message_start",
      message: {
        role: "custom",
        customType: "agent_message",
        content: "[task from parent]\n\nWhat is 17 + 25? Reply with only the number.",
        display: true,
        details: {
          id: "spawn:sub-1fae0326",
          message: "What is 17 + 25? Reply with only the number.",
          from: {
            activeSessionId: "2a1193474648",
            sessionId: "01a12a4a-dae3-763e-a443-b8b159822c79",
          },
          fromRelationship: "parent",
        },
        timestamp: 1791710948998,
      },
    },
  },
  {
    type: "observed_session_event",
    activeSessionId: "f37cc57f87da",
    event: {
      type: "message_end",
      message: {
        role: "custom",
        customType: "agent_message",
        content: "[task from parent]\n\nWhat is 17 + 25? Reply with only the number.",
        display: true,
        details: {
          id: "spawn:sub-1fae0326",
          message: "What is 17 + 25? Reply with only the number.",
          from: {
            activeSessionId: "2a1193474648",
            sessionId: "01a12a4a-dae3-763e-a443-b8b159822c79",
          },
          fromRelationship: "parent",
        },
        timestamp: 1791710948998,
      },
    },
  },
  {
    type: "observed_session_event",
    activeSessionId: "f37cc57f87da",
    event: {
      type: "message_start",
      message: {
        role: "assistant",
        content: [],
        api: "anthropic-messages",
        provider: "cpa-claude",
        model: "claude-haiku-5-5",
        usage: {
          input: 0,
          output: 0,
          cacheRead: 0,
          cacheWrite: 0,
          totalTokens: 0,
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
        },
        stopReason: "stop",
        timestamp: 1791710949042,
      },
      assistantMessageEvent: { type: "start" },
    },
  },
  {
    type: "observed_session_event",
    activeSessionId: "f37cc57f87da",
    event: {
      type: "message_update",
      message: {
        role: "assistant",
        content: [{ type: "thinking", thinking: "", thinkingSignature: "" }],
        api: "anthropic-messages",
        provider: "cpa-claude",
        model: "claude-haiku-5-5",
        responseId: "msg_011CfvDNe8iVv6j15amFpddP",
        usage: {
          input: 0,
          output: 0,
          cacheRead: 0,
          cacheWrite: 0,
          totalTokens: 0,
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
        },
        stopReason: "stop",
        timestamp: 1791710949042,
      },
      assistantMessageEvent: { type: "thinking_start" },
    },
  },
  {
    type: "observed_session_event",
    activeSessionId: "f37cc57f87da",
    event: {
      type: "message_update",
      message: {
        role: "assistant",
        content: [
          {
            type: "thinking",
            thinking:
              '17+25 is 42. Since the instructions say to reply with only the number, I\'ll send "42" to the parent via agent_message.send.\n\n',
            thinkingSignature: "",
          },
        ],
        api: "anthropic-messages",
        provider: "cpa-claude",
        model: "claude-haiku-5-5",
        responseId: "msg_011CfvDNe8iVv6j15amFpddP",
        usage: {
          input: 0,
          output: 0,
          cacheRead: 0,
          cacheWrite: 0,
          totalTokens: 0,
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
        },
        stopReason: "stop",
        timestamp: 1791710949042,
      },
      assistantMessageEvent: {
        type: "thinking_delta",
        delta:
          '17+25 is 42. Since the instructions say to reply with only the number, I\'ll send "42" to the parent via agent_message.send.\n\n',
      },
    },
  },
  {
    type: "observed_session_event",
    activeSessionId: "f37cc57f87da",
    event: {
      type: "message_update",
      message: {
        role: "assistant",
        content: [
          {
            type: "thinking",
            thinking:
              '17+25 is 42. Since the instructions say to reply with only the number, I\'ll send "42" to the parent via agent_message.send.\n\n',
            thinkingSignature: "",
          },
        ],
        api: "anthropic-messages",
        provider: "cpa-claude",
        model: "claude-haiku-5-5",
        responseId: "msg_011CfvDNe8iVv6j15amFpddP",
        usage: {
          input: 0,
          output: 0,
          cacheRead: 0,
          cacheWrite: 0,
          totalTokens: 0,
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
        },
        stopReason: "stop",
        timestamp: 1791710949042,
      },
      assistantMessageEvent: { type: "thinking_end" },
    },
  },
  {
    type: "observed_session_event",
    activeSessionId: "f37cc57f87da",
    event: {
      type: "message_update",
      message: {
        role: "assistant",
        content: [
          {
            type: "thinking",
            thinking:
              '17+25 is 42. Since the instructions say to reply with only the number, I\'ll send "42" to the parent via agent_message.send.\n\n',
            thinkingSignature: "",
          },
          {
            type: "toolCall",
            id: "toolu_01KgCEZ4TZmRfKooJtK87BTL",
            name: "ipython",
            arguments: { code: 'await agent_message.send("42", receiver_role="parent")' },
          },
        ],
        api: "anthropic-messages",
        provider: "cpa-claude",
        model: "claude-haiku-5-5",
        responseId: "msg_011CfvDNe8iVv6j15amFpddP",
        usage: {
          input: 0,
          output: 0,
          cacheRead: 0,
          cacheWrite: 0,
          totalTokens: 0,
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
        },
        stopReason: "stop",
        timestamp: 1791710949042,
      },
      assistantMessageEvent: {
        type: "toolcall_delta",
        delta: '{"code": "await agent_message.send(\\"42\\", receiver_role=\\"parent\\")"}',
      },
    },
  },
  {
    type: "observed_session_event",
    activeSessionId: "f37cc57f87da",
    event: {
      type: "message_update",
      message: {
        role: "assistant",
        content: [
          {
            type: "thinking",
            thinking:
              '17+25 is 42. Since the instructions say to reply with only the number, I\'ll send "42" to the parent via agent_message.send.\n\n',
            thinkingSignature: "",
          },
          {
            type: "toolCall",
            id: "toolu_01KgCEZ4TZmRfKooJtK87BTL",
            name: "ipython",
            arguments: { code: 'await agent_message.send("42", receiver_role="parent")' },
          },
        ],
        api: "anthropic-messages",
        provider: "cpa-claude",
        model: "claude-haiku-5-5",
        responseId: "msg_011CfvDNe8iVv6j15amFpddP",
        usage: {
          input: 0,
          output: 0,
          cacheRead: 0,
          cacheWrite: 0,
          totalTokens: 0,
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
        },
        stopReason: "stop",
        timestamp: 1791710949042,
      },
      assistantMessageEvent: { type: "toolcall_end" },
    },
  },
  {
    type: "observed_session_event",
    activeSessionId: "f37cc57f87da",
    event: {
      type: "message_end",
      message: {
        role: "assistant",
        content: [
          {
            type: "thinking",
            thinking:
              '17+25 is 42. Since the instructions say to reply with only the number, I\'ll send "42" to the parent via agent_message.send.\n\n',
            thinkingSignature: "",
          },
          {
            type: "toolCall",
            id: "toolu_01KgCEZ4TZmRfKooJtK87BTL",
            name: "ipython",
            arguments: { code: 'await agent_message.send("42", receiver_role="parent")' },
          },
        ],
        api: "anthropic-messages",
        provider: "cpa-claude",
        model: "claude-haiku-5-5",
        responseId: "msg_011CfvDNe8iVv6j15amFpddP",
        usage: {
          input: 2,
          output: 171,
          cacheRead: 861,
          cacheWrite: 20834,
          totalTokens: 21868,
          cost: {
            input: 2.0000000000000002e-7,
            output: 8.549999999999999e-5,
            cacheRead: 8.61e-6,
            cacheWrite: 0.00260425,
            total: 0.0026985599999999996,
          },
        },
        stopReason: "toolUse",
        timestamp: 1791710949042,
      },
    },
  },
  {
    type: "observed_session_event",
    activeSessionId: "f37cc57f87da",
    event: {
      type: "tool_execution_start",
      toolCallId: "toolu_01KgCEZ4TZmRfKooJtK87BTL",
      toolName: "ipython",
      args: { code: 'await agent_message.send("42", receiver_role="parent")' },
    },
  },
  {
    type: "observed_session_event",
    activeSessionId: "f37cc57f87da",
    event: {
      type: "tool_execution_update",
      toolCallId: "toolu_01KgCEZ4TZmRfKooJtK87BTL",
      partialResult: {
        content: [{ type: "text", text: "Starting Python kernel..." }],
        details: { status: "starting" },
      },
    },
  },
  {
    type: "observed_session_event",
    activeSessionId: "f37cc57f87da",
    event: {
      type: "tool_execution_update",
      toolCallId: "toolu_01KgCEZ4TZmRfKooJtK87BTL",
      partialResult: {
        content: [{ type: "text", text: "Starting Python kernel..." }],
        details: { status: "starting" },
      },
    },
  },
  {
    type: "observed_session_event",
    activeSessionId: "f37cc57f87da",
    event: {
      type: "tool_execution_update",
      toolCallId: "toolu_01KgCEZ4TZmRfKooJtK87BTL",
      partialResult: {
        content: [{ type: "text", text: "Restoring Python state..." }],
        details: { status: "starting" },
      },
    },
  },
  {
    type: "observed_session_event",
    activeSessionId: "f37cc57f87da",
    event: {
      type: "tool_execution_update",
      toolCallId: "toolu_01KgCEZ4TZmRfKooJtK87BTL",
      partialResult: {
        content: [{ type: "text", text: "Restoring Python state..." }],
        details: { status: "starting" },
      },
    },
  },
  {
    type: "observed_session_event",
    activeSessionId: "f37cc57f87da",
    event: {
      type: "tool_execution_update",
      toolCallId: "toolu_01KgCEZ4TZmRfKooJtK87BTL",
      partialResult: {
        content: [{ type: "text", text: "Preparing Python runtime..." }],
        details: { status: "starting" },
      },
    },
  },
  {
    type: "observed_session_event",
    activeSessionId: "f37cc57f87da",
    event: {
      type: "tool_execution_update",
      toolCallId: "toolu_01KgCEZ4TZmRfKooJtK87BTL",
      partialResult: {
        content: [{ type: "text", text: "Preparing Python runtime..." }],
        details: { status: "starting" },
      },
    },
  },
  {
    type: "session_action_update",
    actions: { queuedCount: 1, steering: ["Agent message received: 42"], followUps: [] },
  },
  {
    type: "session_action_update",
    actions: {
      queuedCount: 0,
      steering: [],
      followUps: [],
      active: { kind: "turn", phase: "preparing", label: "Agent message received: 42" },
    },
  },
  { type: "agent_start" },
  { type: "turn_start" },
  {
    type: "message_start",
    message: {
      role: "custom",
      customType: "agent_message",
      content: "[agent-message from child:adder]\n\n42",
      display: true,
      details: {
        id: "agentmsg_e802480f-da2b-4a4e-a016-a5f301f67fc7",
        message: "42",
        from: {
          activeSessionId: "f37cc57f87da",
          runtimeKind: "subagent",
          clientId: "agent",
          sessionId: "01a12a4b-6e52-71db-8119-4d155cfa6678",
          sessionName: "adder",
          parentActiveSessionId: "2a1193474648",
          parentSessionId: "01a12a4a-dae3-763e-a443-b8b159822c79",
          parentSessionPath:
            "/home/aditya/scratch/prime-fork-slice2/smoke-live2/home/.prime/agent/sessions/01a12a4a-dae3-763e-a443-b8b159822c79.jsonl",
        },
        fromRelationship: "child",
        target: {
          activeSessionId: "2a1193474648",
          sessionId: "01a12a4a-dae3-763e-a443-b8b159822c79",
          runtimeKind: "top-level",
        },
      },
      timestamp: 1791710952850,
    },
  },
  {
    type: "message_end",
    message: {
      role: "custom",
      customType: "agent_message",
      content: "[agent-message from child:adder]\n\n42",
      display: true,
      details: {
        id: "agentmsg_e802480f-da2b-4a4e-a016-a5f301f67fc7",
        message: "42",
        from: {
          activeSessionId: "f37cc57f87da",
          runtimeKind: "subagent",
          clientId: "agent",
          sessionId: "01a12a4b-6e52-71db-8119-4d155cfa6678",
          sessionName: "adder",
          parentActiveSessionId: "2a1193474648",
          parentSessionId: "01a12a4a-dae3-763e-a443-b8b159822c79",
          parentSessionPath:
            "/home/aditya/scratch/prime-fork-slice2/smoke-live2/home/.prime/agent/sessions/01a12a4a-dae3-763e-a443-b8b159822c79.jsonl",
        },
        fromRelationship: "child",
        target: {
          activeSessionId: "2a1193474648",
          sessionId: "01a12a4a-dae3-763e-a443-b8b159822c79",
          runtimeKind: "top-level",
        },
      },
      timestamp: 1791710952850,
    },
  },
  {
    type: "session_action_update",
    actions: {
      queuedCount: 0,
      steering: [],
      followUps: [],
      active: { kind: "turn", phase: "committing", label: "Agent message received: 42" },
    },
  },
  {
    type: "message_start",
    message: {
      role: "assistant",
      content: [],
      api: "faux",
      provider: "faux",
      model: "faux-1",
      diagnostics: [],
      usage: {
        input: 14215,
        output: 9,
        cacheRead: 0,
        cacheWrite: 0,
        totalTokens: 14224,
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
      },
      stopReason: "stop",
      timestamp: 1791710952859,
    },
    assistantMessageEvent: { type: "start" },
  },
  {
    type: "session_action_update",
    actions: {
      queuedCount: 0,
      steering: [],
      followUps: [],
      active: { kind: "turn", phase: "running", label: "Agent message received: 42" },
    },
  },
  {
    type: "message_update",
    message: {
      role: "assistant",
      content: [{ type: "text", text: "CHILD ANSWER: see the tool result" }],
      api: "faux",
      provider: "faux",
      model: "faux-1",
      diagnostics: [],
      usage: {
        input: 14215,
        output: 9,
        cacheRead: 0,
        cacheWrite: 0,
        totalTokens: 14224,
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
      },
      stopReason: "stop",
      timestamp: 1791710952859,
    },
    assistantMessageEvent: { type: "text_delta", delta: "CHILD ANSWER: see the tool result" },
  },
  {
    type: "message_update",
    message: {
      role: "assistant",
      content: [{ type: "text", text: "CHILD ANSWER: see the tool result" }],
      api: "faux",
      provider: "faux",
      model: "faux-1",
      diagnostics: [],
      usage: {
        input: 14215,
        output: 9,
        cacheRead: 0,
        cacheWrite: 0,
        totalTokens: 14224,
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
      },
      stopReason: "stop",
      timestamp: 1791710952859,
    },
    assistantMessageEvent: { type: "text_end" },
  },
  {
    type: "message_end",
    message: {
      role: "assistant",
      content: [{ type: "text", text: "CHILD ANSWER: see the tool result" }],
      api: "faux",
      provider: "faux",
      model: "faux-1",
      diagnostics: [],
      usage: {
        input: 14215,
        output: 9,
        cacheRead: 0,
        cacheWrite: 0,
        totalTokens: 14224,
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
      },
      stopReason: "stop",
      timestamp: 1791710952859,
    },
  },
  {
    type: "turn_end",
    message: {
      role: "assistant",
      content: [{ type: "text", text: "CHILD ANSWER: see the tool result" }],
      api: "faux",
      provider: "faux",
      model: "faux-1",
      diagnostics: [],
      usage: {
        input: 14215,
        output: 9,
        cacheRead: 0,
        cacheWrite: 0,
        totalTokens: 14224,
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
      },
      stopReason: "stop",
      timestamp: 1791710952859,
    },
    toolResults: [],
  },
  {
    type: "agent_end",
    messages: [
      {
        customType: "agent_message",
        content: "[agent-message from child:adder]\n\n42",
        display: true,
        details: {
          id: "agentmsg_e802480f-da2b-4a4e-a016-a5f301f67fc7",
          message: "42",
          from: {
            activeSessionId: "f37cc57f87da",
            runtimeKind: "subagent",
            clientId: "agent",
            sessionId: "01a12a4b-6e52-71db-8119-4d155cfa6678",
            sessionName: "adder",
            parentActiveSessionId: "2a1193474648",
            parentSessionId: "01a12a4a-dae3-763e-a443-b8b159822c79",
            parentSessionPath:
              "/home/aditya/scratch/prime-fork-slice2/smoke-live2/home/.prime/agent/sessions/01a12a4a-dae3-763e-a443-b8b159822c79.jsonl",
          },
          fromRelationship: "child",
          target: {
            activeSessionId: "2a1193474648",
            sessionId: "01a12a4a-dae3-763e-a443-b8b159822c79",
            runtimeKind: "top-level",
          },
        },
        timestamp: 1791710952850,
        role: "custom",
      },
      {
        role: "assistant",
        content: [{ type: "text", text: "CHILD ANSWER: see the tool result" }],
        api: "faux",
        provider: "faux",
        model: "faux-1",
        diagnostics: [],
        usage: {
          input: 14215,
          output: 9,
          cacheRead: 0,
          cacheWrite: 0,
          totalTokens: 14224,
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
        },
        stopReason: "stop",
        timestamp: 1791710952859,
      },
    ],
  },
  {
    type: "observed_session_event",
    activeSessionId: "f37cc57f87da",
    event: {
      type: "tool_execution_end",
      toolCallId: "toolu_01KgCEZ4TZmRfKooJtK87BTL",
      result: {
        content: [
          {
            type: "text",
            text: "{'id': 'agentmsg_e802480f-da2b-4a4e-a016-a5f301f67fc7', 'source': 'agent_message', 'target': {'activeSessionId': '2a1193…",
          },
        ],
        details: {
          status: "ok",
          kernelRestarted: false,
          durationMs: 7,
          result:
            "{'id': 'agentmsg_e802480f-da2b-4a4e-a016-a5f301f67fc7', 'source': 'agent_message', 'target': {'activeSessionId': '2a1193…",
          sentAgentMessages: [
            {
              id: "agentmsg_e802480f-da2b-4a4e-a016-a5f301f67fc7",
              message: "42",
              deliveryStatus: "delivered",
              target: {
                activeSessionId: "2a1193474648",
                sessionId: "01a12a4a-dae3-763e-a443-b8b159822c79",
              },
              receiverRole: "parent",
            },
          ],
        },
      },
      isError: false,
    },
  },
  {
    type: "observed_session_event",
    activeSessionId: "f37cc57f87da",
    event: {
      type: "message_start",
      message: {
        role: "toolResult",
        toolCallId: "toolu_01KgCEZ4TZmRfKooJtK87BTL",
        toolName: "ipython",
        content: [
          {
            text: "{'id': 'agentmsg_e802480f-da2b-4a4e-a016-a5f301f67fc7', 'source': 'agent_message', 'target': {'activeSessionId': '2a1193…",
            type: "text",
          },
        ],
        details: {
          status: "ok",
          kernelRestarted: false,
          durationMs: 7,
          result:
            "{'id': 'agentmsg_e802480f-da2b-4a4e-a016-a5f301f67fc7', 'source': 'agent_message', 'target': {'activeSessionId': '2a1193…",
          sentAgentMessages: [
            {
              id: "agentmsg_e802480f-da2b-4a4e-a016-a5f301f67fc7",
              message: "42",
              deliveryStatus: "delivered",
              target: {
                activeSessionId: "2a1193474648",
                sessionId: "01a12a4a-dae3-763e-a443-b8b159822c79",
              },
              receiverRole: "parent",
            },
          ],
        },
        isError: false,
        timestamp: 1791710952858,
      },
    },
  },
  {
    type: "observed_session_event",
    activeSessionId: "f37cc57f87da",
    event: {
      type: "message_end",
      message: {
        role: "toolResult",
        toolCallId: "toolu_01KgCEZ4TZmRfKooJtK87BTL",
        toolName: "ipython",
        content: [
          {
            text: "{'id': 'agentmsg_e802480f-da2b-4a4e-a016-a5f301f67fc7', 'source': 'agent_message', 'target': {'activeSessionId': '2a1193…",
            type: "text",
          },
        ],
        details: {
          status: "ok",
          kernelRestarted: false,
          durationMs: 7,
          result:
            "{'id': 'agentmsg_e802480f-da2b-4a4e-a016-a5f301f67fc7', 'source': 'agent_message', 'target': {'activeSessionId': '2a1193…",
          sentAgentMessages: [
            {
              id: "agentmsg_e802480f-da2b-4a4e-a016-a5f301f67fc7",
              message: "42",
              deliveryStatus: "delivered",
              target: {
                activeSessionId: "2a1193474648",
                sessionId: "01a12a4a-dae3-763e-a443-b8b159822c79",
              },
              receiverRole: "parent",
            },
          ],
        },
        isError: false,
        timestamp: 1791710952858,
      },
    },
  },
  {
    type: "observed_session_event",
    activeSessionId: "f37cc57f87da",
    event: {
      type: "turn_end",
      message: {
        role: "assistant",
        content: [
          {
            type: "thinking",
            thinking:
              '17+25 is 42. Since the instructions say to reply with only the number, I\'ll send "42" to the parent via agent_message.send.\n\n',
            thinkingSignature: "",
          },
          {
            type: "toolCall",
            id: "toolu_01KgCEZ4TZmRfKooJtK87BTL",
            name: "ipython",
            arguments: { code: 'await agent_message.send("42", receiver_role="parent")' },
          },
        ],
        api: "anthropic-messages",
        provider: "cpa-claude",
        model: "claude-haiku-5-5",
        responseId: "msg_011CfvDNe8iVv6j15amFpddP",
        usage: {
          input: 2,
          output: 171,
          cacheRead: 861,
          cacheWrite: 20834,
          totalTokens: 21868,
          cost: {
            input: 2.0000000000000002e-7,
            output: 8.549999999999999e-5,
            cacheRead: 8.61e-6,
            cacheWrite: 0.00260425,
            total: 0.0026985599999999996,
          },
        },
        stopReason: "toolUse",
        timestamp: 1791710949042,
      },
      toolResults: [
        {
          role: "toolResult",
          toolCallId: "toolu_01KgCEZ4TZmRfKooJtK87BTL",
          toolName: "ipython",
          content: [
            {
              text: "{'id': 'agentmsg_e802480f-da2b-4a4e-a016-a5f301f67fc7', 'source': 'agent_message', 'target': {'activeSessionId': '2a1193…",
              type: "text",
            },
          ],
          details: {
            status: "ok",
            kernelRestarted: false,
            durationMs: 7,
            result:
              "{'id': 'agentmsg_e802480f-da2b-4a4e-a016-a5f301f67fc7', 'source': 'agent_message', 'target': {'activeSessionId': '2a1193…",
            sentAgentMessages: [
              {
                id: "agentmsg_e802480f-da2b-4a4e-a016-a5f301f67fc7",
                message: "42",
                deliveryStatus: "delivered",
                target: {
                  activeSessionId: "2a1193474648",
                  sessionId: "01a12a4a-dae3-763e-a443-b8b159822c79",
                },
                receiverRole: "parent",
              },
            ],
          },
          isError: false,
          timestamp: 1791710952858,
        },
      ],
    },
  },
  {
    type: "observed_session_event",
    activeSessionId: "f37cc57f87da",
    event: { type: "turn_start" },
  },
  { type: "session_action_update", actions: { queuedCount: 0, steering: [], followUps: [] } },
  {
    type: "observed_session_event",
    activeSessionId: "f37cc57f87da",
    event: {
      type: "message_start",
      message: {
        role: "assistant",
        content: [],
        api: "anthropic-messages",
        provider: "cpa-claude",
        model: "claude-haiku-5-5",
        usage: {
          input: 0,
          output: 0,
          cacheRead: 0,
          cacheWrite: 0,
          totalTokens: 0,
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
        },
        stopReason: "stop",
        timestamp: 1791710952862,
      },
      assistantMessageEvent: { type: "start" },
    },
  },
  {
    type: "observed_session_event",
    activeSessionId: "f37cc57f87da",
    event: {
      type: "message_update",
      message: {
        role: "assistant",
        content: [{ type: "text", text: "42" }],
        api: "anthropic-messages",
        provider: "cpa-claude",
        model: "claude-haiku-5-5",
        responseId: "msg_011CfvDNvEbejDLwWe4z2LmC",
        usage: {
          input: 0,
          output: 0,
          cacheRead: 0,
          cacheWrite: 0,
          totalTokens: 0,
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
        },
        stopReason: "stop",
        timestamp: 1791710952862,
      },
      assistantMessageEvent: { type: "text_delta", delta: "42" },
    },
  },
  {
    type: "observed_session_event",
    activeSessionId: "f37cc57f87da",
    event: {
      type: "message_update",
      message: {
        role: "assistant",
        content: [{ type: "text", text: "42" }],
        api: "anthropic-messages",
        provider: "cpa-claude",
        model: "claude-haiku-5-5",
        responseId: "msg_011CfvDNvEbejDLwWe4z2LmC",
        usage: {
          input: 0,
          output: 0,
          cacheRead: 0,
          cacheWrite: 0,
          totalTokens: 0,
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
        },
        stopReason: "stop",
        timestamp: 1791710952862,
      },
      assistantMessageEvent: { type: "text_end" },
    },
  },
  {
    type: "observed_session_event",
    activeSessionId: "f37cc57f87da",
    event: {
      type: "message_end",
      message: {
        role: "assistant",
        content: [{ type: "text", text: "42" }],
        api: "anthropic-messages",
        provider: "cpa-claude",
        model: "claude-haiku-5-5",
        responseId: "msg_011CfvDNvEbejDLwWe4z2LmC",
        usage: {
          input: 2,
          output: 3,
          cacheRead: 21695,
          cacheWrite: 385,
          totalTokens: 22085,
          cost: {
            input: 2.0000000000000002e-7,
            output: 1.5e-6,
            cacheRead: 0.00021695,
            cacheWrite: 4.8125e-5,
            total: 0.000266775,
          },
        },
        stopReason: "stop",
        timestamp: 1791710952862,
      },
    },
  },
  {
    type: "observed_session_event",
    activeSessionId: "f37cc57f87da",
    event: {
      type: "turn_end",
      message: {
        role: "assistant",
        content: [{ type: "text", text: "42" }],
        api: "anthropic-messages",
        provider: "cpa-claude",
        model: "claude-haiku-5-5",
        responseId: "msg_011CfvDNvEbejDLwWe4z2LmC",
        usage: {
          input: 2,
          output: 3,
          cacheRead: 21695,
          cacheWrite: 385,
          totalTokens: 22085,
          cost: {
            input: 2.0000000000000002e-7,
            output: 1.5e-6,
            cacheRead: 0.00021695,
            cacheWrite: 4.8125e-5,
            total: 0.000266775,
          },
        },
        stopReason: "stop",
        timestamp: 1791710952862,
      },
      toolResults: [],
    },
  },
  {
    type: "observed_session_event",
    activeSessionId: "f37cc57f87da",
    event: {
      type: "agent_end",
      messages: [
        {
          customType: "harness_digest",
          content:
            "[harness-digest]\n\nThe persistent memories produced across this session so far:\n\n<harness_state>\n# Continual Harness Stat…",
          display: false,
          details: {
            digest:
              "# Continual Harness State\n\nLocal continual harness entries belong to this Prime Agent session. Global continual harness …",
            stateFingerprint: "b77e4293d649db19d47120215434b760c8bd519e8f6267d96c280d2d547884bc",
          },
          timestamp: 1791710949039,
          role: "custom",
        },
        {
          customType: "agent_message",
          content: "[task from parent]\n\nWhat is 17 + 25? Reply with only the number.",
          display: true,
          details: {
            id: "spawn:sub-1fae0326",
            message: "What is 17 + 25? Reply with only the number.",
            from: {
              activeSessionId: "2a1193474648",
              sessionId: "01a12a4a-dae3-763e-a443-b8b159822c79",
            },
            fromRelationship: "parent",
          },
          timestamp: 1791710948998,
          role: "custom",
        },
        {
          role: "assistant",
          content: [
            {
              type: "thinking",
              thinking:
                '17+25 is 42. Since the instructions say to reply with only the number, I\'ll send "42" to the parent via agent_message.send.\n\n',
              thinkingSignature: "",
            },
            {
              type: "toolCall",
              id: "toolu_01KgCEZ4TZmRfKooJtK87BTL",
              name: "ipython",
              arguments: { code: 'await agent_message.send("42", receiver_role="parent")' },
            },
          ],
          api: "anthropic-messages",
          provider: "cpa-claude",
          model: "claude-haiku-5-5",
          responseId: "msg_011CfvDNe8iVv6j15amFpddP",
          usage: {
            input: 2,
            output: 171,
            cacheRead: 861,
            cacheWrite: 20834,
            totalTokens: 21868,
            cost: {
              input: 2.0000000000000002e-7,
              output: 8.549999999999999e-5,
              cacheRead: 8.61e-6,
              cacheWrite: 0.00260425,
              total: 0.0026985599999999996,
            },
          },
          stopReason: "toolUse",
          timestamp: 1791710949042,
        },
        {
          role: "toolResult",
          toolCallId: "toolu_01KgCEZ4TZmRfKooJtK87BTL",
          toolName: "ipython",
          content: [
            {
              text: "{'id': 'agentmsg_e802480f-da2b-4a4e-a016-a5f301f67fc7', 'source': 'agent_message', 'target': {'activeSessionId': '2a1193…",
              type: "text",
            },
          ],
          details: {
            status: "ok",
            kernelRestarted: false,
            durationMs: 7,
            result:
              "{'id': 'agentmsg_e802480f-da2b-4a4e-a016-a5f301f67fc7', 'source': 'agent_message', 'target': {'activeSessionId': '2a1193…",
            sentAgentMessages: [
              {
                id: "agentmsg_e802480f-da2b-4a4e-a016-a5f301f67fc7",
                message: "42",
                deliveryStatus: "delivered",
                target: {
                  activeSessionId: "2a1193474648",
                  sessionId: "01a12a4a-dae3-763e-a443-b8b159822c79",
                },
                receiverRole: "parent",
              },
            ],
          },
          isError: false,
          timestamp: 1791710952858,
        },
        {
          role: "assistant",
          content: [{ type: "text", text: "42" }],
          api: "anthropic-messages",
          provider: "cpa-claude",
          model: "claude-haiku-5-5",
          responseId: "msg_011CfvDNvEbejDLwWe4z2LmC",
          usage: {
            input: 2,
            output: 3,
            cacheRead: 21695,
            cacheWrite: 385,
            totalTokens: 22085,
            cost: {
              input: 2.0000000000000002e-7,
              output: 1.5e-6,
              cacheRead: 0.00021695,
              cacheWrite: 4.8125e-5,
              total: 0.000266775,
            },
          },
          stopReason: "stop",
          timestamp: 1791710952862,
        },
      ],
    },
  },
  {
    type: "rlm_child_update",
    child: {
      id: "sub-1fae0326",
      activeSessionId: "f37cc57f87da",
      sessionName: "adder",
      label: "What is 17 + 25? Reply with only the number.",
      status: "done",
      durationMs: 5826,
      sessionDir:
        "/home/aditya/scratch/prime-fork-slice2/smoke-live2/home/.prime/agent/session-artifacts/01a12a4a-dae3-763e-a443-b8b159822c79/sub-1fae0326",
      model: "cpa-claude/claude-haiku-5-5",
      answerPreview: "42",
    },
  },
  {
    type: "observed_session_closed",
    activeSessionId: "f37cc57f87da",
    error:
      "The Prime Agent daemon shut down while this window was attached. The session transcript remains saved; restart Prime Agent and reopen it from Agents View.",
  },
];

/** What the fork answered `observe {activeSessionId: "f37cc57f87da"}` with. */
export const PRIME_AGENT_FORK_OBSERVED_MESSAGES: ReadonlyArray<unknown> = [
  {
    customType: "agent_message",
    content: "[task from parent]\n\nWhat is 17 + 25? Reply with only the number.",
    display: true,
    details: {
      id: "spawn:sub-1fae0326",
      message: "What is 17 + 25? Reply with only the number.",
      from: { activeSessionId: "2a1193474648", sessionId: "01a12a4a-dae3-763e-a443-b8b159822c79" },
      fromRelationship: "parent",
    },
    role: "custom",
    timestamp: "2026-10-11T09:29:09.019Z",
  },
];
