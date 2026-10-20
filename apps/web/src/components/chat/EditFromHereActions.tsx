import type { OrchestrationV2RollbackSummary } from "@t3tools/contracts";
import { useState } from "react";

import { AlertDialogClose, AlertDialogFooter } from "../ui/alert-dialog";
import { Button } from "../ui/button";
import { Checkbox } from "../ui/checkbox";
import { Label } from "../ui/label";
import { Textarea } from "../ui/textarea";

/**
 * The "Edit from here" dialog's choices. A provider that can summarize the
 * discarded turns also offers that, with optional instructions. Key it by the
 * message being reverted so each dialog starts unchecked.
 */
export function EditFromHereActions(props: {
  readonly canSummarize: boolean;
  readonly onConfirm: (
    restoreFiles: boolean,
    summary: OrchestrationV2RollbackSummary | undefined,
  ) => void;
}) {
  const [summarize, setSummarize] = useState(false);
  const [instructions, setInstructions] = useState("");
  const confirm = (restoreFiles: boolean) => {
    const trimmed = instructions.trim();
    props.onConfirm(
      restoreFiles,
      props.canSummarize && summarize
        ? trimmed === ""
          ? {}
          : { instructions: trimmed }
        : undefined,
    );
  };

  return (
    <>
      {props.canSummarize ? (
        <div className="flex flex-col gap-3 px-6 pb-6">
          <Label>
            <Checkbox checked={summarize} onCheckedChange={setSummarize} />
            Summarize the discarded turns
          </Label>
          {summarize ? (
            <Textarea
              aria-label="Summary instructions"
              value={instructions}
              onChange={(event) => setInstructions(event.target.value)}
              placeholder="Optional: what the summary should keep"
              size="sm"
            />
          ) : null}
        </div>
      ) : null}
      <AlertDialogFooter>
        <AlertDialogClose render={<Button variant="outline" />}>Cancel</AlertDialogClose>
        <Button variant="destructive-outline" onClick={() => confirm(true)}>
          Revert files too
        </Button>
        <Button onClick={() => confirm(false)}>Revert and keep changes</Button>
      </AlertDialogFooter>
    </>
  );
}
